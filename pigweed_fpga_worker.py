#!/usr/bin/env python3
"""Native bus execution and RTL qualification for the PigWeedSAT FPGA campaign.

Child of ``run_pigweed_fpga.py``: one process at a time talks to one board.
All solver/platform code comes from the run's frozen ``candidate/`` snapshot
(never the live PigWeedSAT checkout; asserted at import); nothing here edits
SDK, firmware or candidate files.

Transport policy (pinned SDK d77d68d1):
* ``SendOnceTransport`` overrides only ``_send_locked``: open if needed, send
  exactly once, never reconnect/replay.  A non-idempotent memory push or GO
  must never reach the board twice.
* ``AttachedApp`` never programs (``_program`` raises), only opens
  ``SendOnceTransport`` connections; framing, reply parsing, the idle-close
  watcher and cleanup stay official SDK behavior.
* Any send/receive exception is terminal for this worker: no further bus
  transaction even if a fresh socket could open.  The parent then performs a
  session-guarded release.

Timing contract for scored attempts (host-observed, NOT device-enforced):
a durable GO-intent marker is fsynced immediately before the single GO write;
its monotonic timestamp is t0 for both the scored interval and the parent's
1,060 s watchdog.  The first done-observation at or after t0+timeout is
timeout/late evidence even if the device finished earlier — but a late
answer that is *invalid* (bad witness, or contradicting the CPU certificate)
is classified wrong_answer first: correctness outranks lateness.  A clean
timeout/resource_error keeps its scored status even when the bounded
recovery afterwards fails; ``recovery_failed`` then halts the campaign.
"""

from __future__ import annotations

import contextlib
import json
import os
import signal
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import benchmark_cnf as BC  # noqa: E402  (strict original-formula validation)

IO_DEADLINE_S = 30.0        # Unix signal deadline around each official bus call
POLL_S = 0.05
LIT_PAGE = 8                # literal-store page size (R_LITPAGE) of the frozen candidate config
CLK_HZ = 50_000_000         # device clock; cycles beyond timeout_s * CLK_HZ
                            # are outside the solve budget, never success
RECOVERY_BUDGET_S = 120.0
WALL_JUMP_GRACE_S = 5.0     # wall clock may not outrun monotonic beyond this

TERMINAL_STATUS = ("sat", "unsat", "timeout", "resource_error",
                   "wrong_answer", "transport_error")


class IOTimeout(Exception):
    """A single bus call exceeded its 30 s deadline."""


class TerminalTransport(Exception):
    """Unrecoverable for this worker: connection failed mid-exchange."""


# ---------------------------------------------------------------------------
# SDK subclasses (pinned official behavior; no monkeypatching)
# ---------------------------------------------------------------------------

def build_sdk_subclasses():
    """Import the pinned SDK lazily and return (App, WebSocketTransport).

    Importing here (not at module top) keeps this module importable for unit
    tests without the SDK installed.
    """
    from manhattan_reasoning_gym import _app, _transport
    return _app.App, _transport.WebSocketTransport


class SendOnceTransportMixin:
    """send-exactly-once replacement for WebSocketTransport._send_locked."""

    def _send_locked(self, payload):
        conn = self._open_locked()
        conn.send(payload)          # exactly one frame; failure is terminal
        return conn


def make_transport_classes(WebSocketTransport):
    class SendOnceTransport(SendOnceTransportMixin, WebSocketTransport):
        pass
    return SendOnceTransport


def make_app_class(App, SendOnceTransport):
    class AttachedApp(App):
        """An App pre-verified onto a live session; never programs or releases."""

        def __init__(self, *, fpga_id, design_path, api_url=None):
            super().__init__("pigweed-sat-fpga", design=design_path,
                             fpga_id=fpga_id,
                             **({"api_url": api_url} if api_url else {}))
            # The organizer verified this board is reserved for the session
            # bound to a completed build job of ours before constructing us.
            self._programmed = True

        def _program(self):
            raise RuntimeError(
                "AttachedApp attaches to a verified session; programming is "
                "owned by the organizer")

        def _open_connection(self):
            if self._connection is None:
                self._connection = SendOnceTransport(
                    self.fpga_id, self.api_key, self.api_url)
            return self._connection
    return AttachedApp


# ---------------------------------------------------------------------------
# Bounded-I/O plumbing
# ---------------------------------------------------------------------------

@contextlib.contextmanager
def io_deadline(seconds: float = IO_DEADLINE_S, enabled: bool = True):
    """SIGALRM deadline around one official SDK call; restores handlers after.

    On expiry raises IOTimeout.  A healthy exchange is never interrupted by
    the solve cutoff: each call simply gets its own 30 s bound.
    """
    if not enabled:
        yield
        return

    def handler(signum, frame):
        raise IOTimeout(f"bus call exceeded {seconds:.0f}s")

    old_handler = signal.signal(signal.SIGALRM, handler)
    old_timer = signal.setitimer(signal.ITIMER_REAL, seconds)
    start = time.monotonic()
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0.0)
        signal.signal(signal.SIGALRM, old_handler)
        if old_timer and old_timer[0] > 0:
            remaining = max(0.0, old_timer[0] - (time.monotonic() - start))
            if remaining > 0:
                signal.setitimer(signal.ITIMER_REAL, remaining, old_timer[1])


class GuardedBus:
    """CloudBus-shaped wrapper: every candidate-driver call gets its own
    30 s deadline; any transport-layer failure marks the worker terminal."""

    def __init__(self, inner, emit=None, alarm_enabled=True):
        self.inner = inner
        self._emit = emit or (lambda *a, **k: None)
        self._alarm = alarm_enabled
        self.terminal: str | None = None

    def _call(self, label, fn, *args, **kwargs):
        if self.terminal is not None:
            raise TerminalTransport(
                f"worker already terminal ({self.terminal}); no bus call")
        try:
            with io_deadline(enabled=self._alarm):
                return fn(*args, **kwargs)
        except TerminalTransport:
            raise
        except IOTimeout as e:
            self.terminal = f"io_timeout:{label}"
            self._emit("transport_terminal", where=label, reason=str(e))
            raise TerminalTransport(str(e)) from e
        except Exception as e:                       # socket/SDK failure
            self.terminal = f"{type(e).__name__}:{label}"
            self._emit("transport_terminal", where=label,
                       reason=f"{type(e).__name__}: {e}")
            raise TerminalTransport(
                f"{label}: {type(e).__name__}: {e}") from e

    def write(self, word, value):
        self._call("write", self.inner.write, word, value)

    def write_fifo(self, word, values):
        self._call("write_fifo", self.inner.write_fifo, word, values)

    def read(self, word):
        return self._call("read", self.inner.read, word)

    def read_fifo(self, count):
        return self._call("read_fifo", self.inner.read_fifo, count)


# ---------------------------------------------------------------------------
# Timing/lease classification (pure; unit-tested)
# ---------------------------------------------------------------------------

def classify_observation(t_obs: float, t0: float, deadline: float) -> str:
    """First done-observation strictly before t0+deadline counts; anything
    observed at/after it is late (timeout evidence), even if the device
    finished earlier."""
    return "done_within" if (t_obs - t0) < deadline else "done_late"


def lease_remaining_s(expires_at_utc: float | None, now_utc: float,
                      request_latency_s: float = 0.0) -> float | None:
    """Conservative remaining lease: server expiry minus local now, with
    observed request latency subtracted (the clock evidence may lag)."""
    if expires_at_utc is None:
        return None
    return expires_at_utc - now_utc - request_latency_s


def lease_ok(remaining: float | None, need: float) -> bool:
    return remaining is not None and remaining >= need


def classify_done(result_code: int, witness_valid: bool | None,
                  expected: str) -> str:
    """Correctness classification of a stable done answer, independent of
    lateness.  A SAT answer requires a valid original-formula witness AND
    agreement with the CPU certificate; UNSAT requires the certificate to
    say UNSAT.  Wrong answers are wrong answers whether late or not."""
    if result_code == 1:
        if witness_valid is not True:
            return "wrong_answer"
        return "sat" if expected == "sat" else "wrong_answer"
    if result_code == 0:
        return "unsat" if expected == "unsat" else "wrong_answer"
    return "resource_error"


# ---------------------------------------------------------------------------
# Stack validation
# ---------------------------------------------------------------------------

def stack_problems(num_vars: int, stack: list[int]) -> list[str]:
    """Raw answer-stack sanity: entries are 0 or literals of variables 1..N,
    with no variable assigned twice (conflicting evidence)."""
    out = []
    seen = set()
    for x in stack:
        v = abs(x)
        if x != 0 and (v < 1 or v > num_vars):
            out.append(f"literal {x} outside 1..{num_vars}")
        elif v in seen:
            out.append(f"variable {v} appears twice on the answer stack")
        elif x != 0:
            seen.add(v)
    return out


# ---------------------------------------------------------------------------
# run_one: one scored attempt over the native bus
# ---------------------------------------------------------------------------

def fsync_write(path: str, obj) -> None:
    data = (json.dumps(obj, sort_keys=True) + "\n").encode()
    with open(path, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())


def run_one(bus, host, drv, img, original_cnf, expected, timeout_s=1000.0,
            attempt_dir=None, emit=None, mono=time.monotonic, sleep=time.sleep,
            go_through_recovery=True, lease=None, recovery_budget_s=None,
            wall=time.time):
    """Load, GO, observe, classify.  Returns a result dict; never raises for
    solver-level outcomes (only TerminalTransport escapes, recorded as
    transport_error by the caller)."""
    emit = emit or (lambda *a, **k: None)
    t_total0 = mono()
    t0 = None

    res = {
        "status": None, "result_code": None, "cycles": None,
        "cycle_counters": None, "stats": None, "model": None,
        "raw_stack": None, "stack_problems": [], "witness_valid": None,
        "expected": expected, "late": False, "pre_abort_snapshot": None,
        "host_times": {}, "recovery": None, "recovery_failed": False,
        "transport_error": None, "timeout_s": timeout_s,
    }
    host_times = res["host_times"]

    if lease is not None:
        def lease_remaining():
            rem = lease_remaining_s(lease["expires_at_utc"], wall(),
                                    lease.get("latency_s", 0.0))
            skew = max(0.0, lease.get("skew_s", 0.0))
            return None if rem is None else rem - skew

    # wall-clock continuity guard: if the wall clock outruns monotonic time
    # by more than the grace window (system sleep), the session is uncertain
    last_wall = wall()
    last_mono = t_total0

    def wall_jump_suspicious(m: float) -> bool:
        """Takes the caller's already-sampled monotonic value: the scored
        loop performs no extra clock sample."""
        nonlocal last_wall, last_mono
        w = wall()
        jump = (w - last_wall) - (m - last_mono)
        last_wall, last_mono = w, m
        return jump > WALL_JUMP_GRACE_S

    try:
        drv.check_caps(bus, img)                       # exact magic/version/caps
        drv.abort(bus)                                 # to idle from any state
        drv.check_caps(bus, img)                       # verified caps after idle

        if lease is not None and not lease_ok(lease_remaining(),
                                              lease["min_load_s"]):
            res["status"] = "session_budget_insufficient"
            res["stage"] = "before_load"
            host_times["total_s"] = mono() - t_total0
            return res

        t = mono()
        drv.load(bus, img)
        host_times["load_s"] = mono() - t
        emit("loaded", load_s=host_times["load_s"])

        if lease is not None and not lease_ok(lease_remaining(),
                                              lease["min_go_s"]):
            drv.abort(bus)                             # leave the board idle
            res["status"] = "session_budget_insufficient"
            res["stage"] = "before_go"
            host_times["total_s"] = mono() - t_total0
            return res
        # Durable GO intent immediately before the single GO send.
        t0 = mono()
        if attempt_dir is not None:
            fsync_write(os.path.join(attempt_dir, "go-intent.json"),
                        {"t0_mono": t0, "t0_utc": wall(),
                         "timeout_s": timeout_s})
        emit("go_intent", t0_mono=t0)
        bus.write(drv.D.R_CTRL, drv.D.CTRL_GO)         # the single GO
        host_times["go_ack_s"] = mono() - t0
        emit("go_sent", ack_delay_s=host_times["go_ack_s"])

        deadline = t0 + timeout_s
        done = False
        while True:
            pre = mono()
            if wall_jump_suspicious(pre):
                # system slept between poll iterations: the server may have
                # expired/reassigned the session while our socket dropped.
                # Halt BEFORE any new bus call could open a stream to a
                # replacement session.
                res["status"] = "transport_error"
                res["transport_error"] = (
                    "wall clock jumped ahead of monotonic time between "
                    f"polls by more than {WALL_JUMP_GRACE_S:.0f}s (system "
                    "sleep?); session state uncertain; no further "
                    "transaction")
                host_times["total_s"] = mono() - t_total0
                return res
            if pre >= deadline:
                break                                  # deadline reached
            ctrl = bus.read(drv.D.R_CTRL)
            t_obs = mono()
            if wall_jump_suspicious(t_obs):
                res["status"] = "transport_error"
                res["transport_error"] = (
                    "wall clock jumped ahead of monotonic time by more than "
                    f"{WALL_JUMP_GRACE_S:.0f}s (system sleep?); session "
                    "state uncertain; no further transaction")
                host_times["total_s"] = mono() - t_total0
                return res
            if ctrl & 2:
                res["late"] = classify_observation(
                    t_obs, t0, timeout_s) == "done_late"
                done = True
                break
            if t_obs >= deadline:
                break
            sleep(POLL_S)

        host_times["t0_to_obs_s"] = mono() - t0
        emit("observed", late=res["late"], within=done)

        if not done:
            # Nominal solve deadline reached while still busy (or reply
            # straddled the cutoff): synchronized timeout -> abort to idle.
            res["status"] = "timeout"
            drv.abort(bus)
            res["post_abort_ctrl"] = bus.read(drv.D.R_CTRL)
            emit("aborted_after_deadline")
        else:
            res["result_code"] = host.s32(bus.read(drv.D.R_RESULT))
            t = mono()
            res["stats"] = drv.read_stats(bus)
            res["cycles"] = (res["stats"].get("cycles_lo", 0)
                             | (res["stats"].get("cycles_hi", 0) << 32))
            res["cycle_counters"] = res["stats"].get("cycle_counters")
            host_times["readback_s"] = mono() - t
            if res["result_code"] == 1:
                height = res["stats"]["height"]
                res["raw_stack"] = drv.read_stack(bus, height)
                model = host.model_from_stack(img.num_vars, res["raw_stack"])
                res["model"] = {str(k): v for k, v in model.items()}
                t = mono()
                res["stack_problems"] = stack_problems(
                    img.num_vars, res["raw_stack"])
                res["witness_valid"] = (
                    not res["stack_problems"]
                    and BC.check_witness(original_cnf, model))
                host_times["validate_s"] = mono() - t

            # correctness first, lateness second: a late answer that is
            # invalid or contradicts the CPU certificate is wrong_answer
            correctness = classify_done(res["result_code"],
                                        res["witness_valid"], expected)
            if res["late"]:
                res["status"] = ("timeout" if correctness in ("sat", "unsat")
                                 else correctness)
            else:
                res["status"] = correctness

            if res["cycles"] is not None \
                    and res["cycles"] > timeout_s * CLK_HZ \
                    and res["status"] in ("sat", "unsat"):
                # outside solve budget: not success
                res["status"] = "resource_error"
                res["over_budget_cycles"] = res["cycles"]
    except TerminalTransport as e:
        res["status"] = "transport_error"
        res["transport_error"] = str(e)
        res["deadline_reached"] = (mono() - t0 >= timeout_s
                                   if t0 is not None else None)
        host_times["total_s"] = mono() - t_total0
        return res

    if go_through_recovery and res["status"] in ("timeout", "resource_error"):
        rec = synthetic_recovery(bus, host, drv, emit=emit, mono=mono,
                                  budget_s=recovery_budget_s
                                  or RECOVERY_BUDGET_S)
        res["recovery"] = rec
        if not rec.get("ok"):
            # keep the scored status (truthful), flag the halt: board health
            # is uncertain and the parent must stop the campaign
            res["recovery_failed"] = True

    host_times["total_s"] = mono() - t_total0
    return res


def synthetic_recovery(bus, host, drv, emit=None, mono=time.monotonic,
                       budget_s: float = RECOVERY_BUDGET_S):
    """Aggregate-bounded recovery: abort to idle, verify caps, solve the
    trivial formula [[1]] and validate the *actual returned stack* against
    the strict original formula.  One stage, one budget."""
    emit = emit or (lambda *a, **k: None)
    out = {"ok": False, "budget_s": budget_s}
    t0 = mono()
    emit("recovery_start", budget_s=budget_s)
    try:
        # the budget bounds the WHOLE stage: check remaining time between
        # every bus stage so slow calls cannot run arbitrarily past it
        # (each individual call is additionally capped by the 30 s alarm)
        drv.abort(bus)
        if mono() - t0 > budget_s:
            out["reason"] = "recovery budget exhausted before caps"
            emit("recovery_done", **out)
            return out
        out["ctrl_after_abort"] = bus.read(drv.D.R_CTRL)
        img = host.build_images(1, [[1]], host.pigweed(host.Config(lit_page=LIT_PAGE)))
        drv.check_caps(bus, img)
        if mono() - t0 > budget_s:
            out["reason"] = "recovery budget exhausted before load"
            emit("recovery_done", **out)
            return out
        drv.load(bus, img)
        if mono() - t0 > budget_s:
            out["reason"] = "recovery budget exhausted before GO"
            emit("recovery_done", **out)
            return out
        bus.write(drv.D.R_CTRL, drv.D.CTRL_GO)
        deadline = t0 + budget_s
        while mono() < deadline:
            ctrl = bus.read(drv.D.R_CTRL)
            if ctrl & 2:
                break
            time.sleep(POLL_S)
        else:
            out["reason"] = "recovery budget exhausted before done"
            emit("recovery_done", **out)
            return out
        if mono() - t0 > budget_s:
            out["reason"] = "recovery budget exhausted before readback"
            emit("recovery_done", **out)
            return out
        result = host.s32(bus.read(drv.D.R_RESULT))
        out["trivial_result"] = result
        stats = drv.read_stats(bus)
        stack = drv.read_stack(bus, stats["height"])
        out["trivial_stack"] = stack
        original = BC.parse_dimacs("p cnf 1 1\n1 0\n")
        if mono() - t0 > budget_s:
            out["reason"] = "recovery budget exhausted during readback"
            emit("recovery_done", **out)
            return out
        out["ok"] = (result == 1
                     and not stack_problems(1, stack)
                     and BC.check_witness(
                         original, host.model_from_stack(1, stack)))
        drv.abort(bus)
        # the budget bounds the whole stage: an over-budget completion is
        # not a pass even if every intermediate check happened to fit
        out["elapsed_s"] = mono() - t0
        if out["elapsed_s"] > budget_s:
            out["ok"] = False
            out["reason"] = "recovery exceeded budget at final stage"
        emit("recovery_done", **out)
        return out
    except TerminalTransport as e:
        out["reason"] = f"transport: {e}"
        return out


# ---------------------------------------------------------------------------
# Session qualification (each newly assigned session)
# ---------------------------------------------------------------------------

QUALIFY_FORMULAS = [
    (2, [[1], [-1, 2]], "sat"),
    (2, [[1], [-1]], "unsat"),
    (2, [[1, 2], [-1, 2]], "sat"),
]


def qualify_session(bus, host, drv, fixture_dir=None, pilot_cnfs=None,
                    emit=None, mono=time.monotonic):
    """Exact magic/version/caps, SAT->UNSAT->SAT on the real board, then an
    observed active-busy abort followed by the bounded synthetic recovery.
    The one-second solve window runs from GO, not from load.  Bounded by the
    parent's 600 s stage."""
    emit = emit or (lambda *a, **k: None)
    out = {"ok": False, "sequence": [], "abort_recovery": None}
    img0 = host.build_images(1, [[1]], host.pigweed(host.Config(lit_page=LIT_PAGE)))
    drv.check_caps(bus, img0)                    # exact magic/version/caps

    for nv, clauses, want in QUALIFY_FORMULAS:
        t = mono()
        img = host.build_images(nv, clauses,
                                host.pigweed(host.Config(lit_page=LIT_PAGE)))
        drv.abort(bus)
        drv.load(bus, img)
        t_go = mono()                            # window opens at GO
        bus.write(drv.D.R_CTRL, drv.D.CTRL_GO)
        done = False
        while True:
            ctrl = bus.read(drv.D.R_CTRL)
            if ctrl & 2:
                done = True
                break
            if (mono() - t_go) > 1.0:
                break
        if not done:
            drv.abort(bus)
            out["reason"] = "qualification formula did not finish in 1s from GO"
            out["sequence"].append({"formula": clauses, "want": want,
                                    "got_result": None, "valid": False,
                                    "elapsed_s": mono() - t})
            return out
        result = host.s32(bus.read(drv.D.R_RESULT))
        model = None
        if result == 1:
            stats = drv.read_stats(bus)
            stack = drv.read_stack(bus, stats["height"])
            model = host.model_from_stack(img.num_vars, stack)
        original = BC.parse_dimacs(
            "p cnf %d %d\n%s" % (nv, len(clauses),
                                 "".join(" ".join(map(str, c)) + " 0\n"
                                         for c in clauses)))
        good = (result == 1 and want == "sat"
                and BC.check_witness(original, model)) or \
               (result == 0 and want == "unsat")
        out["sequence"].append({
            "formula": clauses, "want": want, "got_result": result,
            "valid": bool(good), "elapsed_s": mono() - t})
        if not good:
            out["reason"] = "semantic sequence mismatch"
            return out
        drv.abort(bus)

    out["abort_recovery"] = _demonstrate_active_abort(
        bus, host, drv, fixture_dir, pilot_cnfs, emit=emit, mono=mono)
    if not (out["abort_recovery"] and out["abort_recovery"]["ok"]):
        out["ok"] = False
        out["reason"] = "no active-busy abort demonstrated"
        return out
    # post-abort health: the board must also pass the bounded synthetic
    # recovery before it is certified usable for scored work
    rec = synthetic_recovery(bus, host, drv, emit=emit, mono=mono)
    out["post_abort_recovery"] = rec
    out["ok"] = bool(rec.get("ok"))
    if not out["ok"]:
        out["reason"] = "post-abort synthetic recovery failed"
    return out


def _demonstrate_active_abort(bus, host, drv, fixture_dir, pilot_cnfs,
                              emit, mono):
    """GO on the public pret150_75 fixture, observe CTRL busy within 1 s of
    GO, abort immediately.  If it finished before busy was observed, try each
    eligible fixed pilot once (ascending ID) with the same 1 s limit.  A
    blocking CTRL read that lands after the window counts as not
    demonstrated; the elapsed check runs after every read."""
    attempts = []
    cases = []
    if fixture_dir:
        p = os.path.join(fixture_dir, "unsat", "pret150_75.dimacs")
        if os.path.isfile(p):
            cases.append(("pret150_75", p))
    for iid, path in (pilot_cnfs or []):
        cases.append((iid, path))
    for name, path in cases:
        t = mono()
        try:
            nv, clauses = host.parse_dimacs(path)
            img = host.build_images(
                nv, clauses, host.pigweed(host.Config(lit_page=LIT_PAGE)))
            drv.abort(bus)
            drv.load(bus, img)
            t_go = mono()
            bus.write(drv.D.R_CTRL, drv.D.CTRL_GO)
            busy_seen = False
            while mono() - t_go <= 1.0:
                ctrl = bus.read(drv.D.R_CTRL)
                if mono() - t_go > 1.0:
                    break               # reply landed outside the window
                if ctrl & 1:
                    busy_seen = True
                    break
                if ctrl & 2:
                    break
            att = {"case": name, "busy_seen": busy_seen,
                   "elapsed_s": mono() - t}
            if busy_seen:
                drv.abort(bus)
                ctrl_after = bus.read(drv.D.R_CTRL)
                att["ctrl_after_abort"] = ctrl_after
                att["idle_after_abort"] = (ctrl_after & 1) == 0
                att["ok"] = att["idle_after_abort"]
                attempts.append(att)
                if att["ok"]:
                    return {"ok": True, "attempts": attempts}
            else:
                att["ok"] = False
                attempts.append(att)
        except TerminalTransport:
            attempts.append({"case": name, "transport": True, "ok": False})
            return {"ok": False, "attempts": attempts}
    return {"ok": False, "attempts": attempts}


# ---------------------------------------------------------------------------
# Child entry point
# ---------------------------------------------------------------------------

def _open_bus(spec):
    """Construct AttachedApp + CloudBus over the frozen candidate driver.
    Asserts that design/satlat were imported from the run's candidate
    snapshot, never from a shadowing path."""
    cand = os.path.realpath(spec["candidate_root"])
    sys.path.insert(0, cand)
    import design  # noqa: F401
    import satlat.host as host
    import satlat.driver as drv
    for mod in (design, host, drv):
        origin = os.path.realpath(getattr(mod, "__file__", ""))
        if not origin.startswith(cand + os.sep):
            raise RuntimeError(
                f"{mod.__name__} imported from {origin}, not the frozen "
                f"candidate {cand}")
    App, WebSocketTransport = build_sdk_subclasses()
    SendOnceTransport = make_transport_classes(WebSocketTransport)
    AttachedApp = make_app_class(App, SendOnceTransport)
    app = AttachedApp(fpga_id=spec["fpga_id"],
                      design_path=os.path.join(cand, "design.py"),
                      api_url=spec.get("api_url"))
    bus = GuardedBus(drv.CloudBus(app, chunk=256))
    return app, bus, host, drv


def child_main(job_path: str) -> int:
    with open(job_path) as f:
        spec = json.load(f)
    events_path = spec["events_path"]
    result_path = spec["result_path"]
    stderr_log = spec.get("stderr_path")
    if stderr_log:
        sys.stderr = open(stderr_log, "a", buffering=1)
    ev_f = open(events_path, "a", buffering=1)

    def emit(event, **fields):
        rec = {"event": event, "t_mono": time.monotonic(),
               "t_utc": time.time(), **fields}
        ev_f.write(json.dumps(rec, sort_keys=True) + "\n")
        ev_f.flush()

    emit("worker_start", pid=os.getpid(), command=spec["command"])
    app, bus, host, drv = _open_bus(spec)
    emit("worker_attached", fpga_id=spec["fpga_id"],
         session_id=spec.get("session_id"))

    try:
        if spec["command"] == "qualify":
            out = qualify_session(
                bus, host, drv,
                fixture_dir=spec.get("fixture_dir"),
                pilot_cnfs=spec.get("pilot_cnfs"), emit=emit)
        elif spec["command"] == "solve":
            cnf_path = spec["solve"]["cnf_path"]
            t = time.monotonic()
            nv, clauses = host.parse_dimacs(cnf_path)
            parse_s = time.monotonic() - t
            emit("parsed", elapsed_s=parse_s)
            t = time.monotonic()
            img = host.build_images(
                nv, clauses, host.pigweed(host.Config(lit_page=LIT_PAGE)))
            encode_s = time.monotonic() - t
            emit("encoded", elapsed_s=encode_s)
            original = BC.parse_dimacs(open(cnf_path, encoding="utf-8").read())
            out = run_one(
                bus, host, drv, img, original,
                spec["solve"]["expected"],
                timeout_s=spec["solve"]["timeout_s"],
                attempt_dir=spec["solve"]["attempt_dir"], emit=emit,
                lease=spec["solve"].get("lease"))
            # fold parse/encode into the recorded host timing evidence
            ht = out.setdefault("host_times", {})
            ht["parse_s"] = parse_s
            ht["encode_s"] = encode_s
            ht["total_s"] = parse_s + encode_s + ht.get("total_s", 0.0)
        else:
            out = {"status": "error",
                   "reason": f"unknown command {spec['command']}"}
    except TerminalTransport as e:
        out = {"status": "transport_error", "transport_error": str(e)}
    emit("worker_result",
         **{k: v for k, v in out.items() if k != "model"})
    fsync_write(result_path, out)
    emit("worker_done")
    ev_f.close()
    with contextlib.suppress(Exception):
        app._close_connection()
    return 0


if __name__ == "__main__":
    sys.exit(child_main(sys.argv[sys.argv.index("--job") + 1]))
