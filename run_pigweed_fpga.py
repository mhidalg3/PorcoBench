#!/usr/bin/env python3
"""PigweedSAT native FPGA benchmark campaign on Manhattan Reasoning hardware.

Commands (from the repository root, venv .venv-fpga):

    run_pigweed_fpga.py prepare --out RUN [--solve-timeout S] [--repetitions N]
                                [--only-ids-manifest CSV] [--cpu-override DIR]
    run_pigweed_fpga.py qualify --out RUN
    run_pigweed_fpga.py run --out RUN [--resume]
    run_pigweed_fpga.py report --out RUN

Default contract: manifest of 210 instances, frozen candidate snapshot
(PigWeedSAT clone HEAD 679c1364..., byte-identical, clean checkout), cfg =
H.pigweed(H.Config(lit_page=8)), 50 MHz system clock and timing target,
1,000 s host solve deadline, three repetitions per fresh-eligible instance;
prepare's campaign options change the deadline, repetitions, id scope and
CPU evidence sources and are recorded in run.json and the run identity.
Host-observed timing only: no device-enforced solve deadline is claimed.

Every mutating cloud step is journaled durably before it happens and
reconciled on restart before any new submission or MMIO.  A campaign that
halted on a correctness/transport/uncertainty event refuses further booking
until the recorded halt reason is explicitly cleared by a human decision.
`report` never touches the cloud.
"""

from __future__ import annotations

import argparse
import csv
import datetime as _dt
import fcntl
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from collections import deque
from pathlib import Path

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)

import benchmark_cnf as BC                     # noqa: E402
import calibrate_cpu as CC                     # noqa: E402
import screen_pigweed as SP                    # noqa: E402
import pigweed_fpga_worker as WK               # noqa: E402

RUN_SCHEMA = "pigweed-fpga-run/1"
ATTEMPT_SCHEMA = "pigweed-fpga-attempt/1"
PINNED_SDK_COMMIT = "d77d68d19f0cddf5730aa8d5f17b64df4a333632"

# ------------------------------------------------------------------ config --
SYS_CLK_HZ = 50_000_000
TIMING_TARGET_MHZ = 50.0
SOLVE_TIMEOUT_S = 1000.0
REPETITIONS = 3
LIT_PAGE = WK.LIT_PAGE      # one source of truth: the worker encodes with it too
POLL_S = 0.05

PILOT_IDS = ["MRB0-005", "MRB0-011", "MRB0-017",
             "MRB0-026", "MRB0-077", "MRB0-078"]

DEFAULT_MANIFEST = os.path.join(HERE, "manifest.csv")
DEFAULT_CPU_RUN = os.path.join(HERE, "calibration", "cpu-v0")
CANDIDATE_SRC = os.path.join(ROOT, "PigWeedSAT")
CANDIDATE_GIT_HEAD = "679c1364fa8daf8fcc5f47348b932dd118dbea78"
CANDIDATE_PATHS = ("design.py", "satlat/", "LICENSE", "NOTICE",
                   "SAT_test_cases/")
QUAL_FIXTURES = [
    ("sat", "aim50.dimacs"),
    ("sat", "uf20.dimacs"),
    ("unsat", "pret150_75.dimacs"),
    ("unsat", "aalto_2.dimacs"),
]
SANDBOX_DIGEST = ("sha256:a5c74a81bff5d3874c6a10993d99236f9"
                  "aa52b2d7d01368cc7d2156f20e19034")

API_URL = "https://api.manhattanreasoning.com"

# Parent phase watchdogs (seconds).
WD_LOAD = 120.0            # worker_start -> go_intent (caps/parse/encode/load)
WD_GO = 1060.0             # go_intent t0 -> observed (apply_campaign: D + 60)
WD_READBACK = 120.0        # observed -> worker_result (stats/stack/validation)
WD_RECOVERY = 120.0        # bounded synthetic recovery stage
WD_QUALIFY = 600.0         # whole on-board qualification stage
TERM_KILL_GRACE = 5.0
BUILD_POLL_S = 2.0
BUILD_CEILING_S = 14400.0  # official queue/build ceiling
RELEASE_OBSERVE_S = 300.0
BUILD_QUOTA_FLOOR_MIN = 30  # observed build 13 min; never start below this
HTTP_TIMEOUT = (10, 30)
SUBMIT_TIMEOUT = (10, 120)
SUBMIT_INFLIGHT_WINDOW_S = SUBMIT_TIMEOUT[1] + 30   # POST may still be live
RTL_SEQ_TIMEOUT = 1800.0
RTL_PILOT_TIMEOUT = 900.0
SIM_CYCLE_CAP = 2_000_000

# Lease budgets (seconds of remaining TTL).  The load/GO minima and the GO
# watchdog follow the campaign's solve deadline D (apply_campaign): load
# 120 + GO slack 60 + readback 120 + recovery 120 + TERM->KILL 5 + lifecycle
# 120 + clock safety 60, rounded up so D=1000 keeps the original 1800/1680/1060.
LEASE_LOAD_OVERHEAD_S = 800.0
LEASE_GO_OVERHEAD_S = 680.0
WD_GO_SLACK_S = 60.0
ATTACH_QUALIFY_MARGIN_S = 150.0   # session start -> after on-board qualify
LEASE_MIN_LOAD = SOLVE_TIMEOUT_S + LEASE_LOAD_OVERHEAD_S   # before load
LEASE_MIN_GO = SOLVE_TIMEOUT_S + LEASE_GO_OVERHEAD_S       # before GO
LEASE_MIN_QUALIFY = 2400.0  # fresh lease before qualification stage

# Campaign scope (apply_campaign; defaults = the original 210-row campaign).
ONLY_IDS: tuple | None = None     # restrict the work plan to these ids
CPU_RUNS = [{"path": os.path.relpath(DEFAULT_CPU_RUN, HERE),
             "env": [1000, 3000]}]   # later entries override earlier per id

# Boards whose assigned session is released before any board I/O and rebuilt
# (user-directed 2026-10-16: board 6's host agent dropped /stream with
# 4502/503 "host agent unreachable" ~15-18 s into solves; see the run's
# board6-incident.md).  /submit cannot target a board, so the only lever is
# to decline an assignment.  Bounded: more than MAX_BOARD_SKIPS consecutive
# landings on an excluded board stop the campaign.
SKIP_FPGA_IDS = (6,)
MAX_BOARD_SKIPS = 3

TERMINAL_STATUS = ("sat", "unsat", "timeout", "resource_error",
                   "wrong_answer", "transport_error")
ATTEMPT_INTERRUPTED = "interrupted"
SESSION_BUDGET = "session_budget_insufficient"

# A failed qualification is BELOW prepared: it blocks everything until the
# failure is fixed and qualify is re-run (new artifacts, new identity).
PHASE_RANK = {"qualified_with_failures": -1, "prepared": 0, "qualified": 1,
              "running": 2, "run_done": 3, "run_blocked": 3}


def utcnow_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_utc(s: str) -> float:
    return _dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def sha256_file(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def atomic_write_json(path: Path, obj) -> None:
    CC.atomic_write_json(path, obj)


def atomic_write_text(path: Path, text: str) -> None:
    CC.atomic_write_text(path, text)


def log(msg: str) -> None:
    print(f"[{utcnow_iso()}] {msg}", flush=True)


class Blocker(Exception):
    """Campaign-stopping condition with a precise human reason."""


class AccessBlocked(Blocker):
    pass


class SessionBudget(Blocker):
    """Live lease too short for dispatch: rotate the session, keep the
    same (id, repetition) at the head of the work queue."""


class BoardSkipped(Exception):
    """A completed build landed on an excluded board; its session was
    released before any board I/O.  Not a Blocker: acquire_and_qualify
    rebuilds, bounded by MAX_BOARD_SKIPS."""


# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------

def organizer_identity() -> dict:
    files = ["run_pigweed_fpga.py", "pigweed_fpga_worker.py",
             "benchmark_cnf.py", "calibrate_cpu.py", "screen_pigweed.py"]
    return {f: sha256_file(os.path.join(HERE, f)) for f in files}


def snapshot_candidate(run_dir: Path) -> dict:
    """Freeze design.py, satlat/, LICENSE, NOTICE and the public qualification
    fixtures into RUN/candidate.  Refuses any checkout modification — tracked
    or untracked — under the candidate paths: the frozen contract means the
    recorded HEAD's bytes, nothing else."""
    src = Path(CANDIDATE_SRC)
    head = subprocess.run(["git", "-C", str(src), "rev-parse", "HEAD"],
                          capture_output=True, text=True, timeout=30)
    if head.returncode != 0:
        raise Blocker(f"cannot read PigWeedSAT HEAD: {head.stderr.strip()}")
    head = head.stdout.strip()
    if head != CANDIDATE_GIT_HEAD:
        raise Blocker(f"PigWeedSAT HEAD is {head}, expected {CANDIDATE_GIT_HEAD}")
    status = subprocess.run(["git", "-C", str(src), "status", "--porcelain"],
                            capture_output=True, text=True, timeout=30)
    if status.returncode != 0:
        raise Blocker("cannot read PigWeedSAT git status "
                      f"(rc={status.returncode}): {status.stderr.strip()[:200]}")
    offending = []
    for line in status.stdout.splitlines():
        # porcelain: XY PATH  or  XY ORIG -> PATH (renames carry both sides)
        paths = line[3:]
        for path in [p.strip().strip('"') for p in paths.split("->")]:
            if any(path == p or path.startswith(p) for p in CANDIDATE_PATHS):
                offending.append(line)
                break
    if offending:
        raise Blocker("PigWeedSAT checkout has modifications under candidate "
                      f"paths ({offending[:4]}); the frozen contract refuses "
                      "a modified candidate")
    dst = run_dir / "candidate"
    (dst / "satlat").mkdir(parents=True, exist_ok=True)
    copies = {"design.py": src / "design.py",
              "LICENSE": src / "LICENSE", "NOTICE": src / "NOTICE"}
    for d, f in QUAL_FIXTURES:
        (dst / d).mkdir(parents=True, exist_ok=True)
        copies[f"{d}/{f}"] = src / "SAT_test_cases" / d / f
    hashes = {}
    for rel, p in sorted(copies.items()):
        if not p.is_file():
            raise Blocker(f"candidate source missing: {p}")
        shutil.copyfile(p, dst / rel)
        hashes[rel] = sha256_file(dst / rel)
    for p in sorted((src / "satlat").glob("*.py")):
        shutil.copyfile(p, dst / "satlat" / p.name)
        hashes[f"satlat/{p.name}"] = sha256_file(dst / "satlat" / p.name)
    return {"git_head": head, "checkout_clean": True,
            "dirty_paths": [], "files": hashes}


def candidate_identity(run_dir: Path) -> dict:
    dst = run_dir / "candidate"
    return {str(p.relative_to(dst)): sha256_file(p)
            for p in sorted(dst.rglob("*")) if p.is_file()}


def sdk_identity() -> dict:
    import importlib.metadata as md
    from importlib.util import find_spec
    pkg = Path(find_spec("manhattan_reasoning_gym").origin).parent
    commit = None
    for info in pkg.parent.glob("manhattan_reasoning_gym-*.dist-info"):
        direct = info / "direct_url.json"
        if direct.is_file():
            try:
                commit = json.loads(
                    direct.read_text())["vcs_info"]["commit_id"]
            except (OSError, ValueError, KeyError):
                pass
    tree = hashlib.sha256()
    for p in sorted(pkg.rglob("*.py")):
        tree.update(str(p.relative_to(pkg)).encode())
        tree.update(hashlib.sha256(p.read_bytes()).digest())
    return {
        "commit": commit,
        "version": md.version("manhattan-reasoning-gym"),
        "package_tree_sha256": tree.hexdigest(),
        "websockets": md.version("websockets"),
        "requests": md.version("requests"),
        "amaranth": md.version("amaranth"),
        "python": ".".join(map(str, sys.version_info[:3])),
    }


def config_identity() -> dict:
    return {
        "lit_page": LIT_PAGE, "sys_clk_hz": SYS_CLK_HZ,
        "timing_target_mhz": TIMING_TARGET_MHZ,
        "solve_timeout_s": SOLVE_TIMEOUT_S, "repetitions": REPETITIONS,
        "poll_s": POLL_S, "wd_load_s": WD_LOAD, "wd_go_s": WD_GO,
        "wd_readback_s": WD_READBACK, "wd_recovery_s": WD_RECOVERY,
        "wd_qualify_s": WD_QUALIFY, "lease_min_load_s": LEASE_MIN_LOAD,
        "lease_min_go_s": LEASE_MIN_GO,
        "lease_min_qualify_s": LEASE_MIN_QUALIFY,
        "build_ceiling_s": BUILD_CEILING_S, "sim_cycle_cap": SIM_CYCLE_CAP,
        "sandbox_digest": SANDBOX_DIGEST, "api_url": API_URL,
        "skip_fpga_ids": list(SKIP_FPGA_IDS),
        "max_board_skips": MAX_BOARD_SKIPS,
        "only_ids": list(ONLY_IDS) if ONLY_IDS is not None else None,
        "cpu_runs": CPU_RUNS,
    }


def apply_campaign(campaign: dict | None) -> None:
    """Set the campaign scope recorded in run.json (prepare writes it from
    the CLI; every later command applies it right after load_run, before
    any identity check).  None = the original campaign defaults.  The lease
    minima and the GO watchdog follow the solve deadline; a deadline whose
    load minimum cannot fit a fresh platform session is refused."""
    global SOLVE_TIMEOUT_S, REPETITIONS, ONLY_IDS, CPU_RUNS
    global LEASE_MIN_LOAD, LEASE_MIN_GO, WD_GO
    c = campaign or {}
    SOLVE_TIMEOUT_S = float(c.get("solve_timeout_s", 1000.0))
    REPETITIONS = int(c.get("repetitions", 3))
    ONLY_IDS = tuple(c["only_ids"]) if c.get("only_ids") is not None else None
    CPU_RUNS = c.get("cpu_runs") or [
        {"path": os.path.relpath(DEFAULT_CPU_RUN, HERE), "env": [1000, 3000]}]
    if SOLVE_TIMEOUT_S <= 0 or REPETITIONS < 1:
        raise Blocker("campaign needs a positive deadline and >= 1 repetition")
    LEASE_MIN_LOAD = SOLVE_TIMEOUT_S + LEASE_LOAD_OVERHEAD_S
    LEASE_MIN_GO = SOLVE_TIMEOUT_S + LEASE_GO_OVERHEAD_S
    WD_GO = SOLVE_TIMEOUT_S + WD_GO_SLACK_S
    fits = SESSION_TTL_S - ATTACH_QUALIFY_MARGIN_S
    if LEASE_MIN_LOAD > fits:
        raise Blocker(f"solve deadline {SOLVE_TIMEOUT_S:.0f} s needs "
                      f"{LEASE_MIN_LOAD:.0f} s of lease before load but a "
                      f"fresh {SESSION_TTL_S:.0f} s session leaves about "
                      f"{fits:.0f} s; max deadline is "
                      f"{fits - LEASE_LOAD_OVERHEAD_S:.0f} s")


def artifacts_identity(run_dir: Path) -> dict:
    """Hashes of every fixed run input that later phases must not see
    changed: the manifest snapshot, the private decompressed inputs, the
    CPU-evidence validation record, the inventory, and (once qualified) the
    qualification record."""
    out: dict[str, str | None] = {}
    for key, rel in (("manifest_snapshot", "manifest.snapshot.csv"),
                     ("evidence_validation", "evidence-validation.json"),
                     ("inventory", "inventory.csv"),
                     ("qualification", "qualification.json")):
        p = run_dir / rel
        out[key] = sha256_file(p) if p.is_file() else None
    inputs = run_dir / "inputs"
    if inputs.is_dir():
        tree = hashlib.sha256()
        for p in sorted(inputs.rglob("*")):
            if p.is_file():
                tree.update(str(p.relative_to(inputs)).encode())
                tree.update(hashlib.sha256(p.read_bytes()).digest())
        out["inputs_tree"] = tree.hexdigest()
    else:
        out["inputs_tree"] = None
    return out


def current_identity(run_dir: Path) -> dict:
    return {
        "organizer": organizer_identity(),
        "candidate": candidate_identity(run_dir),
        "sdk": sdk_identity(),
        "config": config_identity(),
        "artifacts": artifacts_identity(run_dir),
    }


def identity_mismatches(recorded: dict, current: dict) -> list[str]:
    errs = []
    for section in ("organizer", "candidate", "sdk", "config", "artifacts"):
        r, c = recorded.get(section, {}), current.get(section, {})
        for k in sorted(set(r) | set(c)):
            if r.get(k) != c.get(k):
                errs.append(f"{section}.{k}: run={r.get(k)} now={c.get(k)}")
    return errs


# ---------------------------------------------------------------------------
# Cloud adapter (documented routes; SDK-shaped; explicit timeouts)
# ---------------------------------------------------------------------------

class Cloud:
    def __init__(self, api_key: str, api_url: str = API_URL,
                 username: str = ""):
        if not api_key:
            raise AccessBlocked(
                "no API key resolved (MRG_API_KEY or `mrg login`); "
                "hardware phases blocked")
        self.key = api_key
        self.url = api_url
        # account name the control plane reports as board/session owner
        self.username = username
        self.skew_s: float | None = None   # server_date - local_time

    def _resp(self, resp):
        date = resp.headers.get("Date")
        skew = None
        if date:
            try:
                server = _dt.datetime.strptime(
                    date, "%a, %d %b %Y %H:%M:%S %Z"
                ).replace(tzinfo=_dt.timezone.utc).timestamp()
                skew = server - time.time()
            except ValueError:
                skew = None
        # a missing/unusable Date invalidates earlier skew evidence: the
        # lease gates must fail closed rather than trust a stale sample
        self.skew_s = skew
        if resp.status_code in (401, 403):
            raise AccessBlocked(f"HTTP {resp.status_code} at {resp.url}: "
                                f"{resp.text[:200]}")
        if resp.status_code in (402, 429):
            raise AccessBlocked(f"billing/quota (HTTP {resp.status_code}): "
                                f"{resp.text[:200]}")
        resp.raise_for_status()          # any other error fails closed
        return resp

    def _get(self, path: str, timeout=HTTP_TIMEOUT, headers=None):
        import requests
        h = {"X-API-Key": self.key}
        if headers:
            h.update(headers)
        return self._resp(requests.get(self.url + path, headers=h,
                                       timeout=timeout))

    def _post(self, path: str, timeout=HTTP_TIMEOUT, headers=None, **kw):
        import requests
        h = {"X-API-Key": self.key}
        if headers:
            h.update(headers)
        return self._resp(requests.post(self.url + path, headers=h,
                                        timeout=timeout, **kw))

    def usage(self) -> dict:
        return self._get("/usage").json()

    def check_quota(self) -> dict:
        u = self.usage()
        remaining = u.get("build_minutes_remaining")
        if type(remaining) is not int:
            raise AccessBlocked(
                "usage response lacks an integer build_minutes_remaining; "
                f"failing closed: {json.dumps(u)[:200]}")
        if remaining < BUILD_QUOTA_FLOOR_MIN:
            raise AccessBlocked(
                f"build quota {remaining} min remaining < floor "
                f"{BUILD_QUOTA_FLOOR_MIN} min (a build took 13 min): "
                f"{u.get('build_minutes_used')}/"
                f"{u.get('build_minutes_limit')} used")
        return u

    def list_jobs(self) -> list:
        return self._get("/jobs").json()

    def get_job(self, job_id: str) -> dict:
        return self._get(f"/jobs/{job_id}").json()

    def get_job_logs(self, job_id: str) -> str:
        return self._get(f"/jobs/{job_id}/logs").text

    def cancel_job(self, job_id: str) -> None:
        import requests
        self._resp(requests.delete(f"{self.url}/jobs/{job_id}",
                                   headers={"X-API-Key": self.key},
                                   timeout=HTTP_TIMEOUT))

    def get_fpga(self, fpga_id: int) -> dict:
        return self._get(f"/fpga/{fpga_id}").json()

    def list_fpgas(self) -> list:
        return self._get("/fpga").json()

    def get_session(self, fpga_id: int, session_id: str | None = None) -> dict:
        h = {"X-Session-ID": session_id} if session_id else {}
        return self._get(f"/fpga/{fpga_id}/session", headers=h).json()

    def submit_design(self, design_path: str) -> str:
        import requests
        with open(design_path, "rb") as f:
            resp = self._resp(requests.post(
                f"{self.url}/submit", headers={"X-API-Key": self.key},
                files={"file": f},
                data={"sys_clk_freq": str(SYS_CLK_HZ),
                      "timing_target_mhz": str(TIMING_TARGET_MHZ)},
                timeout=SUBMIT_TIMEOUT))
        return resp.json()["job_id"]

    def release(self, fpga_id: int, session_id: str) -> str:
        resp = self._post(f"/fpga/{fpga_id}/session/release",
                          headers={"X-Session-ID": session_id})
        return resp.json()["job_id"]


def resolve_api_key() -> str:
    """Normal App credential resolution; the value never leaves memory/logs."""
    env = os.environ.get("MRG_API_KEY")
    if env:
        return env
    try:
        from manhattan_reasoning_gym import _credentials
        return _credentials.load(API_URL) or ""
    except Exception:
        return ""


def resolve_username() -> str:
    """Account name stored by `mrg login` (not a secret); the control plane
    reports it as board/session owner.  If MRG_API_KEY overrides the stored
    key, the stored name may belong to a different account: return "" so the
    run fails closed before submitting."""
    try:
        from manhattan_reasoning_gym import _credentials
        env = os.environ.get("MRG_API_KEY")
        if env and env != _credentials.load(API_URL):
            return ""
        entry = _credentials._load_all().get(API_URL) or {}
        return entry.get("github_username") or ""
    except Exception:
        return ""


def account_lock(username: str):
    """Machine-wide exclusive lock per account so two harness invocations
    can never hold same-account builds/sessions at once."""
    tag = hashlib.sha256(username.encode()).hexdigest()[:16]
    base = Path(os.environ.get("TMPDIR") or "/tmp")
    fh = open(base / f"pigweed-fpga-account-{tag}.lock", "w")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        fh.close()
        raise Blocker("another harness invocation on this machine holds the "
                      "account lock; refusing to run concurrently")
    return fh


SESSION_TTL_S = 3600.0          # observed: session expiry = build completion + 1 h
SESSION_BIND_TOL_S = 5.0     # observed jitter 0.0007 s
COMPETING_BUILD_WINDOW_S = 60.0   # other own build on same board this close


def our_live_session(cloud, board: dict, job: dict,
                     journal) -> dict | None:
    """The board's session iff it is reserved, board AND session are owned by
    our account, and the session expiry is the build's completion time plus
    the fixed session TTL (the real control plane leaves current_job_id null
    for a build session and exposes no job pointer, so the expiry/updated_at
    relation is the only per-build provenance; observed offset 0.0007 s).
    Timing alone is not unique, so: (1) a session id already journaled for a
    session of this run is never attributed to another job, and (2) if any
    OTHER own build (another machine, direct SDK use, an unjournaled
    submit) reached the same board within COMPETING_BUILD_WINDOW_S the
    provenance is ambiguous and this blocks without mutation.  Builds
    whose session this run journaled by id are accounted for and ignored."""
    sess = board.get("session")
    me = getattr(cloud, "username", "")
    if not (me and sess and board.get("state") == "reserved"
            and board.get("owner") == me and sess.get("owner") == me
            and sess.get("session_id")):
        return None
    entries = journal.entries()
    if sess["session_id"] in {e.get("session_id") for e in entries
                              if e["type"] == "session_assigned"}:
        return None
    try:
        done = parse_utc(job["updated_at"])
        offset = parse_utc(sess["expires_at"]) - done
    except (KeyError, ValueError, TypeError, AttributeError):
        return None
    if abs(offset - SESSION_TTL_S) > SESSION_BIND_TOL_S:
        return None
    accounted = {e.get("job_id") for e in entries
                 if e["type"] == "session_assigned"}
    for other in cloud.list_jobs():
        if other.get("job_id") == job.get("job_id") or \
                other.get("job_id") in accounted or \
                other.get("fpga_id") != job.get("fpga_id") or \
                other.get("type") != job.get("type"):
            continue
        try:
            gap = abs(parse_utc(other["updated_at"]) - done)
        except (KeyError, ValueError, TypeError, AttributeError):
            gap = 0.0                  # unparseable competitor: ambiguous
        if gap <= COMPETING_BUILD_WINDOW_S:
            raise Blocker(
                f"own build {other.get('job_id')} reached board "
                f"{job.get('fpga_id')} within {COMPETING_BUILD_WINDOW_S:.0f}"
                f" s of {job.get('job_id')}; session provenance is "
                "ambiguous; resolve manually (no release/attach performed)")
    return sess


def conservative_remaining_s(expires_at: float | None, cloud,
                             latency_s: float) -> float | None:
    """Server expiry minus local now, minus observed request latency and any
    conservative clock skew.  Missing expiry OR missing clock-skew evidence
    -> None (dispatch blocked)."""
    if expires_at is None or cloud is None or cloud.skew_s is None:
        return None
    rem = expires_at - time.time() - latency_s
    if cloud.skew_s > 0:
        rem -= cloud.skew_s
    return rem


# ---------------------------------------------------------------------------
# Journal (durable, fsynced, monotonic seq)
# ---------------------------------------------------------------------------

class Journal:
    def __init__(self, run_dir: Path):
        self.path = run_dir / "journal.jsonl"
        self._fh = None
        self._n = len(self.entries())

    def entries(self) -> list[dict]:
        if not self.path.is_file():
            return []
        out = []
        for line in self.path.read_text().splitlines():
            if not line.strip():
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                out.append({"type": "journal_corrupt", "raw": line[:200]})
        return out

    def append(self, etype: str, **fields) -> dict:
        created = not self.path.exists()
        if self._fh is None:
            self._fh = open(self.path, "a")
        rec = {"seq": self._n + 1, "type": etype,
               "ts_utc": utcnow_iso(), "t_mono": time.monotonic(), **fields}
        self._fh.write(json.dumps(rec, sort_keys=True, default=str) + "\n")
        self._fh.flush()
        os.fsync(self._fh.fileno())
        if created:
            dfd = os.open(str(self.path.parent), os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        self._n += 1
        return rec

    def close(self):
        if self._fh:
            self._fh.close()
            self._fh = None


def run_lock(run_dir: Path):
    """Exclusive flock on the run dir; second invocation fails up front."""
    run_dir.mkdir(parents=True, exist_ok=True)
    fh = open(run_dir / ".lock", "w")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        raise Blocker(f"another invocation holds the lock on {run_dir}")
    return fh


def load_run(run_dir: Path) -> dict:
    p = run_dir / "run.json"
    if not p.is_file():
        raise Blocker(f"{run_dir} has no run.json; run prepare first")
    run = json.loads(p.read_text())
    # the run's campaign scope (deadline, repetitions, ids, CPU sources)
    # must be in force before any identity check or dispatch
    apply_campaign(run.get("campaign"))
    return run


def gate_identity(run_dir: Path, run: dict) -> None:
    sdk = sdk_identity()
    if sdk.get("commit") != PINNED_SDK_COMMIT:
        raise Blocker(f"installed SDK commit {sdk.get('commit')} != pinned "
                      f"{PINNED_SDK_COMMIT}; refusing")
    errs = identity_mismatches(run["identity"], current_identity(run_dir))
    if errs:
        raise Blocker("identity mismatch, refusing to continue:\n  "
                      + "\n  ".join(errs[:12])
                      + ("\n  ..." if len(errs) > 12 else ""))


def gate_phase(run: dict, need: str) -> None:
    phase = run.get("phase")
    if PHASE_RANK.get(phase, -1) < PHASE_RANK[need]:
        raise Blocker(f"run phase is {phase!r}; needs >= {need!r}")


def update_run(run_dir: Path, run: dict, **fields) -> None:
    run.update(fields)
    run["updated_utc"] = utcnow_iso()
    atomic_write_json(run_dir / "run.json", run)


# ---------------------------------------------------------------------------
# prepare
# ---------------------------------------------------------------------------

def screen_worker_main(cnf_path: str, candidate_root: str,
                       result_path: str) -> int:
    """Separate `python -B` process: frozen candidate parse + P=16 build."""
    import traceback
    out = {"candidate_parse": None, "build": None, "caps": None}
    try:
        sys.path.insert(0, candidate_root)
        import satlat.host as host
        import design as D
        out["caps"] = {k: getattr(D, k) for k in
                       ("N_MAX", "C_MAX", "LE_MAX", "CE_MAX", "MAX_LEARN",
                        "NUM_W", "REM_W", "FREE_W", "VAR_W", "LVL_W")}
        nv, clauses = host.parse_dimacs(cnf_path)
        out["candidate_parse"] = {"num_vars": nv, "clauses": clauses}
        try:
            cfg = host.pigweed(host.Config(lit_page=LIT_PAGE))
            img = host.build_images(nv, clauses, cfg)
            out["build"] = {
                "status": "loaded",
                "lit_words": img.lit_elems, "cls_words": img.cls_elems,
                "fixed_height": img.fixed_height,
                "clause_ids_used": img.num_clauses,
            }
        except host.Unsupported as e:
            out["build"] = {"status": "unsupported", "reason": str(e)}
    except Exception as e:                        # noqa: BLE001
        out["candidate_parse"] = {"crash": f"{type(e).__name__}: {e}",
                                  "trace": traceback.format_exc()[-1500:]}
    with open(result_path, "w") as f:
        json.dump(out, f)
    return 0


def run_screen_subprocess(cnf_path: str, candidate_root: str) -> dict:
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        result = os.path.join(td, "result.json")
        try:
            proc = subprocess.run(
                [sys.executable, "-B", os.path.abspath(__file__),
                 "--screen-worker", cnf_path, candidate_root, result],
                capture_output=True, text=True, timeout=600,
                env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
        except subprocess.TimeoutExpired:
            return {"worker_crash": "timeout after 600s"}
        if proc.returncode != 0 or not os.path.isfile(result):
            return {"worker_crash":
                    f"exit {proc.returncode}: {proc.stderr.strip()[-500:]}"}
        with open(result) as f:
            return json.load(f)


def expected_from_cert(certified: bool, kind: str | None) -> tuple[str | None, str | None]:
    """Expected verdict from certificate kind, never from result_gbd alone."""
    if not certified:
        return None, "cpu campaign did not certify this instance"
    if kind == "sat_witness":
        return "sat", None
    if kind in ("unsat_proof", "unsat_trivial_original"):
        return "unsat", None
    return None, f"uncertified kind {kind!r}"


def pick_cpu_evidence(iid: str, sources: list) -> tuple[dict | None, tuple]:
    """The latest CPU run (last in `sources`) holding a terminal record for
    `iid` supplies its evidence; earlier runs are fallbacks.  Returns the
    record (or None) and its (dir, env, rel) source."""
    for cdir, env, rel in reversed(sources):
        rec, _n = CC.latest_terminal_attempt(cdir, iid)
        if rec is not None:
            return rec, (cdir, env, rel)
    return None, sources[0]


def derive_eligibility(semantics_status: str, field_flags: list,
                       build_status: str, certified: bool,
                       certified_kind: str | None) -> tuple[str | None, list]:
    """Eligibility = fresh encoder acceptance AND semantics/field safety AND
    validated CPU certification.  Expected verdict comes from the
    certificate kind, never the manifest label alone."""
    expected, _why = expected_from_cert(certified, certified_kind)
    reasons = []
    if semantics_status != "ok":
        reasons.append("input_semantics_error")
    if field_flags:
        reasons.append("field_limit")
    if build_status != "loaded":
        reasons.append("non_fitting")
    if expected is None:
        reasons.append("uncertified")
    return expected, reasons


def cmd_prepare(out: str, campaign: dict | None = None) -> int:
    run_dir = Path(out).resolve()
    if run_dir.exists() and any(run_dir.iterdir()):
        raise Blocker(f"{run_dir} exists and is not empty; prepare refuses")
    apply_campaign(campaign)       # refuses a deadline that cannot fit a lease
    run_dir.mkdir(parents=True)
    _lock = run_lock(run_dir)      # held (bound) for the whole command
    log(f"prepare: freezing candidate from {CANDIDATE_SRC}")
    snapshot = snapshot_candidate(run_dir)
    run = {"schema": RUN_SCHEMA, "phase": "prepared",
           "created_utc": utcnow_iso(),
           "cpu_run": os.path.relpath(DEFAULT_CPU_RUN, HERE),
           "manifest": os.path.relpath(DEFAULT_MANIFEST, HERE),
           "candidate_snapshot": snapshot,
           "campaign": {"solve_timeout_s": SOLVE_TIMEOUT_S,
                        "repetitions": REPETITIONS,
                        "only_ids": (list(ONLY_IDS) if ONLY_IDS is not None
                                     else None),
                        "cpu_runs": CPU_RUNS}}
    atomic_write_json(run_dir / "run.json", run)

    shutil.copyfile(DEFAULT_MANIFEST, run_dir / "manifest.snapshot.csv")
    rows = CC.load_manifest(str(run_dir / "manifest.snapshot.csv"))
    if len(rows) != 210:
        raise Blocker(f"manifest has {len(rows)} rows, expected 210")
    log("prepare: preflighting 210 instances (hash + strict parse + counts)")
    inputs_dir = run_dir / "inputs"
    inputs_dir.mkdir()
    preflight_errors = []
    for row in rows:
        data, err = CC.preflight_instance(row, HERE)
        if err:
            preflight_errors.append(f"{row['id']}: {err}")
            continue
        atomic_write_text(inputs_dir / f"{row['id']}.cnf",
                          data.decode("utf-8"))
    if preflight_errors:
        raise Blocker("preflight failed (corruption stops cloud use):\n  "
                      + "\n  ".join(preflight_errors[:10]))

    if ONLY_IDS is not None:
        unknown = sorted(set(ONLY_IDS) - {r["id"] for r in rows})
        if unknown:
            raise Blocker(f"--only-ids not in the manifest: {unknown}")
    log("prepare: validating retained CPU evidence through pure helpers")
    # later CPU runs override earlier ones per id: an id's evidence comes
    # from the last run holding a terminal record for it, validated against
    # that run's own solve/check limits
    sources = [(Path(HERE) / c["path"], tuple(c["env"]), c["path"])
               for c in CPU_RUNS]
    for cdir, _env, rel in sources:
        if not (cdir / "run.json").is_file():
            raise Blocker(f"CPU run {rel} has no run.json")
    validator_hash = {
        "benchmark_cnf": sha256_file(os.path.join(HERE, "benchmark_cnf.py")),
        "calibrate_cpu": sha256_file(os.path.join(HERE, "calibrate_cpu.py"))}
    ev = {"validator": validator_hash,
          "cpu_run_identity": {
              rel: json.loads((cdir / "run.json").read_text())["identity"]
              for cdir, _env, rel in sources},
          "note": ("re-validation of the retained CPU runs with the organizer "
                   "parser; later runs override earlier ones per id; no "
                   "Kissat/drat-trim rerun, no FPGA proof claimed"),
          "instances": {}}
    cert_errors = []
    for row in rows:
        iid = row["id"]
        try:
            rec, src = pick_cpu_evidence(iid, sources)
            errs = CC.validate_record_evidence(src[0], rec, row,
                                               src[1]) if rec else []
        except (ValueError, OSError) as e:
            cert_errors.append(f"{iid}: {e}")
            continue
        if errs:
            cert_errors.extend(errs)
            continue
        ev["instances"][iid] = {
            "cpu_run": src[2] if rec else None,
            "attempt": rec["attempt"] if rec else None,
            "certified": bool(rec and rec.get("certified")),
            "certified_kind": rec.get("certified_kind") if rec else None,
        }
    atomic_write_json(run_dir / "evidence-validation.json", ev)
    if cert_errors:
        raise Blocker("CPU evidence corruption blocks cloud use:\n  "
                      + "\n  ".join(cert_errors[:10]))

    log("prepare: fresh encoder screen at P=16 (frozen candidate subprocess)")
    screen = {}
    for row in rows:
        iid = row["id"]
        cnf_path = str(inputs_dir / f"{iid}.cnf")
        strict = BC.parse_dimacs(Path(cnf_path).read_text())
        max_len = max((len(c) for c in strict.clauses), default=0)
        res = run_screen_subprocess(cnf_path, str(run_dir / "candidate"))
        if "worker_crash" in res:
            screen[iid] = {"semantics_status": "candidate_crash",
                           "detail": res["worker_crash"],
                           "build_status": "crash", "field_flags": []}
            continue
        cp = res.get("candidate_parse") or {}
        if "crash" in cp:
            screen[iid] = {"semantics_status": "candidate_crash",
                           "detail": cp["crash"], "build_status": "crash",
                           "field_flags": []}
            continue
        sem = SP.compare_semantics(strict.num_vars, strict.clauses,
                                   cp["num_vars"], cp["clauses"])
        b = res.get("build") or {}
        caps = res.get("caps") or {}
        flags = SP.field_flags(caps, max_len, b.get("fixed_height", 0))
        occ = SP.occupancy(b, caps) if (b.get("status") == "loaded"
                                        and caps) else None
        screen[iid] = {
            "semantics_status": sem["status"], "detail": sem["detail"],
            "empty_clauses_in_original": sem["empty_clauses_in_original"],
            "build_status": b.get("status", "missing"),
            "build_reason": b.get("reason"),
            "unsupported_bucket": (SP.classify_unsupported(b["reason"])
                                   if b.get("reason") else None),
            "field_flags": flags,
            "occupancy": ({k: round(v, 6) for k, v in occ.items()}
                          if occ else None),
        }
    atomic_write_json(run_dir / "screen-p16.json", screen)

    log("prepare: deriving eligibility inventory (all 210 rows retained)")
    inventory = []
    for row in rows:
        iid = row["id"]
        sc = screen[iid]
        evi = ev["instances"][iid]
        expected, reasons = derive_eligibility(
            sc["semantics_status"], sc["field_flags"], sc["build_status"],
            evi["certified"], evi["certified_kind"])
        inventory.append({
            "id": iid, "tier": row["tier"], "family": row["family"],
            "category": row["category"], "result_gbd": row["result_gbd"],
            "expected": expected or "",
            "eligible": "true" if not reasons else "false",
            "reasons": ";".join(reasons),
            "semantics_status": sc["semantics_status"],
            "build_status": sc["build_status"],
            "field_flags": ";".join(sc["field_flags"]),
            "cpu_certified": str(evi["certified"]).lower(),
            "cpu_certified_kind": evi["certified_kind"] or "",
            "cpu_attempt": evi["attempt"] or "",
            "cpu_run": evi["cpu_run"] or "",
            "pilot": "true" if iid in PILOT_IDS else "false",
        })
    write_inventory(run_dir / "inventory.csv", inventory)
    run["identity"] = {"organizer": organizer_identity(),
                       "candidate": candidate_identity(run_dir),
                       "sdk": sdk_identity(), "config": config_identity(),
                       "artifacts": artifacts_identity(run_dir)}
    update_run(run_dir, run,
               counts={"rows": len(inventory),
                       "eligible": sum(1 for r in inventory
                                       if r["eligible"] == "true")})
    bad_pilots = [p for p in PILOT_IDS
                  if not any(r["id"] == p and r["eligible"] == "true"
                             for r in inventory)]
    if bad_pilots:
        raise Blocker(f"fixed pilots not eligible: {bad_pilots}; changed "
                      "eligibility is a blocker, not a replacement license")
    n_elig = sum(1 for r in inventory if r["eligible"] == "true")
    scope = ""
    if ONLY_IDS is not None:
        planned = sum(1 for r in inventory
                      if r["eligible"] == "true" and r["id"] in ONLY_IDS)
        scope = f"; {planned}/{len(ONLY_IDS)} requested ids eligible"
    log(f"prepare: done; eligible={n_elig}/210{scope}")
    return 0


def write_inventory(path: Path, inventory: list[dict]) -> None:
    fields = ["id", "tier", "family", "category", "result_gbd", "expected",
              "eligible", "reasons", "semantics_status", "build_status",
              "field_flags", "cpu_certified", "cpu_certified_kind",
              "cpu_attempt", "cpu_run", "pilot"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(inventory)


def load_inventory(run_dir: Path) -> list[dict]:
    with open(run_dir / "inventory.csv", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ---------------------------------------------------------------------------
# qualify: RTL scenarios + capped pilots + local PnR
# ---------------------------------------------------------------------------

def rtl_worker_main(spec_path: str) -> int:
    """Subprocess over the frozen candidate's rtlsim (amaranth)."""
    import traceback
    with open(spec_path) as f:
        spec = json.load(f)
    sys.path.insert(0, spec["candidate_root"])
    out: dict = {"mode": spec["mode"]}
    try:
        import satlat.host as host
        import satlat.rtlsim as rtlsim
        cfg = host.pigweed(host.Config(lit_page=LIT_PAGE))
        if spec["mode"] == "semantic":
            formulas = spec["formulas"]
            imgs = [host.build_images(nv, cl, cfg) for nv, cl in formulas]
            outs = rtlsim.simulate(imgs)
            seq = []
            ok = True
            for (nv, cl), want, o in zip(formulas, spec["wants"], outs):
                original = BC.parse_dimacs(
                    "p cnf %d %d\n%s" % (nv, len(cl),
                                         "".join(" ".join(map(str, c)) + " 0\n"
                                                 for c in cl)))
                good = False
                if o.get("result") == 1 and want == "sat":
                    stack = o.get("stack") or []
                    good = (not WK.stack_problems(nv, stack)
                            and BC.check_witness(
                                original,
                                host.model_from_stack(nv, stack)))
                elif o.get("result") == 0 and want == "unsat":
                    good = True
                ok &= good
                seq.append({"formula": cl, "want": want,
                            "got": o.get("result"), "valid": good})
            out.update(ok=ok, sequence=seq)
        elif spec["mode"] == "abort":
            paths = spec["paths"]
            imgs = [host.load_cnf(p, cfg) for p in paths]
            outs = rtlsim.simulate(imgs, abort_after={
                i: at for i, at in enumerate(spec["abort_after"])
                if at is not None})
            recs = []
            ok = True
            for i, (p, o) in enumerate(zip(paths, outs)):
                if spec["expect_aborted"][i]:
                    good = bool(o.get("aborted")) and o.get("ctrl") == 0 \
                        and o.get("result") == 0
                else:
                    img = imgs[i]
                    original = BC.parse_dimacs(Path(p).read_text())
                    good = validate_sim_answer(
                        host, img, original, o, spec["expected"][i])
                ok &= good
                recs.append({"path": os.path.basename(p),
                             "aborted": bool(o.get("aborted")),
                             "result": o.get("result"), "valid": good,
                             "abort_cycles": o.get("abort_cycles")})
            out.update(ok=ok, records=recs)
        elif spec["mode"] == "pilot":
            p = spec["cnf"]
            img = host.load_cnf(p, cfg)
            original = BC.parse_dimacs(Path(p).read_text())
            try:
                o = rtlsim.simulate(img, max_cycles=spec["max_cycles"],
                                    poll_every=spec.get("poll_every", 2000))
            except TimeoutError as e:
                # nominal simulation cap: information, not a wrong answer
                out.update(ok=True, cap_exceeded=True, cap_reason=str(e),
                           sim_cycle_cap=spec["max_cycles"])
            else:
                good = validate_sim_answer(host, img, original, o,
                                           spec["expected"])
                out.update(ok=good, cap_exceeded=False,
                           result=o.get("result"),
                           stats=o.get("stats"), cyc=o.get("cyc"),
                           sim_cycle_cap=spec["max_cycles"],
                           poll_every=spec.get("poll_every", 2000))
        else:
            out.update(ok=False, reason=f"unknown mode {spec['mode']}")
    except Exception as e:                            # noqa: BLE001
        out.update(ok=False, crash=f"{type(e).__name__}: {e}",
                   trace=traceback.format_exc()[-2000:])
    with open(spec["result"], "w") as f:
        json.dump(out, f, indent=1, sort_keys=True, default=str)
    return 0


def validate_sim_answer(host, img, original: BC.CNF, out: dict,
                        expected: str) -> bool:
    res = out.get("result")
    if res == 1 and expected == "sat":
        stack = out.get("stack") or []
        if WK.stack_problems(img.num_vars, stack):
            return False
        return BC.check_witness(
            original, host.model_from_stack(img.num_vars, stack))
    return res == 0 and expected == "unsat"


def run_rtl_subprocess(spec: dict, timeout: float) -> dict:
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        spec = {**spec, "result": os.path.join(td, "result.json")}
        spec_path = os.path.join(td, "spec.json")
        with open(spec_path, "w") as f:
            json.dump(spec, f)
        try:
            proc = subprocess.run(
                [sys.executable, "-B", os.path.abspath(__file__),
                 "--rtl-worker", spec_path],
                capture_output=True, text=True, timeout=timeout,
                env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
        except subprocess.TimeoutExpired:
            return {"ok": False, "reason": f"process watchdog {timeout}s"}
        with open(spec["result"]) as f:
            out = json.load(f)
        out["proc_returncode"] = proc.returncode
        if proc.returncode != 0:
            out.setdefault("reason", proc.stderr.strip()[-500:])
            out["ok"] = False
        return out


def run_local_pnr(run_dir: Path) -> dict:
    mrg = Path(sys.executable).parent / "mrg"
    design = run_dir / "candidate" / "design.py"
    env_override = os.environ.get("MRG_SANDBOX_IMAGE", "")
    pinned = f"@{SANDBOX_DIGEST}"
    if env_override and not (env_override.endswith(pinned)
                             or env_override == SANDBOX_DIGEST):
        raise Blocker(f"MRG_SANDBOX_IMAGE={env_override!r} is not pinned to "
                      f"the approved digest (must end {pinned}); refusing "
                      "to route with an unapproved image")
    env = {**os.environ, "MRG_SANDBOX_IMAGE": env_override
           or f"ghcr.io/manhattanreasoning/mrg-sandbox@{SANDBOX_DIGEST}"}
    cmd = [str(mrg), "pnr", str(design), "--sys-clk-mhz", "50",
           "--timing-target-mhz", "50"]
    log(f"qualify: local PnR via official image digest {SANDBOX_DIGEST[:22]}…")
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              timeout=7200, env=env)
    except subprocess.TimeoutExpired:
        return {"ok": False, "reason": "PnR exceeded 7200s"}
    rep = {"command": " ".join(cmd), "returncode": proc.returncode,
           "stderr_tail": proc.stderr[-4000:]}
    try:
        rep["report"] = json.loads(proc.stdout)
    except json.JSONDecodeError:
        rep["ok"] = False
        rep["reason"] = f"no JSON report on stdout: {proc.stdout[:400]}"
        return rep
    r = rep["report"]
    checks = {
        "ok": proc.returncode == 0,
        "scope_soc": r.get("scope") == "soc",
        "fits": bool(r.get("fits")),
        "timing_met": bool(r.get("timing_met")),
        "sys_clk_50": float(r.get("sys_clk_mhz") or 0) == 50.0,
        "target_50": float(r.get("target_mhz") or 0) == 50.0,
    }
    rep["checks"] = checks
    rep["ok"] = all(checks.values())
    rep["image_digest"] = SANDBOX_DIGEST
    rep["design_source_sha256"] = sha256_file(design)
    rep["design_hash_reported"] = r.get("design_hash")
    rep["util"] = r.get("util")
    rep["fmax_mhz"] = r.get("fmax_mhz")
    rep["clock"] = r.get("clock")
    return rep


def cmd_qualify(out: str) -> int:
    run_dir = Path(out).resolve()
    _lock = run_lock(run_dir)      # held (bound) for the whole command
    run = load_run(run_dir)
    gate_phase(run, "prepared")
    gate_identity(run_dir, run)
    cand = run_dir / "candidate"
    qual = {"created_utc": utcnow_iso()}

    log("qualify: RTL semantic sequence (one DUT, SAT->UNSAT->SAT, n=2)")
    semantic = run_rtl_subprocess({
        "mode": "semantic", "candidate_root": str(cand),
        "formulas": [[2, [[1], [-1, 2]]], [2, [[1], [-1]]],
                     [2, [[1, 2], [-1, 2]]]],
        "wants": ["sat", "unsat", "sat"],
    }, RTL_SEQ_TIMEOUT)
    qual["rtl_semantic"] = semantic

    log("qualify: abort pattern (pret150_75 init/early/search, then solves)")
    fx = {f"{d}/{f}": str(cand / d / f) for d, f in QUAL_FIXTURES}
    abort = run_rtl_subprocess({
        "mode": "abort", "candidate_root": str(cand),
        "paths": [fx["unsat/pret150_75.dimacs"]] * 3
                 + [fx["sat/aim50.dimacs"], fx["sat/uf20.dimacs"],
                    fx["unsat/aalto_2.dimacs"]],
        "abort_after": [100, 6000, 40000, None, None, None],
        "expect_aborted": [True, True, True, False, False, False],
        "expected": [None, None, None, "sat", "sat", "unsat"],
    }, RTL_SEQ_TIMEOUT)
    qual["rtl_abort"] = abort

    log("qualify: six pilots under nominal sim cap 2,000,000 cycles")
    inv = {r["id"]: r for r in load_inventory(run_dir)}
    pilots = []
    for pid in PILOT_IDS:
        row = inv[pid]
        if row["eligible"] != "true":
            pilots.append({"id": pid, "ok": False,
                           "reason": "pilot not eligible (prepare blocker)"})
            continue
        r = run_rtl_subprocess({
            "mode": "pilot", "candidate_root": str(cand),
            "cnf": str(run_dir / "inputs" / f"{pid}.cnf"),
            "max_cycles": SIM_CYCLE_CAP, "poll_every": 2000,
            "expected": row["expected"]}, RTL_PILOT_TIMEOUT)
        r["id"] = pid
        pilots.append(r)
        log(f"qualify: pilot {pid}: ok={r.get('ok')} "
            f"cap_exceeded={r.get('cap_exceeded')}")
    qual["rtl_pilots"] = pilots

    log("qualify: local full-SoC PnR at 50 MHz")
    qual["pnr"] = run_local_pnr(run_dir)

    # commit-time identity recheck: the organizer must not have changed
    # while the (long) qualification ran, else this record could certify
    # results produced by different code than run.json pins
    gate_identity(run_dir, run)
    gates = {
        "rtl_semantic": bool(semantic.get("ok")),
        "rtl_abort": bool(abort.get("ok")),
        "rtl_pilots": all(p.get("ok") for p in pilots),
        "pnr": bool(qual["pnr"].get("ok")),
    }
    qual["gates"] = gates
    atomic_write_json(run_dir / "qualification.json", qual)
    if not all(gates.values()):
        failed = [k for k, v in gates.items() if not v]
        update_run(run_dir, run, phase="qualified_with_failures",
                   qualify_gates=gates)
        raise Blocker(f"qualification gates failed: {failed}; run is below "
                      "prepared and blocks cloud submission until fixed and "
                      "re-qualified (no platform/candidate edits authorized)")
    run["identity"]["artifacts"] = artifacts_identity(run_dir)
    update_run(run_dir, run, phase="qualified", qualify_gates=gates)
    log("qualify: all local gates passed")
    return 0


# ---------------------------------------------------------------------------
# run: campaign
# ---------------------------------------------------------------------------

class Session:
    def __init__(self, index, fpga_id, session_id, owner, expires_at,
                 build_job_id):
        self.index = index
        self.fpga_id = fpga_id
        self.session_id = session_id
        self.owner = owner
        self.expires_at = expires_at      # epoch float | None
        self.build_job_id = build_job_id
        self.qualified = False


def scan_attempts(run_dir: Path, iid: str, rep: int) -> tuple[list[dict], int]:
    base = run_dir / "attempts" / iid / f"repetition-{rep:03d}"
    attempts = []
    max_n = 0
    if not base.is_dir():
        return attempts, 0
    for d in sorted(base.iterdir()):
        if not d.is_dir() or not d.name.startswith("attempt-"):
            continue
        try:
            n = int(d.name.split("-")[1])
        except (IndexError, ValueError):
            continue
        max_n = max(max_n, n)
        p = d / "attempt.json"
        if p.is_file():
            try:
                attempts.append(json.loads(p.read_text()))
            except json.JSONDecodeError:
                attempts.append({"attempt": n, "status": "corrupt"})
    return attempts, max_n


def validated_terminal(run_dir: Path, iid: str, rep: int,
                       attempts: list[dict]) -> dict | None:
    """A terminal repetition counts as done only if its bindings still hold.
    wrong_answer/transport_error stay terminal (evidence is never rerun),
    but a recorded campaign halt refuses resumption separately."""
    for a in attempts:
        if a.get("status") not in TERMINAL_STATUS:
            continue
        if not isinstance(a.get("input"), dict):
            continue
        p = run_dir / a["input"].get("cnf_path", "")
        if not p.is_file() or sha256_file(p) != a["input"].get("cnf_sha256"):
            continue
        w = a.get("witness")
        if w:
            wp = (run_dir / "attempts" / iid / f"repetition-{rep:03d}"
                  / f"attempt-{a.get('attempt', 0):03d}" / w.get("path", ""))
            if not wp.is_file() or sha256_file(wp) != w.get("sha256"):
                continue
        return a
    return None


def rep_terminal(run_dir: Path, iid: str, rep: int,
                 attempts: list[dict]) -> dict | None:
    return validated_terminal(run_dir, iid, rep, attempts)


def build_work_plan(run_dir: Path) -> list[tuple[str, int]]:
    inv = load_inventory(run_dir)
    eligible = sorted(r["id"] for r in inv if r["eligible"] == "true"
                      and (ONLY_IDS is None or r["id"] in ONLY_IDS))
    plan = [(p, 1) for p in PILOT_IDS if p in eligible]
    plan += [(i, 1) for i in eligible if i not in PILOT_IDS]
    for rep in range(2, REPETITIONS + 1):
        plan += [(i, rep) for i in eligible]
    return plan


STOP = {"flag": False}


def cmd_run(out: str, resume: bool) -> int:
    run_dir = Path(out).resolve()
    _lock = run_lock(run_dir)      # held (bound) for the whole command
    run = load_run(run_dir)
    gate_phase(run, "qualified")
    if run.get("halt_reason"):
        raise Blocker(f"campaign halted earlier: {run['halt_reason']}; "
                      "clearing a halt requires an explicit human decision "
                      "(edit run.json only with justification)")
    gate_identity(run_dir, run)
    inv = {r["id"]: r for r in load_inventory(run_dir)}
    if not resume and (run_dir / "attempts").exists() and \
            any((run_dir / "attempts").iterdir()):
        raise Blocker("attempts exist; use --resume")
    update_run(run_dir, run, phase="running")

    api_key = resolve_api_key()
    cloud = Cloud(api_key, API_URL, resolve_username()) if api_key else None
    journal = Journal(run_dir)
    session: Session | None = None
    stop = STOP
    stop["flag"] = False

    def on_signal(signum, frame):
        stop["flag"] = True
        log(f"signal {signum} received; stopping after current bookkeeping")

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    halted = {"flag": False}

    def halt(reason: str):
        """Durable campaign halt: record why FIRST (so a failed release
        cannot lose the halt), then attempt the guarded board release."""
        nonlocal session
        halted["flag"] = True
        update_run(run_dir, run, phase="run_blocked", halt_reason=reason)
        if session is not None:
            release_session_guarded(run_dir, journal, cloud, session,
                                    reason="halt: " + reason)
            session = None
        raise Blocker(reason)

    acct_lock = None
    try:
        if cloud is None:
            raise AccessBlocked(
                "no credentials: complete `.venv-fpga/bin/mrg "
                "login` (device flow), then rerun with --resume")
        if not cloud.username:
            raise Blocker("no stored `mrg login` account name matching the "
                          "API key in use; cannot bind sessions to this "
                          "account")
        acct_lock = account_lock(cloud.username)
        reconcile(run_dir, journal, cloud)
        queue = deque(build_work_plan(run_dir))
        n_plan = len({i for i, _ in queue})
        log(f"run: plan {len(queue)} attempts ({n_plan} eligible ids x "
            f"{REPETITIONS} reps, pilots first; deadline "
            f"{SOLVE_TIMEOUT_S:.0f} s)")
        zero_progress_bookings = 0
        while queue:
            iid, rep = queue[0]
            if stop["flag"]:
                log("run: stop requested; draining")
                break
            attempts, max_n = scan_attempts(run_dir, iid, rep)
            if rep_terminal(run_dir, iid, rep, attempts) is not None:
                queue.popleft()     # validated terminal: never rerun
                continue
            row = inv[iid]
            attempt_n = max_n + 1
            while True:
                if session is None or not session.qualified:
                    session = acquire_and_qualify(run_dir, journal, cloud)
                rem = conservative_remaining_s(session.expires_at, cloud,
                                               HTTP_TIMEOUT[1])
                if rem is None:
                    raise Blocker("missing/unusable clock evidence; "
                                  "dispatch blocked")
                if rem >= LEASE_MIN_LOAD:
                    break
                log(f"run: lease {rem:.0f}s < {LEASE_MIN_LOAD:.0f}s; "
                    "rotating session")
                release_session_guarded(run_dir, journal, cloud, session,
                                        reason="lease_exhausted")
                session = None
                zero_progress_bookings += 1
                if zero_progress_bookings > 1:
                    raise Blocker(SESSION_BUDGET + ": repeated bookings "
                                  "with zero scored progress")
            try:
                rec = execute_attempt(run_dir, journal, cloud, session,
                                      row, rep, attempt_n, stop)
            except SessionBudget as sb:
                # live lease shortened server-side: rotate like a worker
                # session-budget outcome and keep this (id, rep) at the head
                log(f"run: {sb}")
                release_session_guarded(run_dir, journal, cloud, session,
                                       reason=SESSION_BUDGET)
                session = None
                zero_progress_bookings += 1
                if zero_progress_bookings > 1:
                    raise Blocker(SESSION_BUDGET + ": repeated bookings "
                                  "with zero scored progress")
                continue
            status = rec.get("status")
            if status == SESSION_BUDGET:
                release_session_guarded(run_dir, journal, cloud, session,
                                        reason=SESSION_BUDGET)
                session = None
                zero_progress_bookings += 1
                if zero_progress_bookings > 1:
                    raise Blocker(SESSION_BUDGET + ": repeated bookings "
                                  "with zero scored progress")
                continue          # same (id, rep) stays at the queue head
            if status == "wrong_answer":
                halt(f"wrong witness/certified-result contradiction at "
                     f"{iid} rep {rep}; campaign halted (board released)")
            if status == "transport_error":
                halt(f"transport failure at {iid} rep {rep}; no replay/"
                     "retry; campaign halted (board released)")
            if status == ATTEMPT_INTERRUPTED:
                halt(f"attempt interrupted mid-flight at {iid} rep {rep} "
                     f"({rec.get('reason')}); session state uncertain; "
                     "campaign halted (board released); resume with --resume")
            if (rec.get("result") or {}).get("recovery_failed"):
                halt(f"bounded recovery failed after {status} at {iid} rep "
                     f"{rep}; board health uncertain; campaign halted "
                     "(board released)")
            queue.popleft()
            zero_progress_bookings = 0
            regenerate_results(run_dir)
        if session is not None:
            release_session_guarded(run_dir, journal, cloud, session,
                                    reason="campaign_end")
            session = None
        regenerate_results(run_dir)
        pending = count_pending(run_dir)
        if pending:
            update_run(run_dir, run, phase="run_blocked",
                       blocked_reason=f"{pending} attempts not terminal")
            log(f"run: finished with {pending} pending (not_run retained)")
            return 2
        update_run(run_dir, run, phase="run_done")
        log("run: campaign complete")
        return 0
    finally:
        if session is not None and cloud is not None and not halted["flag"]:
            log("run: releasing session on exit path")
            release_session_guarded(run_dir, journal, cloud, session,
                                    reason="exit")
            session = None
        journal.close()
        if (run_dir / "inventory.csv").is_file():
            regenerate_results(run_dir)
        if acct_lock is not None:
            acct_lock.close()


def next_session_index(run_dir: Path) -> int:
    d = run_dir / "sessions"
    if not d.is_dir():
        return 1
    ns = [int(p.stem) for p in d.glob("*.json") if p.stem.isdigit()]
    return (max(ns) + 1) if ns else 1


def count_pending(run_dir: Path) -> int:
    n = 0
    for iid, rep in build_work_plan(run_dir):
        attempts, _ = scan_attempts(run_dir, iid, rep)
        if rep_terminal(run_dir, iid, rep, attempts) is None:
            n += 1
    return n


# ---------------------------------------------------------------------------
# Journal reconciliation (before any new submission or MMIO)
# ---------------------------------------------------------------------------

def delta_candidates(intent: dict, jobs: dict) -> list:
    t0 = parse_utc(intent["ts_utc"])
    out = []
    for j in jobs.values():
        if j.get("type") != "build_and_program":
            continue
        try:
            created = parse_utc(j["created_at"])
        except (KeyError, ValueError):
            continue
        if t0 - 5 <= created <= t0 + SUBMIT_INFLIGHT_WINDOW_S and \
                j["job_id"] not in intent.get("own_jobs_before", []):
            out.append(j)
    return out


def reconcile(run_dir: Path, journal: Journal, cloud: Cloud) -> None:
    entries = journal.entries()
    if not entries:
        return
    log(f"reconcile: {len(entries)} journal entries")
    if any(e["type"] == "journal_corrupt" for e in entries):
        raise Blocker("journal contains unparseable lifecycle records; an "
                      "unknown job/session may be live; resolve manually")
    jobs = {j["job_id"]: j for j in cloud.list_jobs()}
    accepted_intents = {e.get("submit_intent_seq") or e.get("ref")
                        for e in entries if e["type"] == "submit_accepted"}
    released_sessions = {e.get("session_id")
                         for e in entries if e["type"] in
                         ("release_terminal", "release_skip")}
    assigned_jobs = {e.get("job_id") for e in entries
                     if e["type"] == "session_assigned"}
    pending_release: dict[int, dict] = {}
    for e in entries:
        t = e["type"]
        if t == "submit_intent":
            if e["seq"] in accepted_intents or any(
                    a.get("job_id") and a.get("adopted_from") == e["seq"]
                    for a in entries if a["type"] == "submit_accepted") or \
                    any(x["type"] == "resolved" and x.get("ref") == e["seq"]
                        for x in entries):
                continue
            # the POST may still be in flight: wait out the window, then
            # re-list before declaring it never reached the server
            age = time.time() - parse_utc(e["ts_utc"])
            if age < SUBMIT_INFLIGHT_WINDOW_S:
                wait_s = SUBMIT_INFLIGHT_WINDOW_S - age
                log(f"reconcile: submit intent {e['seq']} possibly still "
                    f"in flight; waiting {wait_s:.0f}s before resolving")
                time.sleep(wait_s)
                jobs = {j["job_id"]: j for j in cloud.list_jobs()}
            cands = delta_candidates(e, jobs)
            if not cands:
                journal.append("resolved", ref=e["seq"],
                               note="submit never reached server (no "
                                    "matching own-job delta after the "
                                    "in-flight window)")
            elif cands:
                # Plan rule: a delta is only a CANDIDATE.  The job records
                # carry no design/source provenance, so no reliable binding
                # between this intent and any delta job exists -> block
                # without mutation.  Never cancel/release a guessed job.
                ids = ", ".join(c["job_id"] for c in cands)
                raise Blocker(
                    f"unbound submit intent seq={e['seq']}: own-job "
                    f"delta(s) {ids} could be this submission or an "
                    "unrelated same-account submit; resolve manually "
                    "(verify/cancel the job(s) yourself), then delete this "
                    "intent's pending state by journaling a resolved record")
        elif t == "submit_accepted":
            jid = e["job_id"]
            job = jobs.get(jid) or cloud.get_job(jid)
            if job is None:
                raise Blocker(f"job {jid} absent from own-job inventory")
            if job["status"] in ("queued", "running"):
                job = wait_build_terminal(cloud, journal, jid,
                                          run_dir=run_dir)
            if job["status"] == "complete":
                # a job whose session_assigned is journaled is handled by
                # exact session id below; never re-derive it from the board
                sess = None if jid in assigned_jobs else \
                    session_for_completed_job(cloud, job, journal)
                if sess is not None:
                    log(f"reconcile: releasing live session from job {jid}")
                    release_by_ids(run_dir, journal, cloud, sess,
                                   f"reconcile job {jid}")
            elif job["status"] not in ("failed", "cancelled"):
                raise Blocker(f"job {jid} unexpected status "
                              f"{job['status']!r}")
        elif t == "session_assigned":
            fid = e["fpga_id"]
            try:
                f = cloud.get_fpga(fid)
            except AccessBlocked:
                raise
            except Exception as exc:
                raise Blocker(f"cannot inspect board {fid}: {exc}")
            if f.get("session") and \
                    f["session"].get("session_id") == e["session_id"]:
                pending_release[fid] = e
            elif e["session_id"] in released_sessions:
                pass                       # cleanly released earlier
            elif not any(x["type"] == "session_expired_or_reassigned"
                         and x.get("session_id") == e["session_id"]
                         for x in entries):
                journal.append("session_expired_or_reassigned",
                               fpga_id=fid, session_id=e["session_id"],
                               current_session=(
                                   f.get("session") or {}
                               ).get("session_id"),
                               board_state=f.get("state"),
                               note="not ours anymore; no mutation")
        elif t == "release_intent":
            fid = e["fpga_id"]
            pending_release.setdefault(fid, e)
        elif t == "release_skip":
            pending_release.pop(e.get("fpga_id"), None)   # already resolved
        elif t == "release_accepted":
            fid = e.get("fpga_id")
            pending_release.pop(fid, None)
            job = wait_release_terminal(cloud, e["job_id"])
            if job["status"] != "complete":
                raise Blocker(f"release job {e['job_id']} ended "
                              f"{job['status']!r}; session may still be "
                              "reserved; resolve manually")
            board = cloud.get_fpga(fid)
            if (board.get("session") or {}).get("session_id") == \
                    e.get("session_id"):
                raise Blocker(f"release job {e['job_id']} reports complete "
                              f"but session {e.get('session_id')} is still "
                              "present on the board; resolve manually")
        elif t == "release_terminal":
            pending_release.pop(e.get("fpga_id"), None)
    for fid, e in pending_release.items():
        log(f"reconcile: finishing pending release for board {fid}")
        release_by_ids(run_dir, journal, cloud,
                       {"fpga_id": fid, "session_id": e["session_id"]},
                       "reconcile pending release")
    log("reconcile: clean")


def session_for_completed_job(cloud: Cloud, job: dict,
                              journal: Journal) -> dict | None:
    """A live session counts as ours only when the board is reserved and
    owned (board and session) by our account; see our_live_session."""
    fid = job.get("fpga_id")
    if fid is None:
        return None
    sess = our_live_session(cloud, cloud.get_fpga(fid), job, journal)
    if sess is None:
        return None
    return {"fpga_id": fid, "session_id": sess["session_id"],
            "owner": sess.get("owner"),
            "expires_at": sess.get("expires_at")}


def _cancel_and_settle(run_dir: Path, journal: Journal, cloud: Cloud,
                       job_id: str, reason: str) -> None:
    """Durable cancel of a recorded build, then reconcile any raced board
    assignment before returning.  Used by the build-ceiling path and by
    stop-during-build: the paid job must never keep running unattended."""
    journal.append("cancel_intent", job_id=job_id, reason=reason)
    try:
        cloud.cancel_job(job_id)
        journal.append("cancel_accepted", job_id=job_id)
    except Exception as exc:
        status = getattr(getattr(exc, "response", None), "status_code",
                         None)
        if status != 409:
            raise
        # 409: the build is already flashing / popped for flashing and can
        # no longer be cancelled -- exactly when a board is about to be
        # reserved.  Record the refusal and settle the assignment instead.
        journal.append("cancel_refused_409", job_id=job_id,
                       reason="build already flashing; not cancellable")
    job = _wait_cancel_terminal(cloud, job_id, observe_s=300.0)
    if job.get("fpga_id") is not None:
        raced = session_for_completed_job(cloud, job, journal)
        if raced is not None:
            release_by_ids(run_dir, journal, cloud, raced,
                           "board assigned to a build we cancelled "
                           "(or could not cancel: 409)")


def wait_build_terminal(cloud: Cloud, journal: Journal, job_id: str,
                        run_dir: Path | None = None) -> dict:
    """Adopt an in-flight build after restart: poll to its real terminal.
    On stop or restarted-ceiling expiry the recorded job is durably
    cancelled and any raced assignment released, never left running."""
    # the ceiling follows the SERVER's job clock (created_at), not this
    # process's restart time: a build that already consumed most of the
    # ceiling before the crash must not earn a fresh one
    job = cloud.get_job(job_id)
    ceiling_evidence_usable = True
    try:
        deadline_utc = parse_utc(job["created_at"]) + BUILD_CEILING_S
    except (KeyError, ValueError, AttributeError, TypeError):
        ceiling_evidence_usable = False
        deadline_utc = 0.0
    if cloud.skew_s is None:
        ceiling_evidence_usable = False
    if not ceiling_evidence_usable:
        # fail closed: without created_at + skew evidence we cannot bound
        # the paid build's age -- settle it now instead of granting a
        # fresh local ceiling
        if run_dir is not None:
            _cancel_and_settle(run_dir, journal, cloud, job_id,
                               "unusable build-ceiling evidence "
                               "(missing created_at or clock skew)")
        raise Blocker(f"build {job_id}: unusable ceiling evidence "
                      "(created_at/clock skew); "
                      "cancelled and settled" if run_dir is not None else
                      "cannot bound build age; resolve manually")
    last = None
    while True:
        if cloud.skew_s is None:
            # a poll response with a missing/unparseable Date cleared the
            # skew evidence: the ceiling is no longer provable, so settle
            # the build instead of continuing on an unbounded paid job
            if run_dir is not None:
                _cancel_and_settle(run_dir, journal, cloud, job_id,
                                   "clock-skew evidence lost mid-poll")
            raise Blocker(f"build {job_id}: clock-skew evidence lost "
                          "mid-poll; cancelled and settled"
                          if run_dir is not None else
                          "cannot bound build age; resolve manually")
        skew = cloud.skew_s
        server_now = time.time() + skew
        if server_now >= deadline_utc:
            break
        if STOP["flag"]:
            if run_dir is not None:
                _cancel_and_settle(run_dir, journal, cloud, job_id,
                                   "stop requested during adopted build")
            raise Blocker(f"stop requested while build {job_id} in flight; "
                          "job cancelled and settled" if run_dir is not None
                          else "job left running; resume reconciles it")
        job = cloud.get_job(job_id)
        if job["status"] != last:
            journal.append("build_state", job_id=job_id,
                           status=job["status"], fpga_id=job.get("fpga_id"))
            last = job["status"]
        if job["status"] in ("complete", "failed", "cancelled"):
            return job
        time.sleep(BUILD_POLL_S)
    if run_dir is not None:
        _cancel_and_settle(run_dir, journal, cloud, job_id,
                           "build ceiling (from server created_at) exceeded")
    raise Blocker(f"build {job_id} exceeded its "
                  f"{BUILD_CEILING_S:.0f}s ceiling "
                  "(server created_at + ceiling); cancelled; no retry loop")


def wait_release_terminal(cloud: Cloud, job_id: str,
                          observe_s: float = RELEASE_OBSERVE_S) -> dict:
    t0 = time.monotonic()
    while time.monotonic() - t0 < observe_s:
        job = cloud.get_job(job_id)
        if job["status"] in ("complete", "failed", "cancelled"):
            return job
        time.sleep(2.0)
    raise Blocker(f"release job {job_id} not terminal after {observe_s}s")


# ---------------------------------------------------------------------------
# Session acquire / qualify / release
# ---------------------------------------------------------------------------

def acquire_session(run_dir: Path, journal: Journal, cloud: Cloud) -> Session:
    if not cloud.username:
        raise Blocker("no stored `mrg login` account name; cannot bind a "
                      "session to this account; refusing to submit")
    cloud.check_quota()
    design = run_dir / "candidate" / "design.py"
    own_list = cloud.list_jobs()
    busy = [j["job_id"] for j in own_list
            if j.get("status") not in ("complete", "failed", "cancelled")]
    if busy:
        raise Blocker(f"own non-terminal jobs {busy}; refusing to submit "
                      "(session provenance requires a single in-flight "
                      "build)")
    held = [b.get("fpga_id") for b in cloud.list_fpgas()
            if (b.get("session") or {}).get("owner") == cloud.username]
    if held:
        raise Blocker(f"account already owns board(s) {held}; refusing to "
                      "submit (release or wait for TTL first)")
    own = {j["job_id"] for j in own_list}
    intent = journal.append("submit_intent", design_sha256=sha256_file(design),
                            sys_clk_hz=SYS_CLK_HZ,
                            timing_target_mhz=TIMING_TARGET_MHZ,
                            own_jobs_before=sorted(own))
    job_id = cloud.submit_design(str(design))
    journal.append("submit_accepted", job_id=job_id,
                   submit_intent_seq=intent["seq"])
    log(f"session: build job {job_id} submitted; polling to terminal")
    last = None
    t0 = time.monotonic()
    job = None
    while time.monotonic() - t0 < BUILD_CEILING_S:
        if STOP["flag"]:
            _cancel_and_settle(run_dir, journal, cloud, job_id,
                               "stop requested during build")
            raise Blocker(f"stop requested while build {job_id} in flight; "
                          "job cancelled and settled")
        job = cloud.get_job(job_id)
        if job["status"] != last:
            journal.append("build_state", job_id=job_id,
                           status=job["status"], fpga_id=job.get("fpga_id"))
            log(f"session: build {job['status']} fpga={job.get('fpga_id')}")
            last = job["status"]
        if job["status"] in ("complete", "failed", "cancelled"):
            break
        time.sleep(BUILD_POLL_S)
    else:
        _cancel_and_settle(run_dir, journal, cloud, job_id,
                           "build ceiling exceeded")
        raise Blocker(f"build {job_id} exceeded {BUILD_CEILING_S}s; "
                      "cancelled; no retry loop")
    save_job_evidence(run_dir, cloud, job_id)
    if job["status"] != "complete":
        raise Blocker(f"build job {job_id} {job['status']}; no retry loop")
    fid = job.get("fpga_id")
    if fid is None:
        raise Blocker(f"build {job_id} complete without board assignment")
    board = cloud.get_fpga(fid)
    sess = our_live_session(cloud, board, job, journal)
    if sess is None:
        raise Blocker(
            f"board {fid} (state={board.get('state')}, "
            f"owner={board.get('owner')!r}) is not reserved by our account "
            f"{cloud.username!r} for completed job {job_id}; "
            "refusing to attach")
    live = cloud.get_session(fid, sess["session_id"])
    if live.get("session_id") != sess["session_id"] or \
            live.get("owner") != cloud.username:
        raise Blocker(f"session endpoint for board {fid} disagrees with the "
                      "board record; refusing to attach")
    expires = live.get("expires_at")
    expires_at = parse_utc(expires) if expires else None
    journal.append("session_assigned", job_id=job_id, fpga_id=fid,
                   session_id=sess["session_id"], owner=sess.get("owner"),
                   expires_at=expires, board_state=board.get("state"))
    if fid in SKIP_FPGA_IDS:
        # excluded board: release before any board I/O, then rebuild
        journal.append("board_skipped", job_id=job_id, fpga_id=fid,
                       session_id=sess["session_id"],
                       reason="excluded board (SKIP_FPGA_IDS)")
        log(f"session: build {job_id} landed on excluded board {fid}; "
            "releasing before any board I/O")
        release_by_ids(run_dir, journal, cloud,
                       {"fpga_id": fid, "session_id": sess["session_id"]},
                       f"excluded board {fid}")
        raise BoardSkipped(f"board {fid} (build {job_id})")
    idx = next_session_index(run_dir)
    s = Session(idx, fid, sess["session_id"], sess.get("owner"),
                expires_at, job_id)
    atomic_write_json(run_dir / "sessions" / f"{idx:03d}.json", {
        "index": idx, "build_job_id": job_id, "fpga_id": fid,
        "session_id": s.session_id, "owner": s.owner,
        "expires_at": expires, "acquired_utc": utcnow_iso(),
        "clock_evidence": {
            "expires_at": expires, "skew_s": cloud.skew_s,
            "note": "expiry from GET /fpga/{id}/session; skew from server "
                    "Date headers; dispatch gates subtract latency+skew"},
        "qualified": False, "released": False})
    log(f"session #{idx}: board {fid} session {s.session_id[:8]}… "
        f"expires {expires}")
    return s


def _wait_cancel_terminal(cloud: Cloud, job_id: str,
                          observe_s: float = 60.0) -> dict:
    t0 = time.monotonic()
    while time.monotonic() - t0 < observe_s:
        job = cloud.get_job(job_id)
        if job["status"] in ("complete", "failed", "cancelled"):
            return job
        time.sleep(2.0)
    raise Blocker(f"cancelled job {job_id} not terminal after {observe_s}s; "
                  "resolve manually before new submissions")


def save_job_evidence(run_dir: Path, cloud: Cloud, job_id: str) -> None:
    d = run_dir / "build-logs"
    d.mkdir(exist_ok=True)
    try:
        atomic_write_json(d / f"{job_id}.job.json", cloud.get_job(job_id))
    except (AccessBlocked, OSError, ValueError):
        pass
    try:
        logs = cloud.get_job_logs(job_id)
        atomic_write_text(d / f"{job_id}.log",
                          logs if logs and logs.strip() != "" else
                          "(no logs available)")
    except (AccessBlocked, OSError):
        pass


def qualify_via_worker(run_dir: Path, journal: Journal, cloud: Cloud,
                       s: Session) -> dict:
    rem = conservative_remaining_s(s.expires_at, cloud, HTTP_TIMEOUT[1])
    if rem is None or rem < LEASE_MIN_QUALIFY:
        raise Blocker(f"{SESSION_BUDGET}: fresh lease has {rem} s, need "
                      f"{LEASE_MIN_QUALIFY} s for qualification + dispatch")
    inv = {r["id"]: r for r in load_inventory(run_dir)}
    spec = {
        "command": "qualify",
        "candidate_root": str(run_dir / "candidate"),
        "api_url": API_URL,
        "fpga_id": s.fpga_id,
        "session_id": s.session_id,
        "events_path": str(run_dir / "sessions" / f"{s.index:03d}.events.jsonl"),
        "result_path": str(run_dir / "sessions" / f"{s.index:03d}.qualify.json"),
        "stderr_path": str(run_dir / "sessions" / f"{s.index:03d}.stderr.log"),
        "_job_path": str(run_dir / "sessions" / f"{s.index:03d}.job.json"),
        "fixture_dir": str(run_dir / "candidate"),
        "pilot_cnfs": [(p, str(run_dir / "inputs" / f"{p}.cnf"))
                       for p in PILOT_IDS
                       if p in inv and inv[p]["eligible"] == "true"],
    }
    result, kill = supervise_worker(spec, {"stage": WD_QUALIFY},
                                    should_stop=lambda: STOP["flag"])
    journal.append("qualification", session=s.session_id,
                   fpga_id=s.fpga_id,
                   result_summary=summarize_result(result), killed=kill)
    if result is None or not result.get("ok"):
        raise Blocker("session failed on-board qualification: "
                      f"{json.dumps(summarize_result(result))[:400]}")
    s.qualified = True
    srec_path = run_dir / "sessions" / f"{s.index:03d}.json"
    srec = json.loads(srec_path.read_text())
    srec["qualified"] = True
    srec["qualified_utc"] = utcnow_iso()
    atomic_write_json(srec_path, srec)
    return result


def acquire_and_qualify(run_dir: Path, journal: Journal, cloud: Cloud) -> Session:
    skips = 0
    while True:
        try:
            s = acquire_session(run_dir, journal, cloud)
            break
        except BoardSkipped as bs:
            skips += 1
            if skips > MAX_BOARD_SKIPS:
                raise Blocker(f"{skips} consecutive builds landed on excluded "
                              f"board(s) {list(SKIP_FPGA_IDS)} (last: {bs}); "
                              "all released; stopping instead of rebuilding")
            log(f"session: excluded-board skip {skips}/{MAX_BOARD_SKIPS}; "
                "rebuilding")
    try:
        qualify_via_worker(run_dir, journal, cloud, s)
        rem = conservative_remaining_s(s.expires_at, cloud, HTTP_TIMEOUT[1])
        if rem is None or rem < LEASE_MIN_LOAD:
            raise Blocker(f"{SESSION_BUDGET}: after qualification only {rem} "
                          f"s left; need {LEASE_MIN_LOAD} s for a scored "
                          "attempt")
    except BaseException:
        # the paid board must never be left reserved on an acquisition error
        release_session_guarded(run_dir, journal, cloud, s,
                                reason="acquire_and_qualify error")
        raise
    return s


def release_by_ids(run_dir: Path, journal: Journal, cloud: Cloud,
                   sess: dict, reason: str) -> None:
    """Guarded release by known ids: never touches a session not ours."""
    fid, sid = sess["fpga_id"], sess["session_id"]
    board = cloud.get_fpga(fid)
    cur = (board.get("session") or {}).get("session_id")
    if cur != sid:
        journal.append("release_skip", fpga_id=fid, session_id=sid,
                       current_session=cur, reason=reason,
                       note="session differs or gone; no mutation")
        log(f"release: board {fid} session now {cur}; not ours -> no mutation")
        mark_session_released(run_dir, fid, sid, skipped=True)
        return
    journal.append("release_intent", fpga_id=fid, session_id=sid,
                   reason=reason)
    try:
        job_id = cloud.release(fid, sid)
    except AccessBlocked:
        raise
    except Exception as exc:
        if "404" in str(exc) or "not found" in str(exc).lower():
            journal.append("release_skip", fpga_id=fid, session_id=sid,
                           reason="already gone (404)")
            mark_session_released(run_dir, fid, sid, skipped=True)
            return
        raise Blocker(f"release POST failed for board {fid}: {exc}")
    journal.append("release_accepted", job_id=job_id, fpga_id=fid,
                   session_id=sid)
    job = wait_release_terminal(cloud, job_id)
    board = cloud.get_fpga(fid)
    gone = (board.get("session") or {}).get("session_id") != sid
    journal.append("release_terminal", job_id=job_id, fpga_id=fid,
                   job_status=job["status"], board_state=board.get("state"),
                   session_gone=gone)
    if job["status"] != "complete" or not gone:
        raise Blocker(f"release job {job_id} status={job['status']} "
                      f"session_gone={gone}; explicit blocker; no "
                      "force-idle/direct reset")
    mark_session_released(run_dir, fid, sid, skipped=False,
                          release_job=job_id)
    log(f"release: board {fid} reset complete (job {job_id})")


def release_session_guarded(run_dir: Path, journal: Journal, cloud: Cloud,
                            s: Session, reason: str) -> None:
    release_by_ids(run_dir, journal, cloud,
                   {"fpga_id": s.fpga_id, "session_id": s.session_id},
                   reason)


def mark_session_released(run_dir: Path, fid: int, sid: str, skipped: bool,
                          release_job: str | None = None) -> None:
    d = run_dir / "sessions"
    if not d.is_dir():
        return
    for p in sorted(d.glob("*.json")):
        try:
            rec = json.loads(p.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        if rec.get("fpga_id") == fid and rec.get("session_id") == sid \
                and not rec.get("released"):
            rec["released"] = True
            rec["released_utc"] = utcnow_iso()
            rec["release_skipped"] = skipped
            if release_job:
                rec["release_job_id"] = release_job
            atomic_write_json(p, rec)
            return


def summarize_result(res: dict | None) -> dict:
    if res is None:
        return {"status": "interrupted"}
    return {"status": res.get("status"), "result_code": res.get("result_code"),
            "cycles": res.get("cycles"), "late": res.get("late"),
            "stage": res.get("stage"), "recovery_failed":
                res.get("recovery_failed"),
            "transport_error": res.get("transport_error"),
            "ok": res.get("ok")}


# ---------------------------------------------------------------------------
# Worker supervision
# ---------------------------------------------------------------------------

def read_new_lines(path: str, pos: int) -> dict:
    out = []
    with open(path) as f:
        f.seek(pos)
        for line in f:
            line = line.strip()
            if line:
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
        pos = f.tell()
    return {"events": out, "pos": pos}


def supervise_worker(spec: dict, budgets: dict,
                     should_stop=None) -> tuple[dict | None, str | None]:
    """Spawn the worker child; enforce parent watchdogs from its event
    stream; TERM then KILL after 5 s if hung; reap.  Returns (result, kill).

    Watchdog modes are mutually exclusive: a whole-stage budget (e.g.
    qualification) OR the solve-phase ladder (load/GO/readback/recovery).
    The child fsyncs its result before emitting worker_done, so waiting for
    that event guarantees the result is durable when present.  Child stderr
    goes to a file, never an undrained pipe."""
    job_path = spec["_job_path"]
    events_path = spec["events_path"]
    result_path = spec["result_path"]
    atomic_write_json(Path(job_path), spec)
    open(events_path, "a").close()
    stderr_path = spec.get("stderr_path") or (events_path + ".stderr")
    stderr_fh = open(stderr_path, "a")
    proc = subprocess.Popen(
        [sys.executable, "-B", os.path.join(HERE, "pigweed_fpga_worker.py"),
         "--job", job_path],
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        stdout=subprocess.DEVNULL, stderr=stderr_fh)
    kill_reason = None
    try:
        phases = {"go_intent": None, "observed": None, "recovery_start": None}
        t_start = time.monotonic()
        pos = 0
        done_seen = False
        while proc.poll() is None:
            chunk = read_new_lines(events_path, pos)
            pos = chunk["pos"]
            for ev in chunk["events"]:
                name = ev.get("event")
                if name == "worker_start":
                    t_start = ev.get("t_mono", t_start)
                elif name in phases and phases[name] is None:
                    phases[name] = ev.get("t_mono", time.monotonic())
                elif name == "worker_done":
                    done_seen = True
            if done_seen:
                break
            if should_stop is not None and should_stop():
                kill_reason = "stop requested"
            now = time.monotonic()
            if "stage" in budgets:
                if now - t_start > budgets["stage"]:
                    kill_reason = f"stage watchdog {budgets['stage']:.0f}s"
            else:
                if phases["go_intent"] is None and now - t_start > WD_LOAD:
                    kill_reason = f"load watchdog {WD_LOAD:.0f}s"
                elif phases["go_intent"] is not None and \
                        phases["observed"] is None and \
                        now - phases["go_intent"] > WD_GO:
                    kill_reason = f"GO watchdog {WD_GO:.0f}s"
                elif phases["observed"] is not None and \
                        phases["recovery_start"] is None and \
                        now - phases["observed"] > WD_READBACK:
                    kill_reason = f"readback watchdog {WD_READBACK:.0f}s"
                elif phases["recovery_start"] is not None and \
                        now - phases["recovery_start"] > WD_RECOVERY:
                    kill_reason = f"recovery watchdog {WD_RECOVERY:.0f}s"
            if kill_reason:
                break
            time.sleep(0.2)
    finally:
        if proc.poll() is None:
            log(f"supervise: TERM child ({kill_reason})")
            proc.terminate()
            try:
                proc.wait(timeout=TERM_KILL_GRACE)
            except subprocess.TimeoutExpired:
                log("supervise: KILL child")
                proc.kill()
                proc.wait()
    stderr_fh.close()
    result = None
    if os.path.isfile(result_path):
        try:
            result = json.loads(Path(result_path).read_text())
        except json.JSONDecodeError:
            result = None
    if result is not None and kill_reason:
        result["supervised_kill"] = kill_reason
    if kill_reason:
        log(f"supervise: killed: {kill_reason}; stderr at {stderr_path}")
    return result, kill_reason


def execute_attempt(run_dir: Path, journal: Journal, cloud: Cloud,
                    session: Session, row: dict, rep: int, attempt_n: int,
                    stop: dict) -> dict:
    iid = row["id"]
    adir = run_dir / "attempts" / iid / f"repetition-{rep:03d}" \
        / f"attempt-{attempt_n:03d}"
    adir.mkdir(parents=True, exist_ok=True)
    # live ownership check immediately before dispatch: the board's session
    # must still be the one we acquired (the server also refuses stream
    # handshakes for sessions we do not own)
    board = cloud.get_fpga(session.fpga_id)
    live = board.get("session") or {}
    if live.get("session_id") != session.session_id:
        raise Blocker(f"board {session.fpga_id} session is "
                      f"{live.get('session_id')}, not {session.session_id}; "
                      f"refusing to dispatch {iid}")
    # the server may shorten the TTL while keeping the session ID: refresh
    # the deadline from live evidence and gate on it before any MMIO
    live_exp = live.get("expires_at")
    session.expires_at = parse_utc(live_exp) if live_exp else None
    rem = conservative_remaining_s(session.expires_at, cloud,
                                   HTTP_TIMEOUT[1])
    if rem is None or rem < LEASE_MIN_LOAD:
        raise SessionBudget(
            f"{SESSION_BUDGET}: live lease for {session.session_id} "
            f"has {rem} s before load; need {LEASE_MIN_LOAD:.0f} s; "
            "rotate the session")
    cnf_path = run_dir / "inputs" / f"{iid}.cnf"
    rem = conservative_remaining_s(session.expires_at, cloud, HTTP_TIMEOUT[1])
    spec = {
        "command": "solve",
        "candidate_root": str(run_dir / "candidate"),
        "api_url": API_URL,
        "fpga_id": session.fpga_id,
        "session_id": session.session_id,
        "events_path": str(adir / "worker-events.jsonl"),
        "result_path": str(adir / "worker-result.json"),
        "stderr_path": str(adir / "worker-stderr.log"),
        "_job_path": str(adir / "job.json"),
        "solve": {
            "cnf_path": str(cnf_path),
            "expected": row["expected"],
            "timeout_s": SOLVE_TIMEOUT_S,
            "attempt_dir": str(adir),
            "lease": {
                "expires_at_utc": session.expires_at,
                "latency_s": HTTP_TIMEOUT[1],
                "skew_s": cloud.skew_s or 0.0,
                "min_load_s": LEASE_MIN_LOAD,
                "min_go_s": LEASE_MIN_GO,
            },
        },
    }
    log(f"attempt {iid} rep{rep} #{attempt_n}: lease "
        f"{'None' if rem is None else f'{rem:.0f}s'}")
    result, kill = supervise_worker(spec, {},
                                    should_stop=lambda: STOP["flag"])
    rec = {
        "schema": ATTEMPT_SCHEMA, "id": iid, "repetition": rep,
        "attempt": attempt_n, "tier": row["tier"], "family": row["family"],
        "input": {"cnf_path": str(cnf_path.relative_to(run_dir)),
                  "cnf_sha256": sha256_file(cnf_path)},
        "cpu_evidence": {"cpu_run": row.get("cpu_run") or "calibration/cpu-v0",
                         "cpu_attempt": row.get("cpu_attempt"),
                         "certified_kind": row["cpu_certified_kind"],
                         "expected": row["expected"]},
        "session": {"index": session.index, "fpga_id": session.fpga_id,
                    "session_id": session.session_id,
                    "build_job_id": session.build_job_id},
        "identity": {"candidate": candidate_identity(run_dir),
                     "sdk": sdk_identity(), "config": config_identity()},
        "clock": {"sys_clk_hz": SYS_CLK_HZ,
                  "clock_confirmed": clock_confirmed(run_dir,
                                                     session.build_job_id)},
        "ts_utc": utcnow_iso(), "supervised_kill": kill,
        "result": result, "status": None,
    }
    if result is None:
        rec["status"] = ATTEMPT_INTERRUPTED
        rec["reason"] = f"worker killed or died: {kill or 'no result file'}"
    else:
        st = result.get("status")
        rec["status"] = st if st in TERMINAL_STATUS or st == SESSION_BUDGET \
            else ATTEMPT_INTERRUPTED
        if st == "sat" and result.get("model") is not None:
            wpath = adir / "witness.json"
            atomic_write_json(wpath, result["model"])
            rec["witness"] = {"path": wpath.name,
                              "sha256": sha256_file(wpath)}
            rec["independent_validation"] = {
                "checker": "benchmark_cnf.check_witness on the original "
                           "strict formula",
                "passed": result.get("witness_valid"),
                "stack_problems": result.get("stack_problems")}
        if result.get("cycles") is not None:
            confirmed = rec["clock"]["clock_confirmed"]
            rec["device_seconds"] = (result["cycles"] / SYS_CLK_HZ
                                     if confirmed else None)
            if not confirmed:
                rec["device_seconds_note"] = (
                    "withheld: clock not confirmed from cloud build "
                    "evidence; raw cycles retained")
    atomic_write_json(adir / "attempt.json", rec)
    journal.append("attempt_recorded", id=iid, repetition=rep,
                   attempt=attempt_n, status=rec["status"])
    log(f"attempt {iid} rep{rep} #{attempt_n}: status={rec['status']}")
    return rec


def clock_confirmed(run_dir: Path, job_id: str) -> bool:
    """Clock evidence: the retained build job record must echo the 50 MHz
    system clock, else device-seconds are withheld (raw cycles kept)."""
    d = run_dir / "build-logs"
    p = d / f"{job_id}.job.json" if d.is_dir() else None
    if p and p.is_file():
        try:
            job = json.loads(p.read_text())
        except (OSError, json.JSONDecodeError):
            return False
        for key in ("sys_clk_freq", "sys_clk_hz", "clock_hz"):
            if job.get(key) == SYS_CLK_HZ:
                return True
        blob = json.dumps(job)
        if '"sys_clk_freq": 50000000' in blob or \
                '"sys_clk_hz": 50000000' in blob:
            return True
    return False


# ---------------------------------------------------------------------------
# results / report
# ---------------------------------------------------------------------------

def regenerate_results(run_dir: Path) -> None:
    if not (run_dir / "inventory.csv").is_file():
        return
    inv = load_inventory(run_dir)
    plan = build_work_plan(run_dir)
    by_key: dict[tuple, dict] = {}
    for iid, rep in plan:
        attempts, _ = scan_attempts(run_dir, iid, rep)
        term = rep_terminal(run_dir, iid, rep, attempts)
        row = next(r for r in inv if r["id"] == iid)
        rec = {"id": iid, "tier": row["tier"], "family": row["family"],
               "repetition": rep, "expected": row["expected"],
               "status": term["status"] if term else "not_run"}
        if term:
            r = term.get("result") or {}
            ht = r.get("host_times") or {}
            rec.update({"attempt": term["attempt"],
                        "result_code": r.get("result_code"),
                        "cycles": r.get("cycles"),
                        "parse_s": ht.get("parse_s"),
                        "encode_s": ht.get("encode_s"),
                        "load_s": ht.get("load_s"),
                        "host_total_s": ht.get("total_s"),
                        "host_t0_to_obs_s": ht.get("t0_to_obs_s"),
                        "late": r.get("late"),
                        "device_seconds": term.get("device_seconds")})
        else:
            interrupted = [a for a in attempts
                           if a.get("status") == ATTEMPT_INTERRUPTED]
            rec["reason"] = (interrupted[-1].get("reason")
                             if interrupted else "pending/not dispatched")
        by_key[(iid, rep)] = rec
    fields = ["id", "tier", "family", "repetition", "expected", "status",
              "attempt", "result_code", "cycles", "parse_s", "encode_s",
              "load_s", "host_total_s", "host_t0_to_obs_s", "late",
              "device_seconds", "reason"]
    with open(run_dir / "results.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for (iid, rep) in plan:
            w.writerow(by_key[(iid, rep)])


def cmd_report(out: str) -> int:
    run_dir = Path(out).resolve()
    _lock = run_lock(run_dir)      # serialize against a live `run`
    run = load_run(run_dir)
    gate_identity(run_dir, run)          # report revalidates identities
    regenerate_results(run_dir)
    inv = load_inventory(run_dir)
    plan = build_work_plan(run_dir)
    rows = []
    for iid, rep in plan:
        attempts, _ = scan_attempts(run_dir, iid, rep)
        rows.append((iid, rep, rep_terminal(run_dir, iid, rep, attempts)))
    status_counts: dict[str, int] = {}
    for _, _, term in rows:
        key = term["status"] if term else "not_run"
        status_counts[key] = status_counts.get(key, 0) + 1
    binding_errors = []
    for iid, rep, term in rows:
        if not term:
            continue
        p = run_dir / term["input"]["cnf_path"]
        if sha256_file(p) != term["input"]["cnf_sha256"]:
            binding_errors.append(f"{iid} rep{rep}: input hash changed")
        w = term.get("witness")
        if w:
            wp = (run_dir / "attempts" / iid / f"repetition-{rep:03d}"
                  / f"attempt-{term['attempt']:03d}" / w["path"])
            if not wp.is_file() or sha256_file(wp) != w["sha256"]:
                binding_errors.append(f"{iid} rep{rep}: witness hash changed")
    completed = [(iid, rep, t) for iid, rep, t in rows
                 if t and t["status"] in ("sat", "unsat")]

    def med(vals):
        if not vals:
            return None
        vs = sorted(vals)
        n = len(vs)
        if n % 2:
            return round(vs[n // 2], 4)
        return round((vs[n // 2 - 1] + vs[n // 2]) / 2, 4)

    host_times = [t["result"]["host_times"]["total_s"]
                  for _, _, t in completed if t.get("result")]
    cyc = [t["result"]["cycles"] for _, _, t in completed if t.get("result")]
    summary = {
        "schema": "pigweed-fpga-summary/1",
        "generated_utc": utcnow_iso(),
        "identity_ok": not identity_mismatches(run["identity"],
                                               current_identity(run_dir)),
        "binding_errors": binding_errors,
        "rows": len(rows), "status_counts": status_counts,
        "campaign": {"solve_timeout_s": SOLVE_TIMEOUT_S,
                     "repetitions": REPETITIONS,
                     "only_ids": (list(ONLY_IDS) if ONLY_IDS is not None
                                  else None),
                     "cpu_runs": CPU_RUNS,
                     "planned_ids": len({i for i, _r, _t in rows})},
        "eligibility": {
            "rows_210": len(inv),
            "eligible": sum(1 for r in inv if r["eligible"] == "true"),
            "by_reason": reason_counts(inv)},
        "timing": {
            "completed": len(completed),
            "median_host_total_s": med(host_times),
            "median_cycles": med(cyc),
            "censoring": "medians aggregate validated completions only; "
                         "timeouts/resource errors censored explicitly",
            "device_seconds_withheld_unless_clock_confirmed": True},
        "no_official_score": True,
    }
    atomic_write_json(run_dir / "summary.json", summary)
    write_report_md(run_dir, summary)
    log(f"report: {json.dumps(status_counts, sort_keys=True)}; "
        f"binding_errors={len(binding_errors)}")
    return 0 if not binding_errors else 2


def reason_counts(inv) -> dict:
    out: dict[str, int] = {}
    for r in inv:
        if r["eligible"] == "true":
            out["eligible"] = out.get("eligible", 0) + 1
            continue
        for reason in (r["reasons"].split(";") if r["reasons"] else []):
            out[reason] = out.get(reason, 0) + 1
    return out


def write_report_md(run_dir: Path, summary: dict) -> None:
    c = summary["status_counts"]
    cp = summary["campaign"]
    reps = cp["repetitions"]
    scope = ("all eligible ids" if cp["only_ids"] is None else
             f"{len(cp['only_ids'])} requested ids, of which "
             f"{cp['planned_ids']} are eligible and planned")
    cpu = ", ".join(f"`{r['path']}` (solve {r['env'][0]} s / check "
                    f"{r['env'][1]} s)" for r in cp["cpu_runs"])
    lines = [
        "# PigWeedSAT native FPGA benchmark run", "",
        f"Generated {summary['generated_utc']}.", "",
        "## Contract", "",
        "- Unmodified PigWeedSAT candidate (frozen snapshot in `candidate/`), "
        f"cfg `H.pigweed(H.Config(lit_page={LIT_PAGE}))`, P={LIT_PAGE}.",
        f"- 50 MHz system clock and timing target; "
        f"{cp['solve_timeout_s']:,.0f} s **host** solve deadline; "
        f"{reps} repetition{'s' if reps != 1 else ''}; host-observed timing "
        "only — no device-enforced solve deadline is claimed.",
        f"- Scope: {scope}.",
        "- All 210 manifest rows retained; eligibility from fresh encoder "
        "acceptance + semantics/field safety + validated CPU certification "
        f"(CPU evidence: {cpu}; later runs override earlier ones per id).",
        "", "## Outcomes", "",
        f"- status counts over planned repetitions: "
        f"`{json.dumps(c, sort_keys=True)}`",
        f"- eligible ids (whole inventory): "
        f"{summary['eligibility']['eligible']} of 210 "
        f"(`{json.dumps(summary['eligibility']['by_reason'], sort_keys=True)}`)",
        f"- median host total (validated completions): "
        f"{summary['timing']['median_host_total_s']} s",
        f"- median device cycles (validated completions): "
        f"{summary['timing']['median_cycles']}",
        "- CPU-vs-FPGA speedups and competition scores are intentionally "
        "absent: different timing contracts.",
        "",
    ]
    if summary["binding_errors"]:
        lines += ["## Binding errors", ""] + \
            [f"- {e}" for e in summary["binding_errors"]] + [""]
    atomic_write_text(run_dir / "report.md", "\n".join(lines))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_campaign(args) -> dict:
    """Campaign scope from prepare's CLI.  CPU overrides carry the solve and
    check limits recorded in their own run identity, so their evidence is
    validated against the limits it was produced under."""
    cpu_runs = [{"path": os.path.relpath(DEFAULT_CPU_RUN, HERE),
                 "env": [1000, 3000]}]
    for d in args.cpu_override:
        p = Path(d).resolve()
        try:
            ident = json.loads((p / "run.json").read_text())["identity"]
        except (OSError, ValueError, KeyError) as e:
            raise Blocker(f"--cpu-override {d}: unreadable run identity: {e}")
        cpu_runs.append({"path": os.path.relpath(p, HERE),
                         "env": [ident["solve_timeout_seconds"],
                                 ident["check_timeout_seconds"]]})
    only = None
    if args.only_ids_manifest:
        with open(args.only_ids_manifest, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if "id" not in (reader.fieldnames or []):
                raise Blocker(f"{args.only_ids_manifest}: no `id` column")
            only = sorted({r["id"] for r in reader})
        if not only:
            raise Blocker(f"{args.only_ids_manifest}: no ids")
    return {"solve_timeout_s": (args.solve_timeout
                                if args.solve_timeout is not None else 1000.0),
            "repetitions": (args.repetitions
                            if args.repetitions is not None else 3),
            "only_ids": only, "cpu_runs": cpu_runs}


def main() -> int:
    argv = sys.argv[1:]
    cmd = argv[0] if argv and not argv[0].startswith("-") else None
    if cmd:
        argv = argv[1:]
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out")
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--screen-worker", nargs=3,
                    metavar=("CNF", "CANDIDATE_ROOT", "RESULT"))
    ap.add_argument("--rtl-worker", metavar="SPEC")
    # prepare-only campaign scope (recorded in run.json and the identity)
    ap.add_argument("--solve-timeout", type=float, default=None,
                    help="host solve deadline in seconds (default 1000)")
    ap.add_argument("--repetitions", type=int, default=None,
                    help="repetitions per eligible id (default 3)")
    ap.add_argument("--only-ids-manifest", default=None,
                    help="CSV with an `id` column; restricts the work plan")
    ap.add_argument("--cpu-override", action="append", default=[],
                    help="extra CPU run dir (repeatable); its records "
                         "override cpu-v0 per id, validated against its "
                         "own solve/check limits")
    args = ap.parse_args(argv)
    if args.screen_worker:
        return screen_worker_main(*args.screen_worker)
    if args.rtl_worker:
        return rtl_worker_main(args.rtl_worker)
    if not args.out:
        ap.error("--out RUN is required")
    scoped = (args.solve_timeout is not None or args.repetitions is not None
              or args.only_ids_manifest or args.cpu_override)
    if scoped and cmd != "prepare":
        ap.error("campaign options apply to prepare only (recorded in run.json)")
    try:
        if cmd == "prepare":
            return cmd_prepare(args.out, build_campaign(args) if scoped
                               else None)
        if cmd == "qualify":
            return cmd_qualify(args.out)
        if cmd == "run":
            return cmd_run(args.out, args.resume)
        if cmd == "report":
            return cmd_report(args.out)
        ap.error("command required: prepare|qualify|run|report")
    except Blocker as e:
        log(f"BLOCKED: {e}")
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
