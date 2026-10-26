"""Behavioral boundary + integration tests for the PigWeedSAT FPGA campaign.

Layers:
* pure decision logic: eligibility derivation, timing classification, lease
  boundaries, identity/resume gates, attempt plans;
* in-process native-bus execution (``run_one``) against a register-accurate
  fake accelerator: classification of -4 codes, 64-bit cycle combination,
  over-budget cycles, late/straddling observations, lease gates, stack
  validation, terminal transport;
* a local fake control plane — ONE HTTP server that also speaks the
  wishbone WebSocket relay protocol on /fpga/{id}/stream after a real
  RFC 6455 handshake (the SDK derives the stream URL from api_url, so
  HTTP and WS must share the port) — driving the real worker subprocess:
  send-exactly-once transport (no replay), hung children killed/reaped by
  parent watchdogs, crash/restart journal reconciliation, guarded release
  of owned sessions only, and a full cmd_run campaign;
* capped RTL simulation reporting that never claims exact DUT clocks.

Runs with real credentials isolated (XDG_CONFIG_HOME tmpdir, MRG_API_KEY
test value) — no test touches the production cloud.
"""

from __future__ import annotations

import base64
import contextlib
import csv
import hashlib
import json
import os
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
PIGWEED = os.path.join(ROOT, "PigWeedSAT")
sys.path.insert(0, HERE)
sys.path.insert(0, PIGWEED)

import run_pigweed_fpga as R                    # noqa: E402
import pigweed_fpga_worker as W                 # noqa: E402
import benchmark_cnf as BC                      # noqa: E402
import screen_pigweed as SP                     # noqa: E402

import design as D                              # noqa: E402
import satlat.host as HOST                      # noqa: E402
import satlat.driver as DRV                     # noqa: E402

SN = list(D.STAT_NAMES)
I_CYC_LO, I_CYC_HI = SN.index("cycles_lo"), SN.index("cycles_hi")
I_HEIGHT = SN.index("height")
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

TINY_SAT = "p cnf 1 1\n1 0\n"
TINY_UNSAT = "p cnf 1 2\n1 0\n-1 0\n"
TINY_EMPTY_CLAUSE = "p cnf 2 3\n1 -2 0\n0\n2 0\n"


def write_cnf(path, text):
    Path(path).write_text(text, encoding="utf-8")
    return str(path)


def dimacs_of(clauses, nv):
    return "p cnf %d %d\n%s" % (
        nv, len(clauses),
        "".join(" ".join(map(str, c)) + " 0\n" for c in clauses))


# ---------------------------------------------------------------------------
# Fake accelerator register model
# ---------------------------------------------------------------------------

_USERNAME_PATCH = None


def setUpModule():
    """cmd_run resolves the account from stored credentials; tests use the
    fake control plane's owner name."""
    global _USERNAME_PATCH
    _USERNAME_PATCH = mock.patch.object(R, "resolve_username",
                                        lambda: "tester")
    _USERNAME_PATCH.start()


def tearDownModule():
    _USERNAME_PATCH.stop()


class FakeAccel:
    """Register-accurate stub of the SATAccel Wishbone window.

    ``done_after_reads`` gates the scored (first) GO; later GOs (synthetic
    recovery / re-qualification on the same bus) complete after two polls so
    bounded recovery succeeds.  ``results`` scripts R_RESULT per GO."""

    def __init__(self, *, result=1, stack=(1,), done_after_reads=2,
                 cycles=(0, 0), busy_until_done=True, height=None,
                 results=None, stacks=None, done_afters=None):
        self.result = result
        self.results = list(results) if results is not None else None
        self.stacks = list(stacks) if stacks is not None else None
        self._cur_result = result
        self.stack = list(stack)
        self._cur_stack = list(stack)
        self.done_after_reads = done_after_reads
        self.done_afters = list(done_afters) if done_afters is not None \
            else None
        self.cycles = cycles
        self.busy_until_done = busy_until_done
        self.height = len(self.stack) if height is None else height
        self.regs = {}
        caps = [D.N_MAX, D.C_MAX, D.LE_MAX, D.CE_MAX, D.MAX_LEARN, D.FRAC_W,
                D.MAGIC, D.VERSION]
        for i, v in enumerate(caps):
            self.regs[D.R_CAPS + i] = v
        self.busy = False
        self.done = False
        self._ctrl_reads = 0
        self._gos = 0
        self._cur_done_after = done_after_reads
        self.frames = []            # (op, word, words) request log
        self.writes = {}            # word -> [values]

    def read(self, word):
        self.frames.append(("read", word, None))
        if word == D.R_CTRL:
            self._ctrl_reads += 1
            if self.busy and self._ctrl_reads >= self._cur_done_after:
                self.busy = False
                self.done = True
            return (1 if self.busy else 0) | (2 if self.done else 0)
        if D.R_CAPS <= word < D.R_CAPS + 8:
            return self.regs[word]
        if D.R_STATS <= word < D.R_STATS + len(SN):
            i = word - D.R_STATS
            if i == I_CYC_LO:
                return self.cycles[0] & 0xFFFFFFFF
            if i == I_CYC_HI:
                return self.cycles[1] & 0xFFFFFFFF
            if i == I_HEIGHT:
                return len(self._cur_stack)
            return 0
        if word == D.R_RESULT:
            return self._cur_result & 0xFFFFFFFF
        return 0

    def write(self, word, value):
        self.frames.append(("write", word, [value]))
        self.writes.setdefault(word, []).append(value)
        if word == D.R_CTRL and value == D.CTRL_GO:
            self._gos += 1
            self.busy = self.busy_until_done
            self.done = False
            self._ctrl_reads = 0
            self._cur_done_after = (self.done_afters.pop(0)
                                    if self.done_afters else
                                    (self.done_after_reads
                                     if self._gos == 1 else 2))
            self._cur_result = (self.results.pop(0)
                                if self.results else self.result)
            self._cur_stack = (self.stacks.pop(0)
                               if self.stacks else list(self.stack))
        elif word == D.R_CTRL and value == D.CTRL_ABORT:
            self.busy = False
            self.done = False
            self._ctrl_reads = 0

    def write_fifo(self, word, values):
        self.frames.append(("write_fifo", word, list(values)))
        self.writes.setdefault(word, []).extend(values)

    def read_fifo(self, count):
        self.frames.append(("read_fifo", D.R_MEMWIN, None))
        return [HOST.s32(x) for x in self._cur_stack[:count]]

    def ctrl_writes(self, value):
        return self.writes.get(D.R_CTRL, []).count(value)


class FakeMono:
    def __init__(self, scripted=(), step=0.05, start=100.0):
        self.scripted = list(scripted)
        self.step = step
        self.t = start

    def __call__(self):
        if self.scripted:
            self.t = self.scripted.pop(0)
        else:
            self.t += self.step
        return self.t


def make_bus(accel):
    return W.GuardedBus(accel, alarm_enabled=False)


def tiny_img(clauses=None, nv=1):
    return HOST.build_images(nv, clauses if clauses is not None else [[1]],
                             HOST.pigweed(HOST.Config(lit_page=W.LIT_PAGE)))


# ---------------------------------------------------------------------------
# Pure decision logic
# ---------------------------------------------------------------------------

class EligibilityDerivation(unittest.TestCase):
    def test_certified_kind_decides_expected_not_label(self):
        self.assertEqual(R.derive_eligibility("ok", [], "loaded", True,
                                              "sat_witness"), ("sat", []))
        self.assertEqual(R.derive_eligibility("ok", [], "loaded", True,
                                              "unsat_proof"), ("unsat", []))
        self.assertEqual(R.derive_eligibility("ok", [], "loaded", True,
                                              "unsat_trivial_original"),
                         ("unsat", []))

    def test_uncertified_never_uses_result_label(self):
        expected, reasons = R.derive_eligibility("ok", [], "loaded", False,
                                                 None)
        self.assertIsNone(expected)
        self.assertIn("uncertified", reasons)

    def test_empty_clause_loss_and_unsafe_lengths_nondispatchable(self):
        _, reasons = R.derive_eligibility("input_semantics_error", [],
                                          "loaded", True, "sat_witness")
        self.assertIn("input_semantics_error", reasons)
        _, reasons = R.derive_eligibility("ok", ["clause_len>=2048"],
                                          "loaded", True, "sat_witness")
        self.assertIn("field_limit", reasons)
        _, reasons = R.derive_eligibility("ok", [], "unsupported", True,
                                          "sat_witness")
        self.assertIn("non_fitting", reasons)

    def test_real_candidate_drops_empty_clause(self):
        with tempfile.TemporaryDirectory() as td:
            p = write_cnf(os.path.join(td, "e.cnf"), TINY_EMPTY_CLAUSE)
            res = R.run_screen_subprocess(p, PIGWEED)
            self.assertNotIn("worker_crash", res)
            strict = BC.parse_dimacs(TINY_EMPTY_CLAUSE)
            sem = SP.compare_semantics(
                strict.num_vars, strict.clauses,
                res["candidate_parse"]["num_vars"],
                res["candidate_parse"]["clauses"])
            self.assertEqual(sem["status"], "input_semantics_error")


class CampaignScope(unittest.TestCase):
    def tearDown(self):
        R.apply_campaign(None)          # module globals back to defaults

    def test_default_campaign_keeps_original_contract(self):
        R.apply_campaign(None)
        self.assertEqual((R.SOLVE_TIMEOUT_S, R.REPETITIONS, R.ONLY_IDS),
                         (1000.0, 3, None))
        self.assertEqual((R.LEASE_MIN_LOAD, R.LEASE_MIN_GO, R.WD_GO),
                         (1800.0, 1680.0, 1060.0))

    def test_deadline_drives_lease_watchdog_and_session_fit(self):
        R.apply_campaign({"solve_timeout_s": 2600})
        self.assertEqual((R.LEASE_MIN_LOAD, R.LEASE_MIN_GO, R.WD_GO),
                         (3400.0, 3280.0, 2660.0))
        cfg = R.config_identity()
        self.assertEqual((cfg["solve_timeout_s"], cfg["lease_min_load_s"]),
                         (2600.0, 3400.0))
        top = R.SESSION_TTL_S - R.ATTACH_QUALIFY_MARGIN_S \
            - R.LEASE_LOAD_OVERHEAD_S
        R.apply_campaign({"solve_timeout_s": top})       # exactly fits
        with self.assertRaises(R.Blocker):
            R.apply_campaign({"solve_timeout_s": top + 1})

    def test_only_ids_restricts_the_work_plan(self):
        with tempfile.TemporaryDirectory() as td:
            rd = Path(td)
            R.write_inventory(rd / "inventory.csv", [
                {"id": "MRB0-005", "eligible": "true"},   # a fixed pilot
                {"id": "X2", "eligible": "true"},
                {"id": "X3", "eligible": "false"},
                {"id": "X4", "eligible": "true"}])
            R.apply_campaign({"only_ids": ["X2", "X3"], "repetitions": 1})
            self.assertEqual(R.build_work_plan(rd), [("X2", 1)])
            R.apply_campaign(None)
            self.assertEqual(len(R.build_work_plan(rd)), 9)   # 3 ids x 3

    def test_cpu_override_wins_per_id_with_its_own_limits(self):
        v0 = (Path("/cpu-v0"), (1000, 3000), "calibration/cpu-v0")
        v1 = (Path("/cpu-v1"), (5000, 20000), "calibration/cpu-v1")
        held = {("/cpu-v1", "X1"): {"attempt": 1},
                ("/cpu-v0", "X1"): {"attempt": 1},
                ("/cpu-v0", "X2"): {"attempt": 2}}
        with mock.patch.object(
                R.CC, "latest_terminal_attempt",
                lambda d, i: (held.get((str(d), i)), 1)):
            rec, src = R.pick_cpu_evidence("X1", [v0, v1])
            self.assertEqual((src[2], src[1]), ("calibration/cpu-v1",
                                                (5000, 20000)))
            rec, src = R.pick_cpu_evidence("X2", [v0, v1])
            self.assertEqual((src[2], rec["attempt"]),
                             ("calibration/cpu-v0", 2))
            rec, _src = R.pick_cpu_evidence("X9", [v0, v1])
            self.assertIsNone(rec)

    def test_load_run_applies_the_recorded_campaign(self):
        with tempfile.TemporaryDirectory() as td:
            rd = Path(td)
            (rd / "run.json").write_text(json.dumps(
                {"campaign": {"solve_timeout_s": 2600, "repetitions": 1,
                              "only_ids": ["X2"]}}))
            R.load_run(rd)
            self.assertEqual((R.SOLVE_TIMEOUT_S, R.REPETITIONS, R.ONLY_IDS,
                              R.LEASE_MIN_LOAD), (2600.0, 1, ("X2",), 3400.0))
            (rd / "run.json").write_text(json.dumps({}))   # original run
            R.load_run(rd)
            self.assertEqual((R.SOLVE_TIMEOUT_S, R.ONLY_IDS), (1000.0, None))

    def test_report_states_the_campaign_contract(self):
        summary = {
            "generated_utc": "t", "status_counts": {"sat": 1},
            "campaign": {"solve_timeout_s": 2600.0, "repetitions": 1,
                         "only_ids": ["X1", "X2"], "planned_ids": 1,
                         "cpu_runs": [{"path": "calibration/cpu-v1-5000s",
                                       "env": [5000, 20000]}]},
            "eligibility": {"eligible": 69, "by_reason": {}},
            "timing": {"median_host_total_s": 1.0, "median_cycles": 5},
            "binding_errors": []}
        with tempfile.TemporaryDirectory() as td:
            R.write_report_md(Path(td), summary)
            text = (Path(td) / "report.md").read_text()
        self.assertIn("2,600 s **host** solve deadline; 1 repetition;", text)
        self.assertIn("2 requested ids, of which 1 are eligible", text)
        self.assertIn("calibration/cpu-v1-5000s", text)
        self.assertNotIn("1,000 s", text)

    def test_campaign_flags_rejected_outside_prepare(self):
        with mock.patch.object(sys, "argv", ["x", "run", "--out", "/tmp/x",
                                             "--solve-timeout", "2600"]):
            with self.assertRaises(SystemExit):
                R.main()


class TimingClassification(unittest.TestCase):
    def test_observation_boundaries(self):
        self.assertEqual(W.classify_observation(99.99, 0.0, 100.0),
                         "done_within")
        self.assertEqual(W.classify_observation(100.0, 0.0, 100.0),
                         "done_late")
        self.assertEqual(W.classify_observation(100.01, 0.0, 100.0),
                         "done_late")

    def test_lease_boundaries(self):
        self.assertFalse(W.lease_ok(1799.99, 1800.0))
        self.assertTrue(W.lease_ok(1800.0, 1800.0))
        self.assertTrue(W.lease_ok(1680.0, 1680.0))
        self.assertFalse(W.lease_ok(1679.99, 1680.0))
        self.assertFalse(W.lease_ok(None, 1.0))

    def test_remaining_subtracts_latency_and_skew(self):
        now = 1000.0
        self.assertAlmostEqual(
            W.lease_remaining_s(1800.0, now, 30.0), 770.0)
        self.assertIsNone(W.lease_remaining_s(None, now))

    def test_conservative_remaining(self):
        class C:
            skew_s = 2.0
        self.assertAlmostEqual(
            R.conservative_remaining_s(time.time() + 832.0, C(), 30.0),
            800.0, delta=0.5)
        self.assertIsNone(R.conservative_remaining_s(None, C(), 30.0))


class IdentityAndResume(unittest.TestCase):
    def test_identity_mismatches_all_sections(self):
        cur = {"organizer": {"a.py": "h1"}, "candidate": {"design.py": "h2"},
               "sdk": {"commit": "c"}, "config": {"x": 1}}
        self.assertEqual(R.identity_mismatches(cur, dict(cur)), [])
        changed = json.loads(json.dumps(cur))
        changed["candidate"]["design.py"] = "tampered"
        changed["config"]["x"] = 2
        errs = R.identity_mismatches(cur, changed)
        self.assertTrue(any(e.startswith("candidate.") for e in errs))
        self.assertTrue(any(e.startswith("config.") for e in errs))

    def _run_dir(self, tdir):
        run_dir = Path(tdir)
        (run_dir / "inventory.csv").write_text(
            "id,tier,family,category,result_gbd,expected,eligible,reasons,"
            "semantics_status,build_status,field_flags,cpu_certified,"
            "cpu_certified_kind,cpu_attempt,pilot\n"
            "TEST-1,S,f,main,sat,sat,true,,ok,loaded,,true,sat_witness,1,"
            "false\n"
            "TEST-2,S,g,main,sat,sat,true,,ok,loaded,,true,sat_witness,1,"
            "false\n", encoding="utf-8")
        return run_dir

    def test_plan_and_terminal_skipping(self):
        with tempfile.TemporaryDirectory() as td:
            run_dir = self._run_dir(td)
            plan = R.build_work_plan(run_dir)
            self.assertEqual(plan, [("TEST-1", 1), ("TEST-2", 1),
                                    ("TEST-1", 2), ("TEST-2", 2),
                                    ("TEST-1", 3), ("TEST-2", 3)])
            a1 = run_dir / "attempts" / "TEST-1" / "repetition-001" \
                / "attempt-001"
            a1.mkdir(parents=True)
            (a1 / "attempt.json").write_text(json.dumps({
                "id": "TEST-1", "attempt": 1, "status": "interrupted",
                "reason": "worker killed"}), encoding="utf-8")
            a2 = run_dir / "attempts" / "TEST-1" / "repetition-001" \
                / "attempt-002"
            a2.mkdir(parents=True)
            input_rel = "inputs/TEST-1.cnf"
            ip = run_dir / input_rel
            ip.parent.mkdir(exist_ok=True)
            ip.write_text(TINY_SAT, encoding="utf-8")
            (a2 / "attempt.json").write_text(json.dumps({
                "id": "TEST-1", "attempt": 2, "status": "sat",
                "input": {"cnf_path": input_rel,
                          "cnf_sha256": R.sha256_file(ip)}}),
                encoding="utf-8")
            attempts, max_n = R.scan_attempts(run_dir, "TEST-1", 1)
            self.assertEqual(max_n, 2)
            self.assertIsNotNone(R.rep_terminal(run_dir, "TEST-1", 1,
                                                attempts))
            ip.write_text(TINY_UNSAT, encoding="utf-8")   # tampered binding
            self.assertIsNone(R.rep_terminal(run_dir, "TEST-1", 1, attempts))
            ip.write_text(TINY_SAT, encoding="utf-8")
            _, max_n = R.scan_attempts(run_dir, "TEST-1", 1)
            self.assertEqual(max_n + 1, 3)   # interrupted history preserved

    def test_run_lock_exclusive(self):
        with tempfile.TemporaryDirectory() as td:
            held = R.run_lock(Path(td))   # keep the handle: closing releases
            with self.assertRaises(R.Blocker):
                R.run_lock(Path(td))

    def test_gate_phase(self):
        with self.assertRaises(R.Blocker):
            R.gate_phase({"phase": "prepared"}, "qualified")
        # a failed qualification is BELOW prepared: nothing runs until the
        # failure is fixed and qualify re-executed
        with self.assertRaises(R.Blocker):
            R.gate_phase({"phase": "qualified_with_failures"}, "qualified")
        with self.assertRaises(R.Blocker):
            R.gate_phase({"phase": "qualified_with_failures"}, "prepared")
        R.gate_phase({"phase": "qualified"}, "qualified")
        R.gate_phase({"phase": "running"}, "qualified")
        # a finished run may be resumed idempotently; never rewound
        R.gate_phase({"phase": "run_done"}, "running")
        R.gate_phase({"phase": "run_blocked"}, "running")


# ---------------------------------------------------------------------------
# run_one over the in-process fake bus
# ---------------------------------------------------------------------------

class RunOneClassification(unittest.TestCase):
    @staticmethod
    def run_one(accel, expected="sat", timeout_s=2.0, mono=None, lease=None,
                clauses=None, nv=1, original=None, recovery_budget_s=None):
        bus = make_bus(accel)
        img = tiny_img(clauses, nv)
        orig = original if original is not None else \
            BC.parse_dimacs(TINY_SAT if clauses is None else
                            dimacs_of(clauses, nv))
        return W.run_one(bus, HOST, DRV, img, orig, expected,
                         timeout_s=timeout_s, mono=mono or FakeMono(),
                         sleep=lambda s: None, lease=lease,
                         recovery_budget_s=recovery_budget_s)

    def test_sat_within_deadline(self):
        accel = FakeAccel(result=1, stack=[1], done_after_reads=2)
        res = self.run_one(accel)
        self.assertEqual(res["status"], "sat")
        self.assertTrue(res["witness_valid"])
        self.assertFalse(res["late"])
        self.assertEqual(accel.ctrl_writes(D.CTRL_GO), 1)

    def test_unsat_expected_agrees_and_contradiction_is_wrong_answer(self):
        accel = FakeAccel(result=0, done_after_reads=2, results=[0, 1])
        res = self.run_one(accel, expected="unsat",
                           original=BC.parse_dimacs(TINY_UNSAT))
        self.assertEqual(res["status"], "unsat")
        accel = FakeAccel(result=0, done_after_reads=2, results=[0, 1])
        res = self.run_one(accel, expected="sat",
                           original=BC.parse_dimacs(TINY_UNSAT))
        self.assertEqual(res["status"], "wrong_answer")

    def test_negative_code_is_resource_error_never_unsat(self):
        accel = FakeAccel(result=-4, done_after_reads=2, results=[-4, 1])
        res = self.run_one(accel, expected="unsat",
                           original=BC.parse_dimacs(TINY_UNSAT))
        self.assertEqual(res["status"], "resource_error")
        self.assertEqual(res["result_code"], -4)
        self.assertTrue(res["recovery"]["ok"])   # board left healthy

    def test_cycles_combine_64bit(self):
        accel = FakeAccel(result=1, stack=[1], done_after_reads=2,
                          cycles=(0, 2))          # hi=2 -> 2^33
        res = self.run_one(accel)
        self.assertEqual(res["cycles"], 2 << 32)

    def test_cycles_over_budget_not_success(self):
        accel = FakeAccel(result=1, stack=[1], done_after_reads=2,
                          cycles=(0, 12))         # 2^36 > 50e9
        res = self.run_one(accel)
        self.assertEqual(res["status"], "resource_error")
        self.assertEqual(res["over_budget_cycles"], 12 << 32)

    def test_cycle_budget_scales_with_deadline(self):
        # 2^36 cycles (~1374 s at 50 MHz): outside a 1000 s budget, inside
        # a 2600 s one
        for timeout_s, want in ((1000.0, "resource_error"), (2600.0, "sat")):
            accel = FakeAccel(result=1, stack=[1], done_after_reads=2,
                              cycles=(0, 16))
            res = self.run_one(accel, timeout_s=timeout_s)
            self.assertEqual(res["status"], want, timeout_s)

    def test_deadline_busy_aborts_to_idle(self):
        accel = FakeAccel(result=1, stack=[1], done_after_reads=10 ** 9,
                          results=[1, 1])
        res = self.run_one(accel, timeout_s=0.3, recovery_budget_s=5.0)
        self.assertEqual(res["status"], "timeout")
        self.assertGreaterEqual(accel.ctrl_writes(D.CTRL_ABORT), 1)
        self.assertEqual(res["post_abort_ctrl"], 0)

    def test_late_done_observation_is_timeout_evidence(self):
        # Exactly scripted, dyadic-representable monotonic sequence.  Calls:
        # total0, load0, load1, t0(=100.0), go-ack, then per poll iteration
        # deadline-check and t_obs.  Poll 2's done reply is observed at
        # exactly t0 + timeout (101.0): late evidence although that read
        # began before the cutoff.
        script = [99.25, 99.50, 99.75, 100.0, 100.25,   # prelude; t0=100.0
                  100.5, 100.75,                         # chk1, t_obs1
                  100.875, 101.0]                        # chk2, t_obs2 (late)
        mono = FakeMono(scripted=script)
        accel = FakeAccel(result=1, stack=[1], done_after_reads=2,
                          results=[1, 1])
        res = self.run_one(accel, timeout_s=1.0, mono=mono,
                           recovery_budget_s=5.0)
        self.assertTrue(res["late"])
        self.assertEqual(res["status"], "timeout")
        self.assertEqual(res["result_code"], 1)   # retained as evidence

    def test_straddled_reply_after_cutoff_is_timeout_not_success(self):
        mono = FakeMono(step=0.4)
        accel = FakeAccel(result=1, stack=[1], done_after_reads=3,
                          results=[1, 1])
        res = self.run_one(accel, timeout_s=1.0, mono=mono,
                           recovery_budget_s=5.0)
        self.assertEqual(res["status"], "timeout")

    def test_stack_out_of_range_and_conflicts_are_wrong_answer(self):
        accel = FakeAccel(result=1, stack=[1, -1], done_after_reads=2,
                          results=[1, 1])
        res = self.run_one(accel)
        self.assertEqual(res["status"], "wrong_answer")
        accel = FakeAccel(result=1, stack=[5], done_after_reads=2,
                          results=[1, 1])
        res = self.run_one(accel)
        self.assertEqual(res["status"], "wrong_answer")
        self.assertIn("outside 1..1", res["stack_problems"][0])

    def test_transport_failure_is_terminal_no_further_calls(self):
        accel = FakeAccel(result=1, stack=[1], done_after_reads=2)

        class DieBus:
            def __init__(self, inner):
                self.inner = inner
                self.calls = 0

            def read(self, word):
                self.calls += 1
                if self.calls > 3:
                    raise OSError("connection reset")
                return self.inner.read(word)

            def write(self, w, v):
                self.inner.write(w, v)

            def write_fifo(self, w, v):
                self.inner.write_fifo(w, v)

            def read_fifo(self, n):
                return self.inner.read_fifo(n)

        bus = W.GuardedBus(DieBus(accel), alarm_enabled=False)
        img = tiny_img()
        res = W.run_one(bus, HOST, DRV, img, BC.parse_dimacs(TINY_SAT),
                        "sat", timeout_s=2.0, mono=FakeMono(),
                        sleep=lambda s: None)
        self.assertEqual(res["status"], "transport_error")
        self.assertIsNotNone(bus.terminal)
        with self.assertRaises(W.TerminalTransport):
            bus.read(D.R_CTRL)     # no next transaction, ever

    def test_recovery_budget_exhaustion_is_terminal(self):
        class SecondGoBusy(FakeAccel):
            """Scored solve completes; the recovery solve stays busy."""

            def __init__(self):
                super().__init__(result=-4, stack=[1], done_after_reads=2,
                                 results=[-4, 1])
                self.gos = 0

            def write(self, word, value):
                super().write(word, value)
                if word == D.R_CTRL and value == D.CTRL_GO:
                    self.gos += 1

            def read(self, word):
                if word == D.R_CTRL and self.gos >= 2:
                    return 1          # busy forever during recovery
                return super().read(word)

        res = self.run_one(SecondGoBusy(), expected="unsat",
                           original=BC.parse_dimacs(TINY_UNSAT),
                           recovery_budget_s=0.3)
        # the scored resource_error stays truthful; the failed recovery
        # flags a campaign halt instead of relabeling the outcome
        self.assertEqual(res["status"], "resource_error")
        self.assertTrue(res["recovery_failed"])
        self.assertFalse(res["recovery"]["ok"])


class LeaseGates(unittest.TestCase):
    def base_spec(self, **kw):
        return {"expires_at_utc": time.time() + kw.get("remaining", 100.0),
                "latency_s": 0.0, "skew_s": 0.0,
                "min_load_s": kw.get("min_load", 1800.0),
                "min_go_s": kw.get("min_go", 1680.0)}

    def test_insufficient_before_load_skips_load(self):
        accel = FakeAccel(result=1, stack=[1], done_after_reads=2)
        lease = self.base_spec(remaining=1799.9)
        res = W.run_one(make_bus(accel), HOST, DRV, tiny_img(),
                        BC.parse_dimacs(TINY_SAT), "sat", timeout_s=2.0,
                        mono=FakeMono(), sleep=lambda s: None, lease=lease)
        self.assertEqual(res["status"], "session_budget_insufficient")
        self.assertEqual(res["stage"], "before_load")
        self.assertNotIn(D.R_MEMDATA, accel.writes)   # no memory load
        self.assertEqual(accel.ctrl_writes(D.CTRL_GO), 0)

    def test_boundary_exactly_at_minimum_proceeds(self):
        accel = FakeAccel(result=1, stack=[1], done_after_reads=2,
                          results=[1, 1])
        lease = self.base_spec(remaining=1800.5)
        res = W.run_one(make_bus(accel), HOST, DRV, tiny_img(),
                        BC.parse_dimacs(TINY_SAT), "sat", timeout_s=2.0,
                        mono=FakeMono(), sleep=lambda s: None, lease=lease)
        self.assertEqual(res["status"], "sat")

    def test_insufficient_before_go_aborts_without_go(self):
        accel = FakeAccel(result=1, stack=[1], done_after_reads=2)

        class ShrinkLease(dict):
            """Passes the load gate, then collapses before GO."""

            def __init__(self, base):
                super().__init__(base)
                self.checks = 0

            def __getitem__(self, k):
                if k == "expires_at_utc":
                    self.checks += 1
                    return time.time() + (5000.0 if self.checks == 1
                                          else 1.0)
                return super().__getitem__(k)

        lease = ShrinkLease(self.base_spec(remaining=5000.0))
        res = W.run_one(make_bus(accel), HOST, DRV, tiny_img(),
                        BC.parse_dimacs(TINY_SAT), "sat", timeout_s=2.0,
                        mono=FakeMono(), sleep=lambda s: None, lease=lease)
        self.assertEqual(res["status"], "session_budget_insufficient")
        self.assertEqual(res["stage"], "before_go")
        self.assertEqual(accel.ctrl_writes(D.CTRL_GO), 0)
        # one abort-to-idle at start plus the pre-GO abort
        self.assertEqual(accel.ctrl_writes(D.CTRL_ABORT), 2)


# ---------------------------------------------------------------------------
# Fake control plane: one HTTP server, WS hijack on /fpga/{id}/stream
# ---------------------------------------------------------------------------

def _read_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("eof")
        buf += chunk
    return buf


def read_ws_frame(sock):
    try:
        h = _read_exact(sock, 2)
    except (ConnectionError, OSError):
        return None, None
    opcode = h[0] & 0x0F
    masked = h[1] & 0x80
    ln = h[1] & 0x7F
    if ln == 126:
        ln = struct.unpack(">H", _read_exact(sock, 2))[0]
    elif ln == 127:
        ln = struct.unpack(">Q", _read_exact(sock, 8))[0]
    mkey = _read_exact(sock, 4) if masked else None
    payload = _read_exact(sock, ln) if ln else b""
    if mkey:
        payload = bytes(b ^ mkey[i % 4] for i, b in enumerate(payload))
    return opcode, payload


def send_ws_frame(sock, payload, opcode=2):
    h = bytes([0x80 | opcode])
    n = len(payload)
    if n < 126:
        h += bytes([n])
    elif n < 65536:
        h += bytes([126]) + struct.pack(">H", n)
    else:
        h += bytes([127]) + struct.pack(">Q", n)
    sock.sendall(h + payload)


def decode_request(raw):
    op = raw[0] & 0x7F
    length = struct.unpack(">I", b"\x00" + raw[1:4])[0]
    address = struct.unpack(">I", raw[4:8])[0]
    data = list(struct.unpack(f">{length}I", raw[8:8 + 4 * length])) \
        if op == 1 else []
    count = struct.unpack(">I", raw[8:12])[0] if op == 2 else 0
    return op, address, data, count


def _iso(epoch):
    import datetime as dt
    return dt.datetime.fromtimestamp(
        epoch, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")


class FakeCloudState:
    def __init__(self):
        self.jobs = {}
        self.fpga = {1: {"state": "idle", "session": None,
                         "current_job_id": None, "owner": None}}
        self.events = []
        self.submit_pending_polls = 0
        self.release_pending_polls = 0
        self.release_calls = []
        self.job_seq = 0
        # boards to assign to successive /submit builds (default board 1)
        self.assign_queue = []
        # wishbone board I/O shared with the /stream hijack
        self.board_io = {"accel": FakeAccel(), "mode": "normal",
                         "connections": 0, "frames": [], "go_count": 0}

    def new_job(self, jtype, status, fpga_id=None):
        self.job_seq += 1
        jid = f"job-{self.job_seq:04d}"
        self.jobs[jid] = {
            "job_id": jid, "type": jtype, "status": status,
            "fpga_id": fpga_id, "created_at": R.utcnow_iso(),
            "updated_at": R.utcnow_iso()}
        return self.jobs[jid]


class FakeCloudHandler(BaseHTTPRequestHandler):
    state: FakeCloudState = None
    daemon_threads = True

    def log_message(self, *a):
        pass

    def _json(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # -- WebSocket wishbone relay ------------------------------------------
    def _serve_stream(self):
        st = self.state
        io = st.board_io
        key = self.headers.get("Sec-WebSocket-Key")
        accept = base64.b64encode(hashlib.sha1(
            (key + WS_GUID).encode()).digest()).decode()
        self.connection.sendall(
            ("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
             f"Connection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n"
             "\r\n").encode())
        self.connection.settimeout(60)
        io["connections"] += 1
        accel = io["accel"]
        sock = self.connection
        try:
            while True:
                opcode, payload = read_ws_frame(sock)
                if opcode is None or opcode == 8:
                    return
                if opcode == 9:                       # ping -> pong
                    send_ws_frame(sock, payload, opcode=10)
                    continue
                if opcode != 2:
                    return
                op, address, data, count = decode_request(payload)
                word = address // 4
                is_go = op == 1 and word == D.R_CTRL and D.CTRL_GO in data
                if is_go:
                    io["go_count"] += 1
                io["frames"].append((op, address, data, count))
                mode = io["mode"]
                if mode == "drop_first":
                    sock.close()
                    return
                if mode == "close_on_go" and is_go:
                    sock.close()                       # lose GO's reply
                    return
                if mode == "hang" and len(io["frames"]) > 1:
                    time.sleep(120)
                    return
                if op == 1:
                    if word == D.R_CTRL:
                        accel.write(D.R_CTRL, data[0])
                    elif word == D.R_MEMDATA and len(data) > 1:
                        accel.write_fifo(D.R_MEMDATA, data)
                    else:
                        for i, v in enumerate(data):
                            accel.write(word + i, v)
                    reply = b"\x00\x00\x00\x00"
                elif count == 1 and word != D.R_MEMWIN:
                    val = accel.read(word)
                    reply = (b"\x00\x00\x00\x01"
                             + struct.pack(">I", val & 0xFFFFFFFF))
                else:
                    if word == D.R_MEMWIN:
                        vals = accel._cur_stack[:count]
                    else:
                        vals = [accel.read(word + i)
                                for i in range(count)]
                    reply = (b"\x00"
                             + struct.pack(">I", len(vals))[1:]
                             + struct.pack(f">{len(vals)}I",
                                           *[v & 0xFFFFFFFF
                                             for v in vals]))
                send_ws_frame(sock, reply)
        except (OSError, ValueError):
            return

    # -- HTTP routes --------------------------------------------------------
    def do_GET(self):
        st = self.state
        if self.path.endswith("/stream"):
            return self._serve_stream()
        st.events.append(("GET", self.path))
        if self.path == "/usage":
            return self._json({"month": "2026-10", "build_minutes_used": 0,
                               "build_minutes_limit": 100,
                               "build_minutes_remaining": 100, "builds": []})
        if self.path == "/fpga":
            return self._json([dict(b, fpga_id=i)
                               for i, b in sorted(st.fpga.items())])
        if self.path == "/jobs":
            return self._json(list(st.jobs.values()))
        if self.path.startswith("/jobs/"):
            jid = self.path.split("/")[2]
            job = st.jobs.get(jid)
            if job is None:
                return self._json({"detail": "not found"}, 404)
            if job["status"] == "queued":
                if st.submit_pending_polls > 0:
                    st.submit_pending_polls -= 1
                else:
                    job["status"] = "complete"
            if job["type"] == "reset" and job["status"] == "running":
                if st.release_pending_polls > 0:
                    st.release_pending_polls -= 1
                else:
                    job["status"] = "complete"
                    board = st.fpga.get(job["fpga_id"])
                    if board:
                        board["state"] = "idle"
                        board["session"] = None
            return self._json(job)
        if self.path.startswith("/fpga/"):
            fid = int(self.path.split("/")[2])
            if self.path.endswith("/session"):
                sess = st.fpga[fid]["session"]
                if sess is None:
                    return self._json({"detail": "no session"}, 404)
                return self._json(sess)
            return self._json(st.fpga[fid])
        return self._json({"detail": "no route"}, 404)

    def do_POST(self):
        st = self.state
        length = int(self.headers.get("Content-Length") or 0)
        self.rfile.read(length)
        st.events.append(("POST", self.path,
                          self.headers.get("X-Session-ID")))
        if self.path == "/submit":
            fid = st.assign_queue.pop(0) if st.assign_queue else 1
            job = st.new_job("build_and_program", "complete", fpga_id=fid)
            if st.submit_pending_polls:
                job["status"] = "queued"
            board = st.fpga.setdefault(fid, {"state": "idle", "session": None,
                                             "current_job_id": None,
                                             "owner": None})
            board.update(state="reserved", current_job_id=None,
                         owner="tester")
            board["session"] = {
                "session_id": f"sess-{job['job_id']}", "fpga_id": fid,
                "owner": "tester", "expires_at": _iso(time.time() + 3600)}
            return self._json({"job_id": job["job_id"]})
        if "/session/release" in self.path:
            fid = int(self.path.split("/")[2])
            sid = self.headers.get("X-Session-ID")
            board = st.fpga[fid]
            cur = (board.get("session") or {}).get("session_id")
            if cur != sid:
                return self._json({"detail": "not your session"}, 409)
            job = st.new_job("reset", "running", fpga_id=fid)
            st.release_calls.append((fid, sid, job["job_id"]))
            return self._json({"job_id": job["job_id"]})
        return self._json({"detail": "no route"}, 404)

    def do_DELETE(self):
        st = self.state
        st.events.append(("DELETE", self.path))
        jid = self.path.split("/")[2]
        job = st.jobs.get(jid)
        if job:
            job["status"] = "cancelled"
        self.send_response(200)
        self.end_headers()


class FakeCloud:
    def __init__(self, accel=None, mode="normal"):
        self.state = FakeCloudState()
        if accel is not None or mode != "normal":
            self.state.board_io["accel"] = accel or FakeAccel()
            self.state.board_io["mode"] = mode
        handler = type("H", (FakeCloudHandler,), {"state": self.state})
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.httpd.daemon_threads = True
        self.port = self.httpd.server_address[1]
        threading.Thread(target=self.httpd.serve_forever,
                         daemon=True).start()

    @property
    def url(self):
        return f"http://127.0.0.1:{self.port}"

    @property
    def io(self):
        return self.state.board_io

    def cloud(self):
        return R.Cloud("test-key", self.url, username="tester")

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()


@contextlib.contextmanager
def fake_plane(accel=None, mode="normal"):
    cloud = FakeCloud(accel=accel, mode=mode)
    try:
        yield cloud
    finally:
        cloud.stop()


def child_solve_spec(tmp, cloud, cnf_text=TINY_SAT, expected="sat",
                     timeout_s=10.0):
    adir = Path(tmp) / "attempt"
    adir.mkdir(parents=True, exist_ok=True)
    cnf = adir / "input.cnf"
    cnf.write_text(cnf_text, encoding="utf-8")
    return {
        "command": "solve",
        "candidate_root": PIGWEED,
        "api_url": cloud.url,
        "fpga_id": 1,
        "events_path": str(adir / "events.jsonl"),
        "result_path": str(adir / "result.json"),
        "solve": {"cnf_path": str(cnf), "expected": expected,
                  "timeout_s": timeout_s, "attempt_dir": str(adir)},
    }


def run_child(spec, timeout=60):
    return subprocess.run(
        [sys.executable, "-B", os.path.join(HERE, "pigweed_fpga_worker.py"),
         "--job", "/dev/stdin"],
        input=json.dumps(spec), capture_output=True, text=True,
        timeout=timeout,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1",
             "MRG_API_KEY": "test-key"})


class WorkerAgainstFakePlane(unittest.TestCase):
    def test_full_solve_single_go_and_bounded_bursts(self):
        accel = FakeAccel(result=1, stack=[1], done_after_reads=2,
                          results=[1, 1])
        with fake_plane(accel) as cloud:
            with tempfile.TemporaryDirectory() as td:
                spec = child_solve_spec(td, cloud)
                proc = run_child(spec)
                self.assertEqual(proc.returncode, 0, proc.stderr[-500:])
                res = json.loads(Path(spec["result_path"]).read_text())
                self.assertEqual(res["status"], "sat")
                self.assertTrue(res["witness_valid"])
                self.assertEqual(cloud.io["go_count"], 1)
                bursts = [len(f[2]) for f in cloud.io["frames"]
                          if f[0] == 1 and f[1] // 4 == D.R_MEMDATA]
                self.assertTrue(bursts)
                self.assertTrue(all(b <= 256 for b in bursts))

    def test_send_side_disconnect_no_replay(self):
        accel = FakeAccel(result=1, stack=[1], done_after_reads=2)
        with fake_plane(accel, mode="drop_first") as cloud:
            with tempfile.TemporaryDirectory() as td:
                spec = child_solve_spec(td, cloud)
                proc = run_child(spec)
                res = json.loads(Path(spec["result_path"]).read_text())
                self.assertEqual(res["status"], "transport_error")
                # exactly one connection, exactly one frame: no replay
                self.assertEqual(cloud.io["connections"], 1)
                self.assertEqual(len(cloud.io["frames"]), 1)

    def test_lost_reply_after_go_is_terminal_single_go(self):
        accel = FakeAccel(result=1, stack=[1], done_after_reads=2)
        with fake_plane(accel, mode="close_on_go") as cloud:
            with tempfile.TemporaryDirectory() as td:
                spec = child_solve_spec(td, cloud)
                proc = run_child(spec)
                res = json.loads(Path(spec["result_path"]).read_text())
                self.assertEqual(res["status"], "transport_error")
                self.assertEqual(cloud.io["go_count"], 1)   # one GO frame
                self.assertEqual(cloud.io["connections"], 1)  # no reconnect

    def test_hung_child_killed_and_reaped_by_stage_watchdog(self):
        accel = FakeAccel(result=1, stack=[1], done_after_reads=2)
        with fake_plane(accel, mode="hang") as cloud:
            with tempfile.TemporaryDirectory() as td:
                spec = child_solve_spec(td, cloud)
                spec["_job_path"] = str(Path(td) / "job.json")
                result, kill = R.supervise_worker(spec, {"stage": 3.0})
                self.assertIsNone(result)
                self.assertIsNotNone(kill)
                self.assertIn("stage watchdog", kill)


class JournalReconciliation(unittest.TestCase):
    def make_run(self, tdir):
        run_dir = Path(tdir)
        run_dir.mkdir(parents=True, exist_ok=True)
        return run_dir

    def test_no_delta_resolves_never_reached(self):
        with tempfile.TemporaryDirectory() as td:
            run_dir = self.make_run(td)
            with fake_plane() as cloud:
                j = R.Journal(run_dir)
                j.append("submit_intent", design_sha256="x",
                         sys_clk_hz=50_000_000,
                         timing_target_mhz=50.0, own_jobs_before=[])
                with mock.patch.object(R, "SUBMIT_INFLIGHT_WINDOW_S", 0):
                    R.reconcile(run_dir, j, cloud.cloud())
                types = [e["type"] for e in j.entries()]
                self.assertIn("resolved", types)

    def test_unique_delta_blocks_without_binding(self):
        # plan rule: an own-job delta is only a CANDIDATE; job records
        # carry no source provenance, so automatic adoption is impossible
        # -- block without mutation instead of guessing
        with tempfile.TemporaryDirectory() as td:
            run_dir = self.make_run(td)
            with fake_plane() as cloud:
                j = R.Journal(run_dir)
                j.append("submit_intent", design_sha256="x",
                         sys_clk_hz=50_000_000, timing_target_mhz=50.0,
                         own_jobs_before=[])
                cloud.state.new_job("build_and_program", "failed")
                with mock.patch.object(R, "SUBMIT_INFLIGHT_WINDOW_S", 0):
                    with self.assertRaises(R.Blocker):
                        R.reconcile(run_dir, j, cloud.cloud())
                self.assertNotIn("submit_accepted",
                                 [e["type"] for e in j.entries()])
                # no mutation while blocked: only the read-only job list
                self.assertTrue(all(ev[0] == "GET" and ev[1] == "/jobs"
                                    for ev in cloud.state.events))

    def test_ambiguous_delta_blocks(self):
        with tempfile.TemporaryDirectory() as td:
            run_dir = self.make_run(td)
            with fake_plane() as cloud:
                j = R.Journal(run_dir)
                j.append("submit_intent", design_sha256="x",
                         sys_clk_hz=50_000_000, timing_target_mhz=50.0,
                         own_jobs_before=[])
                cloud.state.new_job("build_and_program", "failed")
                cloud.state.new_job("build_and_program", "failed")
                with self.assertRaises(R.Blocker):
                    R.reconcile(run_dir, j, cloud.cloud())

    def test_completed_job_live_session_released(self):
        with tempfile.TemporaryDirectory() as td:
            run_dir = self.make_run(td)
            with fake_plane() as cloud:
                c = cloud.cloud()
                job = cloud.state.new_job("build_and_program", "complete",
                                          fpga_id=1)
                cloud.state.fpga[1].update(
                    state="reserved", current_job_id=None,
                    owner="tester")
                cloud.state.fpga[1]["session"] = {
                    "session_id": "sess-adopt", "fpga_id": 1,
                    "owner": "tester", "expires_at": _iso(time.time() + 3600)}
                j = R.Journal(run_dir)
                j.append("submit_accepted", job_id=job["job_id"])
                R.reconcile(run_dir, j, c)
                self.assertEqual(cloud.state.fpga[1]["state"], "idle")
                self.assertIsNone(cloud.state.fpga[1]["session"])
                terms = [e for e in j.entries()
                         if e["type"] == "release_terminal"]
                self.assertEqual(len(terms), 1)
                self.assertTrue(terms[0]["session_gone"])

    def test_same_account_session_from_another_build_not_released(self):
        with tempfile.TemporaryDirectory() as td:
            run_dir = self.make_run(td)
            with fake_plane() as cloud:
                c = cloud.cloud()
                job = cloud.state.new_job("build_and_program", "complete",
                                          fpga_id=1)
                cloud.state.fpga[1].update(state="reserved", owner="tester")
                # same account, expiry 60 s off this job's completion + TTL
                cloud.state.fpga[1]["session"] = {
                    "session_id": "sess-other-build", "fpga_id": 1,
                    "owner": "tester",
                    "expires_at": _iso(time.time() + 3660)}
                j = R.Journal(run_dir)
                j.append("submit_accepted", job_id=job["job_id"])
                R.reconcile(run_dir, j, c)
                self.assertEqual(cloud.state.release_calls, [])
                self.assertEqual(cloud.state.fpga[1]["session"]["session_id"],
                                 "sess-other-build")

    def test_unjournaled_near_time_competitor_blocks_release(self):
        # session expiry matches job A exactly, but another own build B
        # (foreign submit) reached the same board within the window: the
        # session cannot be attributed -> block, never release
        with tempfile.TemporaryDirectory() as td:
            run_dir = self.make_run(td)
            with fake_plane() as cloud:
                c = cloud.cloud()
                a = cloud.state.new_job("build_and_program", "complete",
                                        fpga_id=1)
                cloud.state.new_job("build_and_program", "complete",
                                    fpga_id=1)         # foreign, +-secs
                cloud.state.fpga[1].update(state="reserved", owner="tester")
                cloud.state.fpga[1]["session"] = {
                    "session_id": "sess-b", "fpga_id": 1,
                    "owner": "tester",
                    "expires_at": _iso(time.time() + 3600)}
                j = R.Journal(run_dir)
                j.append("submit_accepted", job_id=a["job_id"])
                with self.assertRaises(R.Blocker) as ctx:
                    R.reconcile(run_dir, j, c)
                self.assertIn("ambiguous", str(ctx.exception))
                self.assertEqual(cloud.state.release_calls, [])

    def test_journaled_session_job_never_rederived_from_board(self):
        # session_assigned is journaled for this job; a DIFFERENT same-
        # account session that happens to pass the timing relation must not
        # be released (only the journaled session id may be)
        with tempfile.TemporaryDirectory() as td:
            run_dir = self.make_run(td)
            with fake_plane() as cloud:
                c = cloud.cloud()
                job = cloud.state.new_job("build_and_program", "complete",
                                          fpga_id=1)
                cloud.state.fpga[1].update(state="reserved", owner="tester")
                cloud.state.fpga[1]["session"] = {
                    "session_id": "sess-j2", "fpga_id": 1,
                    "owner": "tester",
                    "expires_at": _iso(time.time() + 3600)}
                j = R.Journal(run_dir)
                j.append("submit_accepted", job_id=job["job_id"])
                j.append("session_assigned", job_id=job["job_id"],
                         fpga_id=1, session_id="sess-j1")
                R.reconcile(run_dir, j, c)
                self.assertEqual(cloud.state.release_calls, [])
                self.assertEqual(cloud.state.fpga[1]["session"]["session_id"],
                                 "sess-j2")

    def test_account_lock_is_exclusive_and_releasable(self):
        with tempfile.TemporaryDirectory() as td:
            with mock.patch.dict(os.environ, {"TMPDIR": td}):
                first = R.account_lock("acct")
                with self.assertRaises(R.Blocker):
                    R.account_lock("acct")
                other = R.account_lock("someone-else")   # independent
                other.close()
                first.close()
                R.account_lock("acct").close()

    def test_quota_floor_blocks_low_remaining(self):
        with fake_plane() as cloud:
            c = cloud.cloud()
            with mock.patch.object(R.Cloud, "usage", lambda self: {
                    "build_minutes_remaining": R.BUILD_QUOTA_FLOOR_MIN - 1}):
                with self.assertRaises(R.AccessBlocked):
                    c.check_quota()
            with mock.patch.object(R.Cloud, "usage", lambda self: {
                    "build_minutes_remaining": R.BUILD_QUOTA_FLOOR_MIN}):
                c.check_quota()

    def test_env_key_override_yields_no_username(self):
        with tempfile.TemporaryDirectory() as td:
            cfg = Path(td) / "mrg"
            cfg.mkdir()
            (cfg / "credentials.json").write_text(json.dumps(
                {R.API_URL: {"api_key": "stored-key",
                             "github_username": "stored-user"}}))
            real = _USERNAME_PATCH     # the module-level stub must be off
            real.stop()
            try:
                with mock.patch.dict(os.environ, {"XDG_CONFIG_HOME": td},
                                     clear=False):
                    os.environ.pop("MRG_API_KEY", None)
                    self.assertEqual(R.resolve_username(), "stored-user")
                    os.environ["MRG_API_KEY"] = "stored-key"
                    self.assertEqual(R.resolve_username(), "stored-user")
                    os.environ["MRG_API_KEY"] = "some-other-key"
                    self.assertEqual(R.resolve_username(), "")
            finally:
                real.start()

    def test_acquire_refuses_same_account_session_of_other_build(self):
        with fake_plane(FakeAccel()) as cloud:
            with tempfile.TemporaryDirectory() as td:
                run_dir = Path(td)
                R.snapshot_candidate(run_dir)
                j = R.Journal(run_dir)
                orig = cloud.state.fpga[1]
                real_get_job = R.Cloud.get_job

                def late_job(self_, jid):
                    # the board's session is replaced by a same-account
                    # session minted for a different build before attach
                    out = real_get_job(self_, jid)
                    if out["status"] == "complete":
                        orig["session"]["expires_at"] = \
                            _iso(time.time() + 9000)
                    return out

                with mock.patch.object(R.Cloud, "get_job", late_job):
                    with self.assertRaises(R.Blocker) as ctx:
                        R.acquire_session(run_dir, j, cloud.cloud())
                self.assertIn("not reserved by our account",
                              str(ctx.exception))

    def test_submit_refused_while_own_job_or_board_held(self):
        with fake_plane(FakeAccel()) as cloud:
            with tempfile.TemporaryDirectory() as td:
                run_dir = Path(td)
                R.snapshot_candidate(run_dir)
                j = R.Journal(run_dir)
                cloud.state.new_job("build_and_program", "running")
                with self.assertRaises(R.Blocker) as ctx:
                    R.acquire_session(run_dir, j, cloud.cloud())
                self.assertIn("non-terminal", str(ctx.exception))
                self.assertEqual([e for e in j.entries()
                                  if e["type"] == "submit_intent"], [])
        with fake_plane(FakeAccel()) as cloud:
            with tempfile.TemporaryDirectory() as td:
                run_dir = Path(td)
                R.snapshot_candidate(run_dir)
                j = R.Journal(run_dir)
                cloud.state.fpga[1].update(state="reserved", owner="tester")
                cloud.state.fpga[1]["session"] = {
                    "session_id": "sess-held", "fpga_id": 1,
                    "owner": "tester", "expires_at": _iso(time.time() + 600)}
                with self.assertRaises(R.Blocker) as ctx:
                    R.acquire_session(run_dir, j, cloud.cloud())
                self.assertIn("already owns board", str(ctx.exception))
                self.assertEqual([e for e in j.entries()
                                  if e["type"] == "submit_intent"], [])

    def test_excluded_board_released_before_io_and_rebuilt(self):
        # first build lands on excluded board 6: its session is released
        # with no stream I/O and no session record, then a rebuild lands on
        # board 1 and becomes the campaign session
        with fake_plane(FakeAccel()) as cloud:
            with tempfile.TemporaryDirectory() as td:
                run_dir = Path(td)
                R.snapshot_candidate(run_dir)
                j = R.Journal(run_dir)
                cloud.state.assign_queue = [6, 1]
                qualified = []
                with mock.patch.object(
                        R, "qualify_via_worker",
                        lambda rd, jj, c, s: qualified.append(s.fpga_id)):
                    s = R.acquire_and_qualify(run_dir, j, cloud.cloud())
                self.assertEqual(s.fpga_id, 1)
                self.assertEqual(qualified, [1])        # board 6 never used
                self.assertEqual(cloud.state.board_io["connections"], 0)
                self.assertEqual([c[0] for c in cloud.state.release_calls],
                                 [6])
                self.assertIsNone(cloud.state.fpga[6]["session"])
                skipped = [e for e in j.entries()
                           if e["type"] == "board_skipped"]
                self.assertEqual([e["fpga_id"] for e in skipped], [6])
                recs = [json.loads(p.read_text()) for p in
                        (run_dir / "sessions").glob("[0-9][0-9][0-9].json")]
                self.assertEqual([r["fpga_id"] for r in recs], [1])

    def test_excluded_board_skip_limit_stops_with_nothing_held(self):
        with fake_plane(FakeAccel()) as cloud:
            with tempfile.TemporaryDirectory() as td:
                run_dir = Path(td)
                R.snapshot_candidate(run_dir)
                j = R.Journal(run_dir)
                cloud.state.assign_queue = [6] * (R.MAX_BOARD_SKIPS + 1) + [1]
                with mock.patch.object(R, "qualify_via_worker",
                                       lambda *a: self.fail("qualified")):
                    with self.assertRaises(R.Blocker) as ctx:
                        R.acquire_and_qualify(run_dir, j, cloud.cloud())
                self.assertIn("excluded board", str(ctx.exception))
                submits = [e for e in cloud.state.events
                           if e[:2] == ("POST", "/submit")]
                self.assertEqual(len(submits), R.MAX_BOARD_SKIPS + 1)
                self.assertEqual(len(cloud.state.release_calls),
                                 R.MAX_BOARD_SKIPS + 1)
                self.assertIsNone(cloud.state.fpga[6]["session"])
                self.assertEqual(cloud.state.assign_queue, [1])  # no 5th

    def test_pending_release_intent_completed(self):
        with tempfile.TemporaryDirectory() as td:
            run_dir = self.make_run(td)
            with fake_plane() as cloud:
                c = cloud.cloud()
                cloud.state.fpga[1].update(state="reserved", owner="tester")
                cloud.state.fpga[1]["session"] = {
                    "session_id": "sess-pend", "fpga_id": 1,
                    "owner": "tester", "expires_at": _iso(time.time() + 600)}
                j = R.Journal(run_dir)
                j.append("release_intent", fpga_id=1, session_id="sess-pend",
                         reason="crash")
                R.reconcile(run_dir, j, c)
                self.assertEqual(cloud.state.fpga[1]["state"], "idle")
                self.assertEqual(len(cloud.state.release_calls), 1)

    def test_reassigned_session_never_released(self):
        with tempfile.TemporaryDirectory() as td:
            run_dir = self.make_run(td)
            with fake_plane() as cloud:
                c = cloud.cloud()
                cloud.state.fpga[1].update(state="reserved", owner="other")
                cloud.state.fpga[1]["session"] = {
                    "session_id": "sess-other", "fpga_id": 1,
                    "owner": "other", "expires_at": _iso(time.time() + 600)}
                j = R.Journal(run_dir)
                j.append("session_assigned", fpga_id=1,
                         session_id="sess-ours", job_id="job-0001")
                R.reconcile(run_dir, j, c)
                self.assertEqual(cloud.state.release_calls, [])
                self.assertEqual(cloud.state.fpga[1]["session"]
                                 ["session_id"], "sess-other")
                evidence = [e for e in j.entries()
                            if e["type"] == "session_expired_or_reassigned"]
                self.assertEqual(len(evidence), 1)   # retained, no mutation


class FullCampaignOnFakePlane(unittest.TestCase):
    def test_cmd_run_end_to_end(self):
        # GO order on one session: q-SAT, q-UNSAT, q-SAT, abort-demo,
        # post-abort qualification recovery, then three scored solves
        accel = FakeAccel(result=1, stack=[1], done_after_reads=2,
                          results=[1, 0, 1, 1, 1, 1, 1, 1],
                          stacks=[[1, 2], [], [2], [], [1], [1], [1], [1]])
        with fake_plane(accel) as cloud:
            with tempfile.TemporaryDirectory() as td:
                run_dir = Path(td) / "run"
                run_dir.mkdir()
                R.snapshot_candidate(run_dir)
                (run_dir / "inputs").mkdir()
                (run_dir / "inputs" / "TEST-1.cnf").write_text(
                    TINY_SAT, encoding="utf-8")
                with open(run_dir / "inventory.csv", "w", newline="") as f:
                    w = csv.writer(f)
                    w.writerow(["id", "tier", "family", "category",
                                "result_gbd", "expected", "eligible",
                                "reasons", "semantics_status",
                                "build_status", "field_flags",
                                "cpu_certified", "cpu_certified_kind",
                                "cpu_attempt", "pilot"])
                    w.writerow(["TEST-1", "S", "f", "main", "sat", "sat",
                                "true", "", "ok", "loaded", "", "true",
                                "sat_witness", "1", "false"])
                with mock.patch.object(R, "API_URL", cloud.url), \
                        mock.patch.object(R, "resolve_api_key",
                                          lambda: "test-key"):
                    run = {"schema": R.RUN_SCHEMA, "phase": "qualified",
                           "created_utc": R.utcnow_iso(),
                           "identity": R.current_identity(run_dir)}
                    R.atomic_write_json(run_dir / "run.json", run)
                    rc = R.cmd_run(str(run_dir), resume=False)
                self.assertEqual(rc, 0)
                # one session acquired, qualified, released
                self.assertEqual(len(cloud.state.release_calls), 1)
                self.assertEqual(cloud.state.fpga[1]["state"], "idle")
                with open(run_dir / "results.csv") as f:
                    rows = list(csv.DictReader(f))
                self.assertEqual(len(rows), 3)
                self.assertTrue(all(r["status"] == "sat" for r in rows))
                entries = [json.loads(l)
                           for l in (run_dir / "journal.jsonl")
                           .read_text().splitlines() if l.strip()]
                types = [e["type"] for e in entries]
                self.assertIn("submit_intent", types)
                self.assertIn("submit_accepted", types)
                self.assertIn("session_assigned", types)
                self.assertIn("qualification", types)
                self.assertEqual(types.count("attempt_recorded"), 3)
                self.assertIn("release_terminal", types)
                final_run = json.loads((run_dir / "run.json").read_text())
                self.assertEqual(final_run["phase"], "run_done")
                # exactly one GO per scored attempt (qualification GOs are
                # not part of the scored events stream)
                scored_gos = sum(
                    1 for a in run_dir.rglob("worker-events.jsonl")
                    for line in a.read_text().splitlines()
                    if '"go_sent"' in line)
                self.assertEqual(scored_gos, 3)


# ---------------------------------------------------------------------------
# Fail-closed paths from the pre-booking review
# ---------------------------------------------------------------------------

def campaign_run_dir(td, cloud):
    """Minimal qualified run directory wired to the fake cloud."""
    run_dir = Path(td) / "run"
    run_dir.mkdir()
    R.snapshot_candidate(run_dir)
    (run_dir / "inputs").mkdir()
    (run_dir / "inputs" / "TEST-1.cnf").write_text(TINY_SAT,
                                                   encoding="utf-8")
    with open(run_dir / "inventory.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "tier", "family", "category", "result_gbd",
                    "expected", "eligible", "reasons", "semantics_status",
                    "build_status", "field_flags", "cpu_certified",
                    "cpu_certified_kind", "cpu_attempt", "pilot"])
        w.writerow(["TEST-1", "S", "f", "main", "sat", "sat", "true", "",
                    "ok", "loaded", "", "true", "sat_witness", "1",
                    "false"])
    with mock.patch.object(R, "API_URL", cloud.url), \
            mock.patch.object(R, "resolve_api_key", lambda: "test-key"):
        run = {"schema": R.RUN_SCHEMA, "phase": "qualified",
               "created_utc": R.utcnow_iso(),
               "identity": R.current_identity(run_dir)}
        R.atomic_write_json(run_dir / "run.json", run)
    return run_dir


class FailClosedPaths(unittest.TestCase):
    def test_classify_done_correctness_before_lateness(self):
        self.assertEqual(W.classify_done(1, True, "sat"), "sat")
        self.assertEqual(W.classify_done(1, True, "unsat"), "wrong_answer")
        self.assertEqual(W.classify_done(1, False, "sat"), "wrong_answer")
        self.assertEqual(W.classify_done(1, None, "sat"), "wrong_answer")
        self.assertEqual(W.classify_done(0, None, "unsat"), "unsat")
        self.assertEqual(W.classify_done(0, None, "sat"), "wrong_answer")

    def test_uncancellable_build_409_settles_raced_session(self):
        # DELETE /jobs/{id} returns 409 when the build is already flashing:
        # the cancel path must still wait out the job and release the board
        # it reserved, not abandon the reservation.
        with fake_plane(FakeAccel()) as cloud:
            with tempfile.TemporaryDirectory() as td:
                run_dir = Path(td)
                R.snapshot_candidate(run_dir)
                job = cloud.state.new_job("build_and_program", "running",
                                          fpga_id=1)
                cloud.state.fpga[1].update(
                    state="reserved", current_job_id=None,
                    owner="tester")
                cloud.state.fpga[1]["session"] = {
                    "session_id": "sess-409", "fpga_id": 1,
                    "owner": "tester", "expires_at": _iso(time.time() + 3600)}

                class Refuse409:
                    status_code = 409

                    def __getattr__(self, name):
                        return None

                def refuse_delete(self, job_id):
                    import requests
                    raise requests.HTTPError("409 already flashing",
                                             response=Refuse409())

                calls = {"n": 0}
                real_get_job = R.Cloud.get_job

                def finishing_get_job(self, jid):
                    if jid != job["job_id"]:
                        return real_get_job(self, jid)
                    calls["n"] += 1
                    if calls["n"] > 1:
                        cloud.state.jobs[jid]["status"] = "complete"
                    return cloud.state.jobs[jid]

                with mock.patch.object(R.Cloud, "cancel_job",
                                       refuse_delete), \
                        mock.patch.object(R.Cloud, "get_job",
                                           finishing_get_job):
                    j = R.Journal(run_dir)
                    R._cancel_and_settle(run_dir, j, cloud.cloud(),
                                         job["job_id"], "test 409")
                types = [e["type"] for e in j.entries()]
                self.assertIn("cancel_refused_409", types)
                self.assertEqual(cloud.state.fpga[1]["state"], "idle")
                self.assertIsNone(cloud.state.fpga[1]["session"])

    def test_sleep_guard_halts_before_any_new_bus_call(self):
        # wall clock runs far ahead of monotonic (system slept): the poll
        # loop must go terminal BEFORE issuing the next CTRL read
        class SleepyWall:
            def __init__(self):
                self.t = time.time()
                self.calls = 0

            def __call__(self):
                self.calls += 1
                return self.t + 1000.0 * self.calls   # huge jump per call

        accel = FakeAccel(result=1, stack=[1], done_after_reads=2,
                          results=[1, 1])
        bus = make_bus(accel)
        res = W.run_one(bus, HOST, DRV, tiny_img(),
                        BC.parse_dimacs(TINY_SAT), "sat", timeout_s=2.0,
                        mono=FakeMono(), sleep=lambda s: None,
                        wall=SleepyWall())
        self.assertEqual(res["status"], "transport_error")
        self.assertIn("jumped", res["transport_error"])
        ctrl_reads = sum(1 for f in accel.frames
                         if f[0] == "read" and f[1] == D.R_CTRL)
        # exactly one CTRL read (the pre-GO abort-to-idle poll): the scored
        # poll loop added none after detecting the sleep jump
        self.assertEqual(ctrl_reads, 1)

    def test_live_expiry_rotation_keeps_queue_head(self):
        # server shortens the lease between acquisition and dispatch:
        # cmd_run must rotate to a fresh session and keep the same (id,
        # repetition) at the head of the work queue
        # two full qualification sequences (one per session) + 3 solves
        accel = FakeAccel(result=1, stack=[1], done_after_reads=2,
                          results=[1, 0, 1, 1, 1] * 2 + [1, 1, 1],
                          stacks=[[1, 2], [], [2], [], [1]] * 2
                                 + [[1], [1], [1]],
                          done_afters=[2] * 13)
        with fake_plane(accel) as cloud:
            with tempfile.TemporaryDirectory() as td:
                run_dir = campaign_run_dir(td, cloud)
                state = {"dispatches": 0}
                orig_exec = R.execute_attempt

                def counting_exec(rd, j, c, s, row, rep, n, stop):
                    state["dispatches"] += 1
                    if state["dispatches"] == 1:
                        # the server shortens this live lease right before
                        # the first dispatch: the live gate must rotate
                        c.skew_s = c.skew_s or 0.0
                        cloud.state.fpga[1]["session"]["expires_at"] = \
                            _iso(time.time() + 100)
                        s.expires_at = time.time() + 100
                    return orig_exec(rd, j, c, s, row, rep, n, stop)

                with mock.patch.object(R, "API_URL", cloud.url), \
                        mock.patch.object(R, "resolve_api_key",
                                          lambda: "test-key"), \
                        mock.patch.object(R, "execute_attempt",
                                          counting_exec):
                    rc = R.cmd_run(str(run_dir), resume=False)
                self.assertEqual(rc, 0)
                # two builds: the stale session was rotated away
                self.assertGreaterEqual(len(cloud.state.release_calls), 2)
                with open(run_dir / "results.csv") as f:
                    rows = list(csv.DictReader(f))
                self.assertEqual(len(rows), 3)
                self.assertTrue(all(r["status"] == "sat" for r in rows))
                # the rotated repetition ran as attempt-002 of rep 1
                self.assertIn("attempt-002",
                              [p.name for p in
                               (run_dir / "attempts" / "TEST-1" /
                                "repetition-001").iterdir()])

    def test_old_created_at_ceiling_cancels_build(self):
        # an adopted build whose server created_at is already past the
        # ceiling must be cancelled, not granted a fresh local window
        with fake_plane(FakeAccel()) as cloud:
            with tempfile.TemporaryDirectory() as td:
                run_dir = Path(td)
                R.snapshot_candidate(run_dir)
                old = _iso(time.time() - 10 * R.BUILD_CEILING_S)
                job = cloud.state.new_job("build_and_program", "running")
                job["created_at"] = old
                j = R.Journal(run_dir)
                c = cloud.cloud()
                c.skew_s = 0.0          # isolate the old-timestamp branch
                with mock.patch.object(R.Cloud, "get_job",
                                       side_effect=lambda jid, *a, **k:
                                       cloud.state.jobs[jid]):
                    with self.assertRaises(R.Blocker) as ctx:
                        R.wait_build_terminal(c, j, job["job_id"],
                                              run_dir=run_dir)
                self.assertIn("ceiling", str(ctx.exception))
                self.assertEqual(job["status"], "cancelled")

    def test_missing_created_at_fails_closed(self):
        with fake_plane(FakeAccel()) as cloud:
            with tempfile.TemporaryDirectory() as td:
                run_dir = Path(td)
                R.snapshot_candidate(run_dir)
                job = cloud.state.new_job("build_and_program", "running")
                del job["created_at"]
                j = R.Journal(run_dir)
                c = cloud.cloud()
                with mock.patch.object(R.Cloud, "get_job",
                                       side_effect=lambda jid, *a, **k:
                                       cloud.state.jobs[jid]):
                    with self.assertRaises(R.Blocker):
                        R.wait_build_terminal(c, j, job["job_id"],
                                              run_dir=run_dir)
                self.assertEqual(job["status"], "cancelled")

    def test_skew_loss_mid_poll_cancels_build(self):
        with fake_plane(FakeAccel()) as cloud:
            with tempfile.TemporaryDirectory() as td:
                run_dir = Path(td)
                R.snapshot_candidate(run_dir)
                job = cloud.state.new_job("build_and_program", "running")
                j = R.Journal(run_dir)
                c = cloud.cloud()
                c.skew_s = 0.0          # usable at entry
                calls = {"n": 0}
                real_get = c.get_job

                def clearing_get(jid):
                    out = real_get(jid)
                    calls["n"] += 1
                    if calls["n"] > 1:
                        c.skew_s = None  # a Date-less poll clears evidence
                    return out

                with mock.patch.object(c, "get_job", clearing_get):
                    with self.assertRaises(R.Blocker):
                        R.wait_build_terminal(c, j, job["job_id"],
                                              run_dir=run_dir)
                self.assertEqual(job["status"], "cancelled")

    def test_recovery_over_budget_is_not_ok(self):
        # the stage bound: a recovery whose clock jumps past its budget
        # after the solve completes cannot be a pass, whichever stage
        # check catches it
        class JumpAfterDone:
            """Normal 0.05s ticks; jumps +1000s once the accel reports the
            trivial solve done (recovery then trips a budget check)."""

            def __init__(self, accel):
                self.t = 100.0
                self.accel = accel

            def __call__(self):
                self.t += 0.05
                frames = self.accel.frames
                done_reads = sum(1 for f in frames
                                 if f[0] == "read" and f[1] == D.R_CTRL
                                 and False)
                # jump once any post-GO done has been observed (the bus
                # records CTRL reads with done set via _ctrl_reads)
                if self.accel._gos >= 1 and self.accel._ctrl_reads >= 2:
                    self.t += 1000.0
                return self.t

        accel = FakeAccel(result=1, stack=[1], done_after_reads=2,
                          results=[1])
        res = W.synthetic_recovery(make_bus(accel), HOST, DRV,
                                   mono=JumpAfterDone(accel),
                                   budget_s=10.0)
        self.assertFalse(res["ok"])
        self.assertIn("budget", res.get("reason", ""))

    def test_stale_skew_cleared_on_missing_date(self):
        class FakeResp:
            status_code = 200
            headers = {}
            text = "{}"

            def raise_for_status(self):
                pass

            def json(self):
                return {}

        with fake_plane() as cloud:
            c = cloud.cloud()
            c.skew_s = 0.5          # stale evidence from an earlier call
            c._resp(FakeResp())
            self.assertIsNone(c.skew_s)

    def test_late_contradiction_is_wrong_answer_not_timeout(self):
        # late SAT answer whose valid witness contradicts the CPU-certified
        # UNSAT expectation: wrong_answer even though it is also late
        res = RunOneClassification.run_one(
            FakeAccel(result=1, stack=[1], done_after_reads=1,
                      results=[1, 1]),
            expected="unsat", original=BC.parse_dimacs(TINY_SAT),
            timeout_s=1.0, mono=FakeMono(step=0.4), recovery_budget_s=5.0)
        self.assertTrue(res["late"])
        self.assertEqual(res["status"], "wrong_answer")

    def test_late_invalid_witness_is_wrong_answer(self):
        res = RunOneClassification.run_one(
            FakeAccel(result=1, stack=[1, -1], done_after_reads=1,
                      results=[1, 1]),
            expected="sat", original=BC.parse_dimacs(TINY_SAT),
            timeout_s=1.0, mono=FakeMono(step=0.4), recovery_budget_s=5.0)
        self.assertTrue(res["late"])
        self.assertEqual(res["status"], "wrong_answer")

    def test_conservative_remaining_requires_skew_evidence(self):
        class NoSkew:
            skew_s = None
        self.assertIsNone(R.conservative_remaining_s(time.time() + 5000,
                                                     NoSkew(), 30.0))
        self.assertIsNone(R.conservative_remaining_s(None, NoSkew(), 30.0))

    def test_check_quota_boolean_fails_closed(self):
        # JSON true must not satisfy an int check (bool subclasses int)
        with fake_plane() as cloud:
            c = cloud.cloud()
            with mock.patch.object(c, "usage", return_value={
                    "month": "x", "build_minutes_remaining": True}):
                with self.assertRaises(R.AccessBlocked):
                    c.check_quota()

    def test_check_quota_missing_field_fails_closed(self):
        with fake_plane() as cloud:
            c = cloud.cloud()
            with mock.patch.object(c, "usage", return_value={"month": "x"}):
                with self.assertRaises(R.AccessBlocked):
                    c.check_quota()
    def test_pnr_refuses_digest_substring_spoof(self):
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            R.snapshot_candidate(run_dir)
            with mock.patch.dict(os.environ, {"MRG_SANDBOX_IMAGE":
                    "ghcr.io/dev/mrg-sandbox:latest-"
                    + R.SANDBOX_DIGEST}):
                with self.assertRaises(R.Blocker):
                    R.run_local_pnr(run_dir)

    def test_snapshot_refuses_git_status_failure(self):
        real_run = subprocess.run

        def fake_run(cmd, **kw):
            if "status" in cmd:
                class P:
                    returncode = 128
                    stdout = ""
                    stderr = "fatal: index corrupt"
                return P()
            return real_run(cmd, **kw)

        with tempfile.TemporaryDirectory() as td:
            with mock.patch.object(R.subprocess, "run", fake_run):
                with self.assertRaises(R.Blocker):
                    R.snapshot_candidate(Path(td))

    def test_complete_release_with_session_present_blocks(self):
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            with fake_plane() as cloud:
                job = cloud.state.new_job("reset", "complete", fpga_id=1)
                # reset job complete but our session still on the board
                cloud.state.fpga[1].update(state="reserved", owner="tester")
                cloud.state.fpga[1]["session"] = {
                    "session_id": "sess-live", "fpga_id": 1,
                    "owner": "tester", "expires_at": _iso(time.time() + 600)}
                j = R.Journal(run_dir)
                j.append("release_accepted", job_id=job["job_id"],
                         fpga_id=1, session_id="sess-live")
                with mock.patch.object(R, "SUBMIT_INFLIGHT_WINDOW_S", 0):
                    with self.assertRaises(R.Blocker):
                        R.reconcile(run_dir, j, cloud.cloud())

    def test_competing_intent_blocks_adoption(self):
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            with fake_plane() as cloud:
                j = R.Journal(run_dir)
                j.append("submit_intent", design_sha256="x",
                         sys_clk_hz=50_000_000, timing_target_mhz=50.0,
                         own_jobs_before=[])
                j.append("submit_intent", design_sha256="y",
                         sys_clk_hz=50_000_000, timing_target_mhz=50.0,
                         own_jobs_before=[])
                job = cloud.state.new_job("build_and_program", "failed")
                with mock.patch.object(R, "SUBMIT_INFLIGHT_WINDOW_S", 0):
                    with self.assertRaises(R.Blocker):
                        R.reconcile(run_dir, j, cloud.cloud())
                self.assertNotIn("submit_accepted",
                                 [e["type"] for e in j.entries()])


    def test_corrupt_journal_blocks_reconcile(self):
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            (run_dir / "journal.jsonl").write_text(
                '{"seq": 1, "type": "submit_inte', encoding="utf-8")
            with fake_plane() as cloud:
                with self.assertRaises(R.Blocker):
                    R.reconcile(run_dir, R.Journal(run_dir), cloud.cloud())

    def test_inflight_window_waited_out_before_no_effect(self):
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            with fake_plane() as cloud:
                j = R.Journal(run_dir)
                j.append("submit_intent", design_sha256="x",
                         sys_clk_hz=50_000_000, timing_target_mhz=50.0,
                         own_jobs_before=[])
                with mock.patch.object(R, "SUBMIT_INFLIGHT_WINDOW_S", 0.5):
                    R.reconcile(run_dir, j, cloud.cloud())
                self.assertIn("resolved", [e["type"] for e in j.entries()])

    def test_failed_release_job_blocks_reconcile(self):
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            with fake_plane() as cloud:
                job = cloud.state.new_job("reset", "failed", fpga_id=1)
                j = R.Journal(run_dir)
                j.append("release_accepted", job_id=job["job_id"],
                         fpga_id=1, session_id="sess-x")
                with self.assertRaises(R.Blocker):
                    R.reconcile(run_dir, j, cloud.cloud())

    def test_acquire_links_intent_and_binds_job(self):
        with fake_plane(FakeAccel()) as cloud:
            with tempfile.TemporaryDirectory() as td:
                run_dir = Path(td)
                R.snapshot_candidate(run_dir)
                j = R.Journal(run_dir)
                s = R.acquire_session(run_dir, j, cloud.cloud())
                entries = j.entries()
                intent = [e for e in entries
                          if e["type"] == "submit_intent"]
                accepted = [e for e in entries
                            if e["type"] == "submit_accepted"]
                self.assertEqual(len(intent), 1)
                self.assertEqual(accepted[0]["submit_intent_seq"],
                                 intent[0]["seq"])
                # real control plane: no current_job_id for a build session
                self.assertIsNone(cloud.state.fpga[1]["current_job_id"])
                self.assertEqual(cloud.state.fpga[1]["session"]
                                 ["session_id"], s.session_id)

    def test_acquire_refuses_board_owned_by_another_account(self):
        with fake_plane(FakeAccel()) as cloud:
            with tempfile.TemporaryDirectory() as td:
                run_dir = Path(td)
                R.snapshot_candidate(run_dir)
                j = R.Journal(run_dir)
                c = R.Cloud("test-key", cloud.url, username="not-the-owner")
                with self.assertRaises(R.Blocker) as ctx:
                    R.acquire_session(run_dir, j, c)
                self.assertIn("not reserved by our account",
                              str(ctx.exception))

    def test_execute_attempt_refuses_reassigned_session(self):
        with fake_plane(FakeAccel()) as cloud:
            with tempfile.TemporaryDirectory() as td:
                run_dir = Path(td)
                R.snapshot_candidate(run_dir)
                (run_dir / "inputs").mkdir()
                (run_dir / "inputs" / "X.cnf").write_text(TINY_SAT,
                                                          encoding="utf-8")
                j = R.Journal(run_dir)
                s = R.acquire_session(run_dir, j, cloud.cloud())
                cloud.state.fpga[1]["session"] = {
                    "session_id": "sess-other", "fpga_id": 1,
                    "owner": "other", "expires_at": _iso(time.time() + 600)}
                row = {"id": "X", "tier": "S", "family": "f",
                       "expected": "sat", "cpu_certified_kind": "sat_witness",
                       "cpu_attempt": "1"}
                with self.assertRaises(R.Blocker):
                    R.execute_attempt(run_dir, j, cloud.cloud(), s, row, 1,
                                      1, {"flag": False})

    def test_qualification_failure_releases_board(self):
        accel = FakeAccel(result=0, stack=[1], done_after_reads=2,
                          results=[0] * 8, stacks=[[]] * 8)
        with fake_plane(accel) as cloud:
            with tempfile.TemporaryDirectory() as td:
                run_dir = campaign_run_dir(td, cloud)
                with mock.patch.object(R, "API_URL", cloud.url), \
                        mock.patch.object(R, "resolve_api_key",
                                          lambda: "test-key"):
                    with self.assertRaises(R.Blocker) as ctx:
                        R.cmd_run(str(run_dir), resume=False)
                self.assertIn("qualification", str(ctx.exception))
                # the paid board was released on the acquisition error path
                self.assertEqual(len(cloud.state.release_calls), 1)
                self.assertEqual(cloud.state.fpga[1]["state"], "idle")

    def test_halt_reason_refuses_resume(self):
        with fake_plane(FakeAccel()) as cloud:
            with tempfile.TemporaryDirectory() as td:
                run_dir = campaign_run_dir(td, cloud)
                run = json.loads((run_dir / "run.json").read_text())
                run["halt_reason"] = "wrong_answer earlier"
                R.atomic_write_json(run_dir / "run.json", run)
                with mock.patch.object(R, "API_URL", cloud.url), \
                        mock.patch.object(R, "resolve_api_key",
                                          lambda: "test-key"):
                    with self.assertRaises(R.Blocker):
                        R.cmd_run(str(run_dir), resume=True)

    def test_interrupted_attempt_halts_and_releases(self):
        # qualification GOs complete; the scored solve never does, and the
        # GO watchdog (patched to 2 s) kills the worker mid-flight
        accel = FakeAccel(result=1, stack=[1],
                          results=[1, 0, 1, 1, 1, 1, 1, 1],
                          stacks=[[1, 2], [], [2], [], [1], [1], [1], [1]],
                          done_afters=[2, 2, 2, 2, 2,
                                       10 ** 9, 10 ** 9, 10 ** 9])
        real_apply = R.apply_campaign

        def short_go(campaign):
            # the run's campaign scope is re-applied on every load_run;
            # keep the test's 2 s GO watchdog in force (and in identity)
            real_apply(campaign)
            R.WD_GO = 2.0

        with fake_plane(accel) as cloud:
            with tempfile.TemporaryDirectory() as td:
                with mock.patch.object(R, "WD_GO", 2.0), \
                        mock.patch.object(R, "apply_campaign", short_go):
                    run_dir = campaign_run_dir(td, cloud)
                    with mock.patch.object(R, "API_URL", cloud.url), \
                            mock.patch.object(R, "resolve_api_key",
                                              lambda: "test-key"):
                        with self.assertRaises(R.Blocker) as ctx:
                            R.cmd_run(str(run_dir), resume=False)

                self.assertIn("interrupted", str(ctx.exception))
                run = json.loads((run_dir / "run.json").read_text())
                self.assertEqual(run["phase"], "run_blocked")
                self.assertIn("interrupted", run["halt_reason"])
                self.assertEqual(len(cloud.state.release_calls), 1)
                self.assertEqual(cloud.state.fpga[1]["state"], "idle")
                att = json.loads((run_dir / "attempts" / "TEST-1" /
                                  "repetition-001" / "attempt-001" /
                                  "attempt.json").read_text())
                self.assertEqual(att["status"], "interrupted")


# ---------------------------------------------------------------------------
# RTL worker (real amaranth simulation, frozen checkout)
# ---------------------------------------------------------------------------

class RtlWorker(unittest.TestCase):
    def run_spec(self, spec):
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            spec = {**spec, "result": os.path.join(td, "r.json")}
            p = os.path.join(td, "spec.json")
            with open(p, "w") as f:
                json.dump(spec, f)
            proc = subprocess.run(
                [sys.executable, "-B", os.path.join(HERE,
                                                    "run_pigweed_fpga.py"),
                 "--rtl-worker", p], capture_output=True, text=True,
                timeout=600, env={**os.environ,
                                  "PYTHONDONTWRITEBYTECODE": "1"})
            self.assertEqual(proc.returncode, 0, proc.stderr[-800:])
            with open(spec["result"]) as f:
                return json.load(f)

    def test_semantic_sequence_validates_against_strict(self):
        out = self.run_spec({
            "mode": "semantic", "candidate_root": PIGWEED,
            "formulas": [[2, [[1], [-1, 2]]], [2, [[1], [-1]]],
                         [2, [[1, 2], [-1, 2]]]],
            "wants": ["sat", "unsat", "sat"]})
        self.assertTrue(out["ok"], out)
        self.assertEqual([s["valid"] for s in out["sequence"]],
                         [True, True, True])

    def test_wrong_answer_fails_validation(self):
        out = self.run_spec({
            "mode": "semantic", "candidate_root": PIGWEED,
            "formulas": [[2, [[1], [-1, 2]]]], "wants": ["unsat"]})
        self.assertFalse(out["ok"])

    def test_pilot_cap_is_information_not_failure(self):
        with tempfile.TemporaryDirectory() as td:
            p = write_cnf(os.path.join(td, "p.cnf"), TINY_SAT)
            out = self.run_spec({
                "mode": "pilot", "candidate_root": PIGWEED, "cnf": p,
                "max_cycles": 1000, "poll_every": 2000, "expected": "sat"})
            self.assertTrue(out["ok"])
            self.assertTrue(out["cap_exceeded"])
            self.assertEqual(out["sim_cycle_cap"], 1000)
            self.assertNotIn("result", out)   # no answer claimed

    def test_pilot_answer_validated(self):
        with tempfile.TemporaryDirectory() as td:
            p = write_cnf(os.path.join(td, "p.cnf"), TINY_SAT)
            out = self.run_spec({
                "mode": "pilot", "candidate_root": PIGWEED, "cnf": p,
                "max_cycles": 2_000_000, "poll_every": 2000,
                "expected": "sat"})
            self.assertTrue(out["ok"])
            self.assertFalse(out["cap_exceeded"])
            self.assertEqual(out["result"], 1)

    def test_abort_pattern_and_back_to_back_solves(self):
        fx = os.path.join(PIGWEED, "SAT_test_cases")
        paths = [os.path.join(fx, "unsat", "pret150_75.dimacs")] * 3 + [
            os.path.join(fx, "sat", "aim50.dimacs"),
            os.path.join(fx, "sat", "uf20.dimacs"),
            os.path.join(fx, "unsat", "aalto_2.dimacs")]
        if not all(os.path.isfile(p) for p in paths):
            self.skipTest("qualification fixtures absent")
        out = self.run_spec({
            "mode": "abort", "candidate_root": PIGWEED, "paths": paths,
            "abort_after": [100, 6000, 40000, None, None, None],
            "expect_aborted": [True, True, True, False, False, False],
            "expected": [None, None, None, "sat", "sat", "unsat"]})
        self.assertTrue(out["ok"], json.dumps(out)[:800])
        self.assertEqual([r["aborted"] for r in out["records"]],
                         [True, True, True, False, False, False])


if __name__ == "__main__":
    unittest.main()
