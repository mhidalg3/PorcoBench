# PorcoBench

A 210-instance SAT benchmark for FPGA SAT accelerators on the Manhattan
Reasoning cloud FPGA platform. The repository contains:

- the frozen instance manifest,
- a proof-checked CPU reference (Kissat + drat-trim),
- the organizer that runs an unmodified accelerator design
  ([PigWeedSAT](https://github.com/cscaff/PigWeedSAT)) on real boards, and
- the recorded results of the first native FPGA campaign.

## Results at a glance

- **Corpus:** 210 instances in four memory tiers (S 19, S-tight 71, M-low 68,
  M-high 52), selected from the Global Benchmark Database (GBD). See
  `summary.txt` and `manifest.csv`.
- **CPU reference** (`calibration/cpu-v0`): Kissat 4.0.4 with DRAT proofs,
  1,000 s solve limit and 3,000 s check limit. 148/210 certified (77 SAT by
  witness, 71 UNSAT by proof), 0 label conflicts.
- **FPGA campaign** (`calibration/pigweed-fpga-20261014T022815Z`):
  PigWeedSAT at P=16, 50 MHz, 1,000 s host deadline, 3 repetitions.
  - 68/210 instances eligible: they fit the chip and are CPU-certified.
  - 19 solved, all checked: 17 SAT, 2 UNSAT. 23 timeouts. 26 ran out of
    learned-clause memory.
  - 0 wrong answers. Outcomes were identical across repetitions and boards,
    and cycle counts were bit-identical for every non-timeout instance.
- **Extension** (`calibration/cpu-v1-5000s`,
  `calibration/pigweed-fpga-ext20-20261022T094311Z`): the 20 chip-fitting
  instances left uncertified at 1,000 s were rerun at 5,000 s on the CPU;
  5 became certified.
  - On the FPGA (2,600 s deadline, 1 repetition), MRB0-009 was solved (SAT;
    117.1 G device cycles, 2,342 s host-observed).
  - 3 instances ran out of learned-clause memory. MRB0-001 lost its
    connection, and on re-run (`calibration/pigweed-fpga-rerun001-*`) it
    timed out at the full 2,600 s.
- **Full write-up:** `FPGA_Readiness_Plan.md` §7–8 and
  `FPGA_Extension_Uncertified20_Report.md`.

**Baseline from 2026-10-26:** PigWeedSAT `679c136` (8K clauses, 24K-word
clause and literal stores) at P=8. It loads 120 of 210 instances (S 19,
S-tight 71, M-low 30, M-high 0), about 100 of them CPU-certified. The campaigns
above used the earlier `27e4de5` at P=16. In the golden model, the new
baseline removes every learned-clause memory error on the 22 eligible
instances Kissat solves in under 10 s; it has not yet run on boards.

No competition score and no CPU-vs-FPGA speedup are claimed: the two use
different timing contracts.

## Layout

| Path | What |
|---|---|
| `manifest.csv` | The 210 instances: GBD hash, tier, family, label, size, upstream URL, exact sha256s, license status |
| `build_benchmark.py` | Rebuilds the selection from GBD metadata (`summary.txt` records the inputs) |
| `benchmark_cnf.py` | Strict DIMACS parser and witness checker used everywhere |
| `calibrate_cpu.py` | CPU reference harness (Kissat + drat-trim, fail-closed, resumable) |
| `screen_pigweed.py`, `fit_whatif.py` | Host-only loader screen and capacity what-ifs for PigWeedSAT |
| `run_pigweed_fpga.py` | FPGA organizer: `prepare`, `qualify`, `run`, `report` |
| `pigweed_fpga_worker.py` | Child process that drives one board session (load, GO, poll, readback, abort, recovery) |
| `test_*.py` | Test suites (no network, no real cloud credentials) |
| `calibration/` | Run records: identities, results, summaries, journals. Large evidence stays local (see below) |
| `scripts/fetch_corpus.py` | Downloads and verifies the CNF corpus |
| `scripts/build_tools.sh` | Builds Kissat and drat-trim at the pinned commits |
| `requirements-fpga.txt` | Pinned Python environment (SDK at a fixed git commit) |
| `*.md` | Plans, insights and campaign reports (dated; history is preserved) |

## Getting started

```bash
git clone git@github.com:mhidalg3/PorcoBench.git
# the organizer expects the candidate as a sibling directory named PigWeedSAT
git clone git@github.com:cscaff/PigWeedSAT.git
git -C PigWeedSAT checkout 679c1364fa8daf8fcc5f47348b932dd118dbea78
cd PorcoBench

# 1. corpus: download from GBD and verify every byte against manifest.csv
python3 scripts/fetch_corpus.py            # ~3 MB, 210 files into cnf/

# 2. CPU reference toolchain (only to re-run the CPU campaign)
scripts/build_tools.sh                     # builds into .tools/

# 3. Python environment for the organizer and tests (Python 3.12)
uv venv --python 3.12 .venv-fpga
uv pip install --python .venv-fpga/bin/python -r requirements-fpga.txt

# 4. tests: isolated from any real cloud credentials
XDG_CONFIG_HOME=/tmp/xdg-isol MRG_API_KEY=test-key \
  .venv-fpga/bin/python -m pytest -q test_benchmark_cnf.py \
  test_calibrate_cpu.py test_screen_pigweed.py test_run_pigweed_fpga.py
```

Commands that need more setup:

- **Rebuilding the selection** (`build_benchmark.py`) needs `gbd_tiers.csv` in
  the parent directory. Its sha256 is recorded in `summary.txt`.
- **Local PnR** in `qualify` needs Docker and the official sandbox image
  pinned by digest in `run_pigweed_fpga.py`.
- **Board runs** need a Manhattan Reasoning account (`mrg login`).

Example FPGA campaign (see `FPGA_Readiness_Plan.md` §7 for the full contract):

```bash
export MRG_SANDBOX_IMAGE=ghcr.io/manhattanreasoning/mrg-sandbox@sha256:a5c74a81bff5d3874c6a10993d99236f9aa52b2d7d01368cc7d2156f20e19034
RUN=calibration/pigweed-fpga-$(date -u +%Y%m%dT%H%M%SZ)
.venv-fpga/bin/python -B run_pigweed_fpga.py prepare --out $RUN
.venv-fpga/bin/python -B run_pigweed_fpga.py qualify --out $RUN
.venv-fpga/bin/python -B run_pigweed_fpga.py run --out $RUN      # paid board time
.venv-fpga/bin/python -B run_pigweed_fpga.py report --out $RUN
```

`prepare` also takes `--solve-timeout`, `--repetitions`,
`--only-ids-manifest` and `--cpu-override`. These are recorded in `run.json`
and the run identity. A deadline that cannot fit the platform's one-hour
session is refused.

## What is not in the repository, and why

| Kept local | Why | How to recreate |
|---|---|---|
| CNF instances (`cnf/`) | 76 of 210 have no license statement found; the rest are CC-BY-4.0 via Zenodo `10.5281/zenodo.15125952` | `scripts/fetch_corpus.py` (hash-verified) |
| DRAT proofs, CPU witnesses, solver stdout | >100 GB of proofs; witnesses are instance-derived | Re-run `calibrate_cpu.py`; each `attempt.json` records the sha256 of every evidence file |
| FPGA per-attempt folders | They embed the solver model (`raw_stack`/`model`) | Per-attempt outcomes, cycles and timings are in each run's `results.csv`; lifecycle is in `journal.jsonl` |
| Decompressed inputs, candidate copy in run dirs | Regenerated by `prepare` | `prepare` (the candidate is pinned by commit) |
| `.tools/`, `.venv-fpga/`, `.cache/` | Builds and caches | `scripts/build_tools.sh`, `requirements-fpga.txt`, `build_benchmark.py` |
| Aborted and stale run folders | Superseded or archival | Described in `FPGA_Readiness_Plan.md` |

## Data licensing

The instances come from GBD (`benchmark-database.de`). For 134 instances, the
Zenodo record `10.5281/zenodo.15125952` (SAT Competition 2002–2024 archive,
CC-BY-4.0) lists the instance by its GBD hash. The GBD-served bytes were not
compared against that archive. For 76 instances no license statement was
found. Check `license_status` in `manifest.csv` before redistributing any
instance.

## Credentials

Board runs use the account stored by `mrg login`
(`~/.config/mrg/credentials.json`) or `MRG_API_KEY`. Never commit either.
Run the tests with `XDG_CONFIG_HOME` and `MRG_API_KEY=test-key` set as shown
above, so they never touch real credentials or the real control plane.
