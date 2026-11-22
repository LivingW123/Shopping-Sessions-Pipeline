# Handoff: Windows machine ↔ Ubuntu machine

Two Claude Code sessions work on this repo at the same time:

- **Windows** (the original session) owns the pipeline's CPU work.
- **Ubuntu** owns everything that needs real Linux or Docker.

Read this whole file before starting.

## Goal and rules

- **Target: beat 0.4293 s**, the board's fastest (scorer shopping-sessions-5). The score is container run time on hidden data, start to exit, median of 3 when under 20 s.
- **Data sizes:**
  - Practice: about 20M events.
  - Final: about 60M events, with a 3-minute cap per run.
- **Exact output or nothing.** An almost-right answer scores zero.
- **Every build must pass the correctness gate** at the bottom of this file: 36/36 fixtures and dev `MATCH`.
- **Deadline:** the track closes Sunday 11:00 AM Eastern.
- **Submissions are the user's job**, at most 2 per hour. Neither machine submits. When a build is ready, report its commit SHA and Linux container timings (dev, s20, s60, median of 3) to the user.
- **Linux timing is authoritative.** The score comes from a Linux container, so Windows timings are only for relative comparisons.
- **The Ubuntu machine owns the Dockerfile and all timing work**: tasks 1, 4 and 5 below. It also does the container timing of every candidate build the Windows machine pushes to `handoff/linux-split`.

## Status at handoff

**Correctness: no known bugs.** Every check below passes exactly:
- All 36 fixtures, run natively and inside the real container (GitHub Actions).
- The 1M-event dev set: exact `MATCH` against `dev-answer/`, the track page's `dev-answer.zip`.
- An independent pure-Python reference (`scripts/reference.py`), written straight from `rules.md`, matches the dev answer. It also matches the Rust pipeline on seeds 2, 3, 4, 5, 6, 11 and 249 (300k events each). Seed 249's window contains 2028-02-29.

**Speed: well short of the target.** The board's best is **0.4458 s** on the hidden ~20M-event practice set (8 vCPU Xeon).

Our 20M-event set is seed 7, `--events 20000000 --clicks 5000000`. On a GitHub Actions runner (4 vCPU AMD EPYC) the container wall time is about **2.45–2.7 s**:

| Phase | Container | Native |
| --- | --- | --- |
| links | 0.11 s | 0.11 s |
| parse | 1.17 s | 1.22 s |
| dedup | 0.30 s | 0.31 s |
| sessionize | 0.28 s | 0.28 s |
| **write** | **0.40–0.45 s** | **0.06 s** |

Container start and exit add about 0.2 s more.

**Known hot spots:**
- `hash64`'s tail `copy_from_slice` becomes a libc `memcpy` call. With its hash arithmetic, that's about 20% of parse. Windows is fixing it next.
- In the container, the phase between `campaign_daily written` and `written` takes about 0.4 s. The `File::write_all` calls inside it take only about 0.03 s, and natively the whole phase takes 0.05 s. Cause unknown: **this is Ubuntu task 2.**

**Uncommitted at handoff:** nothing. Everything is on branch `handoff/linux-split`. That branch is `main` plus one commit: the SWAR fast path for F3 timestamps in `src/util.rs`, with tests, plus this file.

## File ownership (to avoid edit conflicts)

| Owner | Files |
| --- | --- |
| **Windows** (do not edit) | `src/main.rs`, `src/util.rs` |
| **Ubuntu** (yours) | `Dockerfile`, `.dockerignore`, `.github/workflows/*`, `scripts/*`, `bench/*` (new), `src/mem.rs`, `src/parquet.rs` |
| Shared, append-only | `HANDOFF.md`: add notes under "Ubuntu → Windows notes" at the bottom |

If a fix needs a change in `main.rs` or `util.rs`, don't make it. Describe it, as a diff or precise prose, in `bench/RESULTS.md` or the notes section, and Windows will apply it.

**Branches.** Ubuntu works on `linux/bench`, branched from `handoff/linux-split`. Windows keeps working on `handoff/linux-split` and merges both into `main` when a version is verified. Neither machine pushes to `main` directly without telling the user. Pull often, since both sides push.

## Setup (Ubuntu, once)

```sh
git fetch origin && git checkout -b linux/bench origin/handoff/linux-split
# toolchains: docker (with buildx), Rust ≥ 1.85 (rustup), python3 + polars + pyarrow, linux-tools (perf)
pip install polars pyarrow
cargo build --release                                   # pipeline, native
(cd generator/generator && cargo build --release)       # dataset generator
G=generator/generator/target/release/generate
$G --seed 1 --events 1000000  --clicks 250000   --output data/dev     # public dev set
$G --seed 7 --events 20000000 --clicks 5000000  --output data/s20     # ~2 GB, practice-sized
$G --seed 9 --events 60000000 --clicks 15000000 --output data/s60     # ~6 GB, final-sized
```

`dev-answer/` is not in git. Download `dev-answer.zip` from the track page and unzip it to `dev-answer/`, so that `dev-answer/user_daily.csv` exists. `data/`, `out*/` and `dev-answer/` are gitignored. **Never commit data**: the repo must stay under 50 MB.

## Tasks for Ubuntu

### 1. Baseline in the real container (do first)

```sh
docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
python3 scripts/run_fixtures.py fixtures -- scripts/docker_run.sh                 # expect 36/36
rm -rf out && scripts/docker_run.sh data/dev out && python3 scripts/compare.py out dev-answer   # expect MATCH
for i in 1 2 3; do rm -rf out; /usr/bin/time -f "%e s" scripts/docker_run.sh data/s20 out; done
```

`docker_run.sh` uses the scorer's flags: 8 CPUs, 24 GB, read-only root, uid 1000, no network. Pass `CPUS=` and `MEM=` if the machine is smaller. For per-phase timings, add `-e PIPE_VERBOSE=1` to the `docker run` line in a local copy of the script, or run the binary natively with `PIPE_VERBOSE=1`.

**Measure and report** in `bench/RESULTS.md`:
- The machine: `lscpu` model, vCPUs, RAM.
- Wall time for dev, s20 and s60, median of 3.
- The per-phase verbose lines.
- Peak memory for s60, from `docker stats` or the cgroup's `memory.peak`. It must stay well under 24 GB.

### 2. Why is the write phase 8× slower in the container?

Natively, the step from `campaign_daily written` to `written` takes about 0.05 s; in Docker it takes about 0.4 s. The file writes themselves take about 0.03 s. The rest of that step builds column vectors in `main.rs`, the `write_ud` closure. User-id bytes are fetched from the mmapped input with `files.get(loc)`, and the Parquet byte buffer is built in `parquet::write`. Those buffers are 4 MB or more, so they come from the huge-page `mmap` path in `src/mem.rs`.

Hypotheses to test, one at a time, comparing native and Docker on s20:
- **Huge pages under the memory cgroup.** Huge-page faults or zeroing behave differently inside the cgroup. Test: in `src/mem.rs`, raise `BIG` to effectively infinite, so everything goes to `System`, and re-time.
- **Page-cache charging.** Faults on the input mmap behave differently inside the container. Test with `perf stat -e page-faults,major-faults`, and `perf record` the container process, either with `--privileged` or via `perf record -p` from the host.
- **Docker flags.** Rerun with `--memory` removed, then `--cpus` removed, and see which flag causes it.

Deliverable: the root cause plus a fix. If the fix is in `mem.rs` or `parquet.rs`, make it. If it's in `main.rs`, describe it.

### 3. Real hardware counters

GitHub's VMs report `<not supported>` for cycles and instructions. On the Ubuntu box:

```sh
perf stat -e cycles,instructions,branch-misses,cache-misses,L1-dcache-load-misses,dTLB-load-misses,page-faults \
  env NCPU=8 DATA_DIR=data/s20 OUTPUT_DIR=/tmp/o target/release/pipeline
CARGO_PROFILE_RELEASE_DEBUG=line-tables-only cargo build --release
perf record -F 2000 -g env NCPU=8 DATA_DIR=data/s20 OUTPUT_DIR=/tmp/o target/release/pipeline
perf report --no-children --sort sym,srcline | head -80
```

Report IPC, the miss rates, and the top 40 source lines in `bench/RESULTS.md`. Parse costs about 630 cycles per line today, so the question is whether that's instructions or stalls.

### 4. Container and runtime knobs (no source changes in `main.rs`)

Measure each separately on s20, in the container, median of 3:
- **Thread count.** `NCPU` is read by the binary, so try `-e NCPU=4`, `8`, `12` and `16` while keeping `--cpus 8`.
- **Runtime image.** `debian:bookworm-slim` vs `gcr.io/distroless/cc-debian12` vs `scratch` with a static binary (`x86_64-unknown-linux-musl`). The last one only if it isn't slower: musl's malloc matters less here because big buffers go through `mmap` in `mem.rs`, but measure it. Report container start-to-exit on an empty run as well.
- **CPU target.** `RUSTFLAGS` `-C target-cpu=x86-64-v3` (current) vs `x86-64-v4`. The scorer is Cascade Lake or newer, so it has AVX-512. Never use `native`: the scorer builds on its own machine, but this must not depend on that.

Change `Dockerfile` only if a variant is clearly faster *and* still passes `run_fixtures` and dev `MATCH` in the container.

### 5. Final-size safety (60M)

Run s60 in the container with the scorer's flags three times. Wall time must be far under the 3-minute cap, and the run must not be killed by OOM. Report time and peak memory.

## Correctness gate (both machines, before any merge to main)

```sh
python3 scripts/run_fixtures.py fixtures -- target/release/pipeline        # 36/36
rm -rf out && DATA_DIR=data/dev OUTPUT_DIR=out target/release/pipeline && python3 scripts/compare.py out dev-answer   # MATCH
# optional, slower: cross-check a fresh seed against the independent reference
G=generator/generator/target/release/generate; $G --seed 12 --events 300000 --clicks 75000 --output data/x12
rm -rf out-x out-xr && DATA_DIR=data/x12 OUTPUT_DIR=out-x target/release/pipeline && python3 scripts/reference.py data/x12 out-xr && python3 scripts/compare.py out-x out-xr
```

## Ubuntu → Windows notes

(append below)

## Windows → Ubuntu notes

- Each time Windows pushes a candidate to `handoff/linux-split`, its commit message says "candidate". Please time it in the container (s20 median of 3, plus dev MATCH and fixtures in the container) and append the numbers to `bench/RESULTS.md` on `linux/bench`.
