# Shopping Sessions pipeline

Entry for the Builders Buildathon **Shopping Sessions** track: turn a dirty click-stream into the exact `user_daily` and `campaign_daily` Parquet tables defined in [rules.md](rules.md), as fast as possible.

## Approach

A single native Rust binary (`src/`). It uses no query engine, because engine CSV readers don't follow this dialect and engine startup costs too much.

1. **Map the inputs.** Every CSV is memory-mapped and cut into newline-aligned chunks for 8 worker threads.
2. **Scan fields with SIMD.** AVX2 finds every `,`, `\n` and `"` in 64-byte blocks. Fields come from those positions, and quote and field-count rules are checked exactly.
3. **Linked IDs.** Union-find on 64-bit ID hashes. Each person is named by its byte-wise smallest ID.
4. **Parse events and clicks.** Timestamps (F1/F2/F3), prices and validation follow `rules.md` to the letter. ID lookups are batched and prefetched.
5. **Dedup.** Events are hash-partitioned by `event_id`. The copy with the smallest `ingest_ts` wins, and is only then validated.
6. **Sessionize and attribute.** Records are hash-partitioned by person, then sorted. The pipeline builds sessions, credits each one to the latest qualifying click, and counts assists with merged 7-day intervals per campaign.
7. **Write Parquet.** A small hand-written writer (PLAIN, uncompressed).

## Run locally

```sh
cargo build --release
DATA_DIR=path/to/data OUTPUT_DIR=out target/release/pipeline
python3 scripts/run_fixtures.py fixtures -- target/release/pipeline   # 36 edge cases
python3 scripts/compare.py out path/to/dev-answer                      # exact diff
```

Set `PIPE_VERBOSE=1` for per-phase timings on stderr. The dataset generator is in `generator/` (`cargo build --release`).
