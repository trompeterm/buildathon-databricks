# Shopping Sessions: native Rust pipeline

A single multithreaded Rust binary that implements `rules.md` exactly and writes
`/output/user_daily/` and `/output/campaign_daily/` as Parquet.

## How it works

1. **Phase A (parallel).** Every input CSV is mmapped and split into ~4 MB line-aligned chunks.
   Worker threads parse event lines with a hand-written parser for the exact CSV dialect
   (quotes are syntax only, `\r` is data, wrong field counts and stray quotes are malformed).
   Each row is validated on the spot: F1/F2/F3 timestamps with full calendar checks, window,
   type, product, price, and the 24 h late rule. The rows are then hash-partitioned by `event_id`
   (xxh3-128). Meanwhile one thread parses `links/` and runs union-find. Each person is named
   by the byte-wise smallest ID in its group.
2. **Phase B (parallel).** Clicks are parsed, validated and routed to person partitions.
   Each `event_id` partition is deduplicated: the copy with the smallest `ingest_ts` wins,
   before validity is considered. Valid survivors are routed to person partitions.
3. **Phase C (parallel).** Each person partition is sorted by `(person, ts)` and sessionized
   (gap > 30 min). Clicks are attributed with a sliding 7-day window: the latest click wins,
   and ties go to the smallest `click_id`. Assisting campaigns are counted once per session.
   Each worker writes its own `user_daily` Parquet file. The campaign counters are merged and
   written last.

## Local testing

```sh
docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
scripts/run_fixtures.sh path/to/fixtures            # 36/36 pass
# dev set: run the image on dev/, then
uv run --with polars python scripts/check.py out dev-answer   # MATCH
```
