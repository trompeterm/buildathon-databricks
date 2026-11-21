An online shop logs page views, add-to-carts, purchases, and ad clicks as CSV files. The logs are dirty, with duplicates, malformed lines, bad timestamps, and late events. One shopper can have many user IDs, and a third file links them across long chains and shared devices.

Clean the logs in /data, join each person's IDs, build sessions, credit each session to the ad click that brought the shopper in, count assisting campaigns from the week before, and write two daily Parquet tables (/output/user_daily/ and /output/campaign_daily/), one per person and one per campaign. The rules are exact so there is one right answer, and a fast answer that is almost right scores nothing.

Your score is container run time in seconds, and lower is better. You may submit 2 times an hour on hidden practice data (about 20 million events). The final runs on about 60 million events and caps each run at 3 minutes: if any final run goes over, you get no final score, so aim for under about 60 seconds in practice.

Start locally
Download rules.md and a starter below (PySpark, DuckDB, or Polars). Any engine, any language, any amount of AI: Spark, DuckDB, Polars, Rust, C++, plain Python.

Each starter writes both tables with the right columns but no rows, so implementing the rules is up to you.

Implement the rules.md pipeline and pass the starter's 36 edge-case fixtures.
Generate the 1M-event dev set with generator.zip, unzip dev-answer.zip, and run make check-dev to check your output locally.
What to hand in
Submit a public GitHub repository with a Dockerfile at its root and the commit to score. Keep the repository at most 50 MB with no data checked in, because the dataset is mounted at run time.

Container limits
Your image must be linux/amd64 and at most 8 GB. The build has network access for up to 10 minutes; runs have no network.
Each run gets 8 pinned vCPUs (hyperthreads), 24 GB of memory, and no swap.
/data is read-only, and /scratch is writable disk for spills. /output and /scratch together may hold at most 50 GB.
The root filesystem is read-only except for /tmp (64 MB in memory), and your process runs as uid 1000.
Environment variables: DATA_DIR=/data, OUTPUT_DIR=/output, SCRATCH_DIR=/scratch, HOME=/scratch, NCPU=8, and MEM_GB=24.
At most 4,096 processes and threads, and at most 50 MB on stdout (log to stderr)