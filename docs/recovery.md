# Inspecting and recovering failed records

`DEAD_LETTER_PATH` defaults to `DATA_DIR/logs-failed.json`. The active file and
rotated archives contain one JSON object per line, not a JSON array. Keep these
files on persistent local storage. A custom path outside DATA_DIR needs its own
mount and permissions. New files use mode 0600; newly created directories use 0700.
Use one application process per data directory and dead-letter path.

## Diagnose before stopping

Check `/readyz` and structured logs. Database failures show destination, event ID,
batch ID, attempt 1–4, SQLite error code and error. A recovered write is logged
with its successful attempt. Exhausted writes log `record dead-lettered` after
sync succeeds. Dead-letter failures are logged while their deliveries stay owned.
The internal counters distinguish retry attempts per record, failed database
batches, durable dead letters and failed append attempts. Gauges track records
awaiting retry or terminal handling, including records retained after cancellation.
Public metrics arrive with the monitoring milestone.

Repair disk space, permissions or the underlying database fault while the process
remains alive. Each destination retries database writes after 1s, 5s and 10s; after
exhaustion it retries dead-letter persistence at one-second intervals. Other
writers continue until shared upstream pressure reaches them. A repaired database
can succeed on a scheduled retry. After exhaustion, that batch stays on its
terminal path even if the database subsequently recovers.

If a partial append cannot be repaired, new bytes stay blocked until truncate and
sync succeed. Rotation failures similarly block appends while metadata work is
retried. Neither path settles undurable records to make shutdown look successful.
Avoid restarting a process holding volatile work: the memory queues are not
recoverable after process loss. A deadline-expired shutdown reports pending raw
and delivery counts and exits unsuccessfully. Some pending deliveries can already
have durable effects but remain unsettled.

## Inspect complete entries

Use a quiesced copy of the files, or stop ingestion and wait for a successful
drain before inspection. Preserve the active file, all rotated `.json` files and
any `.partial` files. Do not edit active files while the application owns them.
Each complete entry includes:

- `id`, `batch_id` and `destination` for identity and routing.
- `original`, preserving the original JSON value and unknown fields.
- `event`, preserving producer fields, enrichment and the destination.
- `attempts`, `last_error` and `failed_at` for failure history.

For an offline copy, this standard-library example prints only recovery metadata:

```sh
python3 - /path/to/copied/logs-failed.json <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    for number, line in enumerate(source, 1):
        record = json.loads(line)
        print(number, record["id"], record["destination"],
              record["attempts"], record["last_error"], record["failed_at"])
PY
```

Rotated names contain a UTC timestamp and UUID. A file rotates only after a
complete line takes its size above 10,000,000 bytes, so an archive can be larger
than that threshold. No retention deletion is automatic.

At startup, a final fragment without a newline is saved to a uniquely named
`.partial` file and synced, including its directory entry, before the active file
is truncated and synced. A failed preservation step fails startup without
truncating the original tail. Inspect partial fragments manually; they are not
complete replay input. Previously terminated rotation attempts may also leave
empty reservation files; preserve and inspect them rather than assuming they
contain events.

## Requirements for a controlled replay

This repository does not yet provide an automated replay tool. Before any replay,
fix the original failure, back up the destination databases using SQLite's backup
facilities, and agree on an independently reviewed operator procedure.

That procedure must select complete JSON Lines, verify the outer ID/destination
against the saved event, and retain the **same event ID, destination and enriched
fields**. Write to the original destination using the eight-column schema and a
transaction with prepared SQL. The existing `ON CONFLICT(id) DO NOTHING` behavior
makes repeat insertion into that database harmless. Do not reroute against newly
edited producer mappings or generate fresh IDs. Deduplication does not span
different destination databases.

Deduplicate copied dead-letter entries by destination and ID before replay. A
crash between sync and settlement, or an uncertain rotation result followed by a
retry, can leave repeated lines. Verify the rows after the replay transaction;
retain the original artifacts until the recovery has been reviewed.

Posting a saved entry to `/logs` is not same-ID replay: the endpoint creates new
IDs and receive metadata. Copying a live SQLite file without accounting for its
WAL is also not a reliable backup.
