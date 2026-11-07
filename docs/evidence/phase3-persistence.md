# Memory-delivery persistence verification

Verified on 7 November 2026 with Go 1.26.0 on Linux/amd64. The application pins
modernc.org/sqlite v1.61.0 and its required modernc.org/libc v1.79.0;
`SELECT sqlite_version()` reports **3.54.1**. No CGO is required for the application.

## Reproducible checks

```sh
make check build race test-launch
go mod verify
docker build --target runtime -t sylloge:local .
```

All passed. `make vuln` was not run and remains optional/manual.

| Behavior | Committed test evidence |
| --- | --- |
| HTTP → raw channel → enrichment → configured route → each database | `TestHTTPToAllSQLiteDestinationsAndFinalDrain` |
| Unknown producers use the configured default | Same application test, with service3 selected as the default |
| Low-volume and full batches reach real SQLite | `TestLiveHTTPPersistsLowVolumeAndFullBatches` |
| Exactly 100 records or one second from the first pending record | `TestWriterFlushesAt100AndOneSecondFromFirstRecord`, using Go virtual time |
| Final partial batches flush before successful shutdown | Both the writer flush test and live three-destination application test |
| Eight columns, UTC timestamps, microsecond enrichment and schema version | `TestSQLiteCreatesRequiredSchemaAndCommitsEnrichedRow` |
| Stable IDs deduplicate without overwriting original rows | `TestSQLiteDeduplicatesIDsButRollsBackOtherConstraints` |
| Duplicate deliveries settle after a real commit | `TestWriterSettlesDuplicateDeliveriesAfterRealSQLiteCommit` |
| Other constraint errors roll back the entire batch | SQLite constraint test above |
| Lock contention respects cancellation and the connection remains usable | `TestSQLiteBusyAttemptHonorsCancellationAndCanResume` |
| Write-attempt deadline is two seconds | `TestWriterBoundsEachStorageAttempt`, using virtual time |
| Failed writes retain custody and bound downstream intake | `TestWriterFailureRetainsCustodyAndBackpressuresBoundedQueue` |
| Real SQLite failure makes readiness fail, saturates bounded memory, and reports undurable work on shutdown | `TestFailedDestinationBecomesUnreadyAndBackpressuresWithoutDiscarding` |
| Unwritable data directories fail startup | `TestUnwritableStorageFailsBeforeListening`; runs as a non-root user |
| Development-file values and injected storage settings obey precedence | `TestDevelopmentStorageEnvironmentPrecedence` |

The live failure test configures raw capacity 2, dispatch batch size 1 and memory
queue capacity 1. After the writer retains one failed event, at most five events
are accepted: one held by the writer, one queued, one held by dispatch, and two
in raw_logs. Further requests receive 503. No accepted delivery is settled on
the failed transaction. The shutdown deadline expires with explicit pending counts.

## Container checks

An isolated smoke run built the development and runtime images, started temporary
containers/volumes, sent HTTP requests, stopped with SIGTERM, copied the closed
database files out, and inspected their rows independently with Python's SQLite
client. Its temporary containers and volumes were removed afterward.

- Development: `.env` port and memory capacity, injected APP_ENV precedence,
  Go 1.26.0, 202 ingestion into all three files, and successful final drain.
- Runtime: UID/GID 65532 writes to the image's `/data` mounted as a new named
  volume; explicit and default routes produce the expected rows.
- A second runtime container using the same volume preserves prior rows and
  appends another event, verifying container replacement does not erase stored data.
- A read-only mount exits unsuccessfully before listening.
- Neither image contains the private planning files or development `.env`.

The README's runtime volume command and sample request reproduce the basic
persistence check. Inspect files after a graceful stop, or use SQLite's backup
interface for live snapshots; the WAL is part of active database state.

These checks establish the memory-delivery milestone. They do not demonstrate
RabbitMQ recovery, retries, dead-letter persistence, crash-proof admission or
the later sustained 1,000 logs/second load target.
