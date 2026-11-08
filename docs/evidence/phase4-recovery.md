# Retry and dead-letter verification

Verified on 8 November 2026 using Go 1.26.0 on Linux/amd64. No runtime dependencies
or toolchain versions changed in this milestone.

```sh
make check build race test-launch
go mod verify
docker build --target runtime -t sylloge:local .
```

All passed. Vulnerability scanning remains optional/manual and was not run.

| Behavior | Test evidence |
| --- | --- |
| Recovery on retries 1–3, after 1s/5s/10s, without changing IDs or the batch | `TestWriterRetriesAfterOneFiveAndTenSecondsWithoutChangingIdentity` |
| Actual SQLite failures at all four attempts for three independent writers, then one shared synced terminal file | `TestRealSQLiteRetriesAndThreeDestinationDeadLetters` |
| No settlement on a failed terminal append; retry and healthy recovery | `TestWriterExhaustionSettlesOnlyAfterSyncedDeadLetters` |
| Cancellation/resumption preserves attempts and skips already durable records | `TestWriterResumesTerminalHandlingWithoutRepeatingHandledRecords` |
| Original JSON, enrichment, identity, destination and failure metadata | `TestDeadLetterAppendsCompleteRecoverableRecords` |
| Three concurrent producers cannot interleave JSON lines | `TestThreeWritersShareOneSerializedDeadLetterFile` |
| Exact 10,000,000-byte boundary; rotation only after a complete crossing line; archives retained | `TestDeadLetterRotatesOnlyAfterExceedingTenMillionBytes` |
| Partial writes, short writes and sync failures restore the complete prefix | `TestDeadLetterRepairsPartialShortAndUnsyncedWrites` |
| Failed repair blocks new bytes, then resumes after the filesystem recovers | `TestDeadLetterRepairFailureHaltsNewWritesWhileFaultPersists`, `TestDeadLetterRetriesRepairBeforeAcceptingNewRecords` |
| Rotation rename, directory-sync and reopen failures resume before further appends | `TestDeadLetterRotationFailuresResumeBeforeFurtherAppends` |
| Startup preserves an incomplete tail before truncation; preservation failure leaves the original intact | `TestDeadLetterPreservesIncompleteTailBeforeStartupRepair`, `TestDeadLetterTailPreservationFailureDoesNotTruncateOriginal` |
| A writer waiting for the shared file can cancel while another sync is blocked | `TestDeadLetterWaitingForAnotherWriterHonorsCancellation` |
| HTTP readiness recovers after a real database retry; unresolved failures still bound admission | `TestReadinessRecoversAfterSuccessfulDatabaseRetry`, `TestFailedDestinationBecomesUnreadyAndBackpressuresWithoutDiscarding` |
| Invalid paths fail startup; database, WAL, shared-memory and linked aliases cannot be dead-letter targets | `TestInvalidDeadLetterPathFailsBeforeListening`, `TestDeadLetterPathCannotOverwriteSQLiteDatabase` |
| DATA_DIR-derived paths, explicit injection and empty-value rejection work through Compose | `TestDevelopmentDeadLetterEnvironmentPrecedence` |

Virtual-time tests establish cumulative retry times of 0s, 1s, 6s and 16s. The
real-time integration test uses actual SQLite constraint failures and three
writers in parallel. It observes the 1s/5s/10s inter-attempt intervals with scheduler
tolerance, twelve failed database attempts, nine per-record retries and three
durable terminal entries. Pending deliveries reach zero only after synced handling.

The filesystem fault tests wrap real local files. They inject syscall failures
and inspect the resulting artifacts rather than treating a mocked success as
durability evidence. File writes, sync and metadata operations remain synchronous;
contexts bound waits and retry delays, not a stuck operating-system syscall.

## Container smoke evidence

Isolated development and CGO-disabled runtime checks passed:

- `.env` inputs and injected environment values retain their precedence.
- HTTP 202 requests persist into all three databases; final batches drain on SIGTERM.
- The runtime runs as UID/GID 65532 and keeps rows in a named volume across
  container replacement; a read-only volume fails before HTTP starts.
- A stopped runtime volume was seeded with a complete JSON line and a truncated
  final entry, using the same non-root UID. On restart, the truncated bytes were
  preserved exactly in a `.partial` file and the active file kept only its complete
  prefix before `/readyz` became healthy.
- Development `.env` and private planning files are absent from the images.

Only the smoke run's temporary containers and volumes were removed afterward.
The README and [recovery guide](../recovery.md) describe normal startup, storage
mounts, offline inspection and the requirements for a separately reviewed replay.

This evidence covers write recovery on memory delivery. It does not establish
RabbitMQ custody/redelivery, crash-proof HTTP admission, an automated replay tool,
or the later sustained 1,000 logs/second target.
