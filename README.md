# Sylloge

A Go log ingestion engine under construction. Startup configuration, health checks
and graceful shutdown are connected. Validation, bounded admission, enrichment
and routing are implemented behind an injectable HTTP/pipeline seam. A custom
per-IP token bucket limits ingestion requests. Storage is not connected: the
running application's `POST /logs` returns `503` (or `429` when rate limited)
and accepts no records.

## Requirements

- Go **1.26.0**, matching go.mod, local tests, CI and the container build.
- GNU Make for command shortcuts.
- Docker Engine and Docker Compose supporting profiles and `--env-file` for development.
- A C compiler for `go test -race`; application builds currently have no external dependencies.

## Development

Copy `.env.example` to `.env` once, preserving any existing local settings:

```sh
cp -n .env.example .env
make dev
```

Compose builds the Go 1.26.0 development image and injects the listed settings.
Existing shell variables override `.env`; absent settings use defaults. Explicitly
empty values are rejected by the application. `.env` is ignored and excluded from
Docker contexts. It is not executed as a shell script or mounted into the container.
Use Compose's key/value and quoting syntax; do not use shell command substitution.

```sh
curl http://localhost:3000/healthz
PORT=8080 make dev
make dev-down
```

The host port follows PORT and is bound to loopback for development. If you change
PORT when starting the app, use the same override when stopping it. Development
defaults APP_ENV to `development`. There is no automatic file watching; rebuild
with `make dev` after changing code. No RabbitMQ container is needed at this milestone.

## Direct execution and production

The binary reads the process environment once into a typed configuration. It never
searches for `.env`, including when APP_ENV is development. For local direct runs,
prepare variables yourself; `make run` uses the environment already supplied.
Production injects variables through the deployment platform, using the same loader:

```sh
make build
PORT=8080 APP_ENV=production ROUTING_CONFIG_FILE=configs/routing.example.json ./bin/sylloge
```

The routing path is relative to the process working directory unless absolute.
The example file is nonsecret; customize it for your producers. Configuration is
validated before listening, including destinations and JSON structure. Routing is
not yet used to process events. The HTTP server listens on all container/host
interfaces; choose network exposure in the deployment configuration.

Build a runtime image with `docker build --target runtime -t sylloge:local .`.
It runs as a non-root user and contains the compiled binary, certificates and
example routing configuration, without the Go toolchain or development environment
file. Runtime variables must be injected when starting it.

| Variable | Default | Validation |
| --- | --- | --- |
| PORT | 3000 | Integer, 1–65535 |
| APP_ENV | production | Nonempty label; does not select a configuration file |
| ROUTING_CONFIG_FILE | configs/routing.example.json | Existing routing JSON file |
| HTTP_READ_HEADER_TIMEOUT | 5s | Positive Go duration |
| HTTP_READ_TIMEOUT | 10s | Positive Go duration |
| HTTP_WRITE_TIMEOUT | 10s | Positive Go duration |
| HTTP_IDLE_TIMEOUT | 60s | Positive Go duration |
| HTTP_MAX_HEADER_BYTES | 16384 | Integer, 1–1048576 |
| SHUTDOWN_TIMEOUT | 60s | Positive Go duration |
| MAX_ACTIVE_REQUESTS | 128 | Positive integer; concurrent ingestion-handler bound |
| RATE_LIMIT | 1000 | Positive integer; requests per second per client IP |
| RATE_LIMIT_MAX_IPS | 10000 | Positive integer; maximum retained client-IP buckets |
| RATE_LIMIT_IDLE_TIMEOUT | 60s | Go duration, at least 1s; idle bucket expiry |
| TRUSTED_PROXIES | empty | Comma-separated proxy CIDRs; empty trusts socket identity only |
| RAW_LOGS_CAPACITY | 10000 | Positive integer; individual records in the raw channel |
| INGEST_BATCH_SIZE | 50 | Positive integer; drain up to this many available records |
| ADMISSION_TIMEOUT | 100ms | Positive Go duration, at most 100ms for the whole valid subset |

Pipeline settings are loaded and validated now, ready for the persistence wiring.
The injectable handler accepts a bounded pipeline as its Admitter; the application
leaves that dependency unset until a real downstream destination is connected.

No credentials are required yet. Further broker/storage settings will be added
when used. Future credentials must come from the environment, with local-only
values in ignored development files and no production defaults. Startup errors
identify invalid scalar settings without echoing their values. The application
does not log its full configuration.

## HTTP and lifecycle

| Endpoint | Current behavior |
| --- | --- |
| GET/HEAD /healthz | `200`, `{"status":"ok"}` for GET; process liveness only |
| POST /logs | `503`, `{"error":"ingestion unavailable"}`; `429` if rate limited; accepts no records |
| Other methods on these routes | `405` with Allow header |

SIGINT or SIGTERM stops listening and drains active HTTP requests under a fresh
shutdown deadline. A second signal can force termination. Startup/listener errors
and failed graceful shutdown exit unsuccessfully; a timed-out shutdown closes
remaining connections. Structured JSON operational logs go to stderr.

## Checks

```sh
make check           # formatting, vet, unit tests and live listener/command tests
make build
make race
make test-launch     # real Compose file/env precedence checks; no daemon needed
make vuln            # optional manual vulnerability scan; needs network access
```

Make disables automatic toolchain switching and checks the exact Go version.
The tests exercise production without `.env`, real TCP startup and SIGTERM, bad
configuration/exit status, HTTP status contracts, and routing-file validation.
Compose launch tests use isolated fixture files and avoid printing rendered input
values. CI runs formatting, vet, build, tests, race detection, and Compose checks.
`make vuln` remains an optional manual command and downloads only a separately
versioned tool; it does not add a runtime dependency to the application.

Current verification: build, vet, unit/subprocess tests, race detection, and Compose
precedence checks pass with Go 1.26.0. The previous manual vulnerability scan
reported 23 reachable standard-library vulnerabilities against this initial
patch version. Its reported fixes extend through Go 1.26.10. The scan is outside
required CI while the Go patch-update policy is agreed. Running `make vuln` still
reports findings and exits unsuccessfully; they are not suppressed or fixed by
this CI change.

## Ingestion foundation

`internal/api/response.go` centralizes JSON success/error encoding, Content-Type,
status codes and response-write failures. It encodes before committing headers
and preserves the endpoint-specific JSON contracts.

Tests exercise the handler with the real bounded pipeline and a capturing dispatch
function. Valid entries enter together or none do; a failed admission returns
`503` with `{"error":"ingestion overloaded"}` within one 100ms wait budget.
Receipt metadata is applied after dequeue, followed by configured routing.
Failed or canceled dispatch retains pending events and stable IDs for resumption.
No production discard sink is installed.

The handler contract supports JSON arrays up to 1,000,000 bytes. A fully valid
batch returns `202` with `status` and `batchId`. Mixed batches add accepted/rejected
counts and zero-based field errors. All-invalid arrays return `422`; malformed
JSON, non-array roots and empty arrays return `400`; unsupported media types or
encodings return `415`; oversized bodies return `413`. These acceptance responses
are available through the injected handler and do not yet enable production
ingestion. Timestamps use Go's RFC 3339 parsing; lengths count Unicode code points.

Client IPs come from the socket unless its peer is a configured trusted proxy.
Forwarded addresses are walked from the trusted end, stopping at the first
untrusted hop. The custom token bucket charges one token per POST /logs attempt,
including invalid or overloaded requests, before reading its body. Each IP starts
with RATE_LIMIT tokens and refills to that capacity once per elapsed second;
tokens do not accumulate beyond capacity. This discrete reset permits bursts
around a refill boundary. Rejections return JSON `429` with a rounded-up
Retry-After delay. Health checks are unaffected.

IP state is bounded and idle entries are reclaimed lazily, without a background
goroutine. A full state map rejects new IPs with `429` rather than evicting live
budgets. Rate limiting and source metadata use the same trusted-IP resolver.
The pipeline exposes accepted/rejected/overload/rate-limit/default-route and
dispatch counters internally; public metrics and dashboard work remain separate.

Enrichment measured approximately 510 ns/event (32 B, one allocation) on an AMD
Ryzen 5 4500U with Go 1.26.0, below the 1ms target for this isolated operation:

```sh
go test ./internal/ingest -run '^$' -bench BenchmarkEnrichment -benchmem
```
