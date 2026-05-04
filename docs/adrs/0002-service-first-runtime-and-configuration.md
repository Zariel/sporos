# ADR 0002: Service-first runtime and configuration

## Status

Proposed

## Date

2026-05-04

## Context

Sporos is intended to be useful as a long-running cross-seeding application. The
current shape still looks like a mixture of standalone workflow CLI commands and
a daemon. That makes the operational model less clear:

- some workflows can run as one-off commands while the daemon has its own
  scheduler and API behavior;
- runtime state is currently discovered through an application directory rather
  than an explicit service configuration contract;
- `CONFIG_DIR` currently acts as a broad base for config, database, cache, logs,
  and generated output defaults;
- Kubernetes users need the binary to behave like a normal service even though
  the project does not intend to ship Kubernetes manifests or a Helm chart.

The core problem is production ownership. Today a user can think of Sporos as a
set of commands to run, a scheduler to start, and API endpoints to call. That
pushes orchestration choices to cron jobs, wrapper scripts, webhook callers, and
deployment manifests. A Kubernetes-first application should instead have one
clear production process that owns the ongoing cross-seeding lifecycle.

First-party Kubernetes support means the binary works naturally as a container
workload: it runs in the foreground, receives configuration predictably, writes
logs to stderr/stdout, responds to standard health probes, exposes metrics, shuts
down on SIGTERM, and keeps durable state in configured paths. Those operational
surfaces support the service model. They are not the main product goal. The main
goal is that users run Sporos as one long-running application, not as a mixture
of CLI workflows, cron, and a daemon mode.

The project should therefore move away from "CLI plus daemon plus implicit app
directory" as the primary mental model and toward "service as the production
runtime, with optional administrative commands".

## Decision

Make Sporos a service-first application. The long-running service command is
`sporos serve`, and it is the primary production runtime.

The service should be the owner of cross-seeding orchestration. It should run in
the foreground and own scheduling, announce/webhook/API intake, runtime workers,
local state, graceful shutdown, and the operational surfaces needed to run it.
ADR 0001 defines the durable announce inbox and asynchronous retry model that
feeds this service runtime.

One-off CLI commands should be limited to administration, diagnostics,
migration, validation, and explicit maintenance tasks. They should not be the
main way production workflows are orchestrated, and users should not need cron
or wrapper scripts to make normal search, RSS, injection, cleanup, or retry
behavior happen.

Configuration should be loaded from an explicit config file path and optional
prefixed environment variable overrides.

The config file path should locate the configuration, not implicitly define all
runtime state. The config file should contain the paths needed to run the
service, such as database/state paths, cache paths, output paths, inject paths,
torrent directories, data directories, and link directories. Defaults may still
exist, but they must be deliberate and documented by the schema rather than
emerging from a broad `CONFIG_DIR` convention.

`database_path` should be an explicit configuration field. When omitted, it
should default from `state_dir`, for example `state_dir/sporos.db`. This keeps
database placement predictable while still giving deployments one simple state
directory default.

Environment variables should use a project prefix, for example `SPOROS__`, and
should cover simple scalar settings where that is operationally useful. Complex
collection configuration such as Torznab indexer tables may remain config-file
only until the project defines a safe and readable environment representation.

## North Star

A production deployment should be able to run Sporos as one service process:

```text
sporos serve --config /etc/sporos/config.toml
```

or the equivalent environment-driven form:

```text
SPOROS__CONFIG_FILE=/etc/sporos/config.toml sporos serve
```

After startup, that process should own ongoing work. Users should not need to
cron separate CLI commands, run separate "search" and "rss" jobs, or rely on an
external caller to retry transient workflow states that the service can manage
itself.

The desired production shape is:

- one container process is the normal way to run Sporos;
- one explicit config file, plus namespaced scalar environment overrides, defines
  how that process runs;
- the service owns scheduler ticks, HTTP intake, background workers, local state,
  and graceful shutdown;
- external systems submit events or requests, but they do not orchestrate the
  workflow;
- local prerequisites controlled by the pod fail fast when invalid;
- remote dependency outages degrade service capability without causing startup
  crash loops;
- administrative commands exist to inspect, repair, migrate, or validate the
  service, not to replace the service loop.

For Kubernetes, the desired behavior is:

- the process runs in the foreground as PID 1 or behind a minimal init;
- SIGTERM starts graceful shutdown;
- logs go to stderr/stdout in configured text or JSON format;
- no runtime log files are required;
- readiness and liveness endpoints are stable and unauthenticated;
- Prometheus metrics are exposed through the service HTTP listener;
- the service can start with temporarily unavailable remote endpoints and report
  degraded state instead of crash-looping;
- invalid config, unavailable configured filesystems, and invalid state paths
  still fail fast;
- runtime work is bounded, durable where needed, and observable;
- a single-writer/single-replica assumption is explicit unless a future design
  adds leader election or distributed locking.

## Scope

In scope:

- making the long-running service the primary runtime mode;
- defining service startup, shutdown, config, state, and runtime ownership
  around Kubernetes-friendly process semantics;
- defining the minimum operational surfaces required to run and troubleshoot
  that service in Kubernetes: logs, probes, status, metrics, and degraded
  dependency reporting;
- replacing implicit app-directory discovery with an explicit config file path
  and explicit runtime paths;
- introducing prefixed environment variable overrides for appropriate scalar
  settings;
- retaining maintenance and diagnostic CLI commands where they serve the
  service model.

Out of scope:

- shipping Kubernetes manifests, Helm charts, operators, or Docker Compose
  files;
- supporting every structured config item as environment variables immediately;
- removing all existing CLI commands in one change;
- adding multi-replica coordination before the single-writer model is replaced;
- defining durable announce inbox storage, retry, expiry, dedupe, or API
  semantics; those belong to ADR 0001;
- changing conservative matching or torrent-client side effects as part of the
  runtime shape change.

## Runtime Model

The service should own these runtime responsibilities:

- HTTP API intake for announces, webhooks, job control, health, status, and
  metrics;
- scheduled RSS, search, inject, cleanup, and maintenance work;
- durable announce work processing as defined by ADR 0001;
- coordination of runtime workers, bounded queues, local filesystem work,
  torrent-client side effects, and database access;
- lifecycle ownership: startup validation, readiness, graceful shutdown, worker
  cancellation, database closure, and durable state handoff;
- service-wide reporting for retry and circuit-breaker state used by transient
  dependencies;
- status and degraded-state reporting.

There should be one production orchestrator: the service runtime. Scheduler
ticks, HTTP intake, and administrative API actions should feed that runtime
rather than each workflow having an equally valid standalone production path.

One-off commands should be treated as administrative tools. Examples include:

- generate or validate config;
- print or reset API keys;
- inspect torrent trees or diffs;
- run explicit repair or migration tasks;
- perform offline diagnostics.

Production workflow commands such as search, RSS, inject, and restore should be
evaluated against the service model before `0.1`. If kept, they should share the
same configuration, validation, logging, and runtime boundaries as the service,
and their role should be clearly administrative rather than the normal
deployment shape.

## Configuration Model

Configuration should have a clear precedence order:

1. built-in defaults;
2. explicit config file;
3. prefixed environment variable overrides;
4. command-line flags for command-specific administrative behavior.

The config file path should be provided by:

- `--config <path>` where supported;
- `SPOROS__CONFIG_FILE=<path>` for service deployments;
- a sensible user default only for local interactive use.

`CONFIG_DIR` should not remain the primary configuration contract. If retained,
it should be treated as compatibility input or a container convenience alias
with a defined migration path.

The config schema should distinguish these concepts:

- config file location;
- state directory;
- database path, defaulting to `state_dir/sporos.db`;
- torrent cache directory;
- output directory;
- inject directory;
- torrent directory;
- data directories;
- link directories;
- HTTP listener host and port, set separately;
- logging format and level;
- metrics and health behavior;
- scheduler cadences and workflow policy.

This separation matters in Kubernetes because a config file may be mounted
read-only from a ConfigMap or Secret while state and cache need writable
persistent volumes.

## Environment Variables

Environment variables should be prefixed, explicit, and predictable. The
project prefix uses a double underscore separator: `SPOROS__`.

Good candidates include:

- `SPOROS__CONFIG_FILE`;
- `SPOROS__LOG_LEVEL`;
- `SPOROS__LOG_FORMAT`;
- `SPOROS__LISTEN_HOST`;
- `SPOROS__LISTEN_PORT`;
- `SPOROS__METRICS_ENABLED`;
- `SPOROS__API_KEY`, subject to secret-handling policy;
- scheduler cadence overrides where scalar values are clear.

Poor initial candidates include complex lists or tables that become hard to
read, validate, or redact when flattened into environment variables. Torznab
indexers, Arr instances, torrent-client arrays, and notification URL lists need
a deliberate representation before being supported through env vars.

The environment model should not make secrets easier to leak. Logging and error
messages must redact configured secret values regardless of whether they came
from a file or environment.

## HTTP Listener

The service HTTP listener should have separate host and port settings.

The default listen host should be `0.0.0.0`, which is suitable for
containerized service operation. The default listen port should be `9000`.

Configuration and environment override names should keep host and port separate,
for example:

- `listen_host`, overridden by `SPOROS__LISTEN_HOST`;
- `listen_port`, overridden by `SPOROS__LISTEN_PORT`.

This keeps common deployment choices simple: a user can change the port without
changing bind address behavior, or bind to a narrower address without changing
service port.

## Operational Visibility

Observability is a supporting requirement for the service-first model. Once
Sporos owns ongoing work, operators need enough visibility to understand what the
service is doing and why it is or is not making progress. This ADR does not make
monitoring the goal; it requires monitoring because a Kubernetes service without
useful visibility is not operable.

ADR 0001 owns the durable announce inbox and asynchronous retry model. ADR 0002
owns the service-level surfaces that make the long-running process observable,
including the places where ADR 0001 queue state appears once that queue exists.

The current implementation already has useful primitives:

- stderr logging with text or JSON formatting and configurable level;
- unauthenticated `/_health/livez` and `/_health/readyz`;
- `/metrics` using a Prometheus client crate;
- metrics for total HTTP requests, runtime queue lifecycle counters, queue
  capacity, scheduler job state, job failures, successful job last-run time, and
  indexer status;
- startup validation that logs remote dependency failures and continues;
- fail-fast validation for configured local filesystem paths.

Those primitives are not yet enough for a service that owns production
orchestration. The ADR 0002 gap is to make the service debuggable as a service,
without moving the announce inbox design from ADR 0001 into this ADR.

Required supporting visibility changes:

- standardize logging configuration under the `SPOROS__` environment namespace
  while keeping logs on stderr/stdout and avoiding runtime log files;
- add request lifecycle logging with method, route, status, latency, and
  redacted request context;
- define stable probe semantics: liveness reports whether the process should be
  restarted, while readiness reports local service ability to accept and make
  progress on work;
- make readiness depend on local prerequisites and runtime state, such as
  database access, configured filesystem paths, intake state, scheduler state,
  and worker cancellation state;
- report remote dependency degradation in readiness detail without making every
  tracker, indexer, notification, Arr, or torrent-client outage turn into
  `not_ready`;
- replace the authenticated `/api/status` `OK` response with structured status
  JSON covering version, config file path, state and database paths, listener,
  readiness checks, scheduler jobs, runtime queues, degraded dependencies, and
  recent service errors;
- extend metrics with bounded-label HTTP request counts, status codes, latency,
  service uptime/build information, queue depth and in-flight gauges, job
  outcomes and durations, dependency failures, retry-after or cooldown state,
  and local database/filesystem readiness;
- ensure Prometheus output uses stable metric names, bounded cardinality, and
  the correct exposition content type;
- use tracing spans or equivalent stable context for service operations that
  cross HTTP, scheduler, database, filesystem, indexer, notification, and
  torrent-client boundaries, without logging secrets.

ADR 0001 queue observability should plug into these same surfaces. ADR 0001
defines the durable announce fields, retry classification, dedupe, expiry,
`202 Accepted` API direction, and circuit-breaker scheduling behavior. ADR 0002
requires that the resulting backlog, oldest age, attempts, outcomes, retry
delays, and breaker/degraded state are visible through logs, status, and
metrics once ADR 0001 is implemented.

## Kubernetes-Native Behavior

The service should be Kubernetes-native by behavior:

- startup fails fast for local prerequisites the pod controls, such as invalid
  config, invalid paths, missing writable state directories, and schema errors;
- startup does not fail just because remote trackers, indexers, notification
  endpoints, Arr instances, or torrent clients are temporarily unavailable;
- degraded remote dependency state is logged, exposed through status and
  metrics, and included in readiness detail without automatically making the
  service unready;
- liveness reports whether the process should be restarted;
- readiness reports whether the service can accept and make useful progress on
  work;
- graceful shutdown stops intake, drains or safely pauses work, persists durable
  state, closes database pools, and exits before the pod grace period expires;
- the service can run as a non-root user with explicitly configured writable
  paths.

This ADR does not require Sporos to provide deployment manifests. It requires
the binary to have a stable operational contract that manifests can target.

## Rationale

The service-first model makes reliability decisions local to Sporos instead of
spreading them across cron jobs, webhook callers, IRC automation, and shell
wrappers.

It also makes Kubernetes support concrete. Kubernetes works best with processes
that have one clear role, explicit configuration, visible health, stdout/stderr
logs, and predictable shutdown. Those expectations should shape the binary even
when users bring their own manifests.

Explicit config paths and prefixed env overrides avoid overloading a single
directory concept. A mounted config file, writable database, torrent cache,
output directory, and data/link mounts are different operational resources and
should be modeled separately.

## Incremental Plan

1. Add an explicit config file path option and `SPOROS__CONFIG_FILE`.
2. Split config-file discovery from runtime state/cache/output path defaults.
3. Add explicit `state_dir` and `database_path` config fields, with
   `database_path` defaulting to `state_dir/sporos.db`.
4. Introduce `SPOROS__` environment overrides for simple scalar settings.
5. Add `sporos serve` and make it the primary documented runtime.
6. Align existing workflow commands with the same service runtime boundaries or
   reclassify them as administrative tools.
7. Remove runtime log-file assumptions and log to stderr/stdout.
8. Finalize supporting operational visibility: log configuration, probe
   semantics, diagnostic status JSON, metrics, degraded dependency state, and
   tracing context.
9. Align SIGTERM, degraded startup, and local prerequisite failure policy with
   the service contract.
10. Integrate ADR 0001 durable announce queue state into the service status,
    health detail, logs, and metrics when ADR 0001 lands.
11. Decide what compatibility behavior remains before `0.1`.

## Consequences

Positive consequences:

- deployments have one clear long-running process to run and observe;
- Kubernetes support is based on stable binary behavior rather than bundled
  manifests;
- config can be mounted read-only while state and cache use writable volumes;
- environment overrides become predictable and namespaced;
- production workflows move into durable, observable service orchestration.

Costs and risks:

- existing CLI workflow habits may need migration;
- config discovery and defaults need careful compatibility handling;
- path defaults must be explicit enough to avoid surprising data placement;
- environment variable support can become unwieldy if complex structures are
  flattened too early;
- a service-first model increases the importance of durable queues, health,
  metrics, and shutdown correctness.

## Alternatives Considered

### Keep CLI and daemon as equal runtime modes

This preserves flexibility but keeps the production model ambiguous. It also
pushes scheduling, retries, and orchestration decisions to users.

### Keep `CONFIG_DIR` as the central app directory

This is simple for local use but conflates configuration, state, cache, logs,
and output. It fits poorly with read-only config mounts and separate persistent
volumes.

### Provide Kubernetes manifests as the main support story

Manifests can help users, but they do not make the binary Kubernetes-native by
themselves. The runtime contract should be correct first.

## Open Questions

- Which existing workflow commands remain for `0.1`, and which become service
  API actions only?
- What defaults should be used when `SPOROS__CONFIG_FILE` is not set?
- Which complex config structures, if any, should be representable through
  environment variables after `0.1`?
