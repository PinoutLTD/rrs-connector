# rrs-connector

`rrs-connector` is a console component for collecting Robonomics Report Service
data. Its purpose is to find links to reports published by configured Home
Assistant senders in the Robonomics datalog, store processing state locally, and
prepare decrypted artifacts for subsequent transfer to a separate
administrative system.

> **Current status:** Phases 1–3 are complete and the contract with the admin
> layer is defined. `run-once` collects new datalog records for every enabled
> sender, downloads each report archive from IPFS, decrypts it with the
> integrator key read from Proton Pass, stores the decrypted files and
> processing state, and writes a `manifest.json` per report. Building the
> reader of those manifests (the Odoo helpdesk layer) is next.

## Purpose and scope

The target processing flow is as follows:

```text
.env + network.yaml + senders.yaml
                  |
                  v
        configuration loading
                  |
                  v
     sender synchronization ------> SQLite
                  |
                  v
       Robonomics datalog reading
                  |
                  v
   event classification: report / ignored
                  |
                  v
     archive download via IPFS
                  |
                  v
 secure extraction and decryption
                  |
                  v
   artifacts in data_dir + statuses in SQLite
```

The connector is responsible for event collection, local state, and file
artifacts. The web interface, report display, user management, email
distribution, and AI responses are outside the scope of this repository. A
separate admin/Odoo layer will eventually be able to read the prepared state
and artifacts through a specifically defined stable interface.

## Architecture

The code is divided into layers with explicit responsibility boundaries:

- `config.py` — loading `.env` and YAML files, validation, and path
  normalization;
- `robonomics/datalog_reader.py` — reading chain state and converting the
  response into simple Python objects; this layer does not write to the
  database or filesystem;
- `state/` — SQLAlchemy models, SQLite initialization, and storage operations;
- `reports/fetcher.py` — archive downloading from IPFS gateways, with no
  knowledge of the encryption format;
- `reports/decryptor.py` — envelope decryption and file name restoration,
  without access to Robonomics;
- `reports/manifest.py` — the `manifest.json` contract handed to the admin
  layer, built from paths and sizes only;
- `proton_pass.py` — reading the integrator seed from Proton Pass via
  `pass-cli`;
- `pipeline.py` — coordination of layers and state transitions;
- `main.py` — CLI, application startup, high-level error handling, and exit
  codes.

## Architectural decisions

### Local state

- SQLite through the SQLAlchemy ORM is used for the MVP. This is suitable for a
  small Linux host and leaves a path to PostgreSQL if the admin/Odoo layer needs
  it.
- Each public `StateStore` operation opens a short-lived session. ORM objects
  returned by the store are considered detached.
- The YAML registry is the source of truth for current metadata and the
  `enabled` flag. A sender removed from the configuration is disabled in the
  database but not deleted: its cursor and history are preserved.

### Event identity and cursor

- The datalog pallet stores records per account in a **ring buffer** of
  `WindowSize` slots (128 on the current runtime, read from the chain constant).
  It keeps the last `WindowSize - 1` records and reuses slots once full, so
  `Datalog.get_index()` may return `end < start` for a busy sender. Records are
  read by walking the ring from `start` to `end - 1` modulo `WindowSize`.
- Because slots are reused, an event is uniquely identified by
  `(sender_id, datalog_index, datalog_timestamp)`. A CID is not unique either:
  the same content can be published multiple times.
- The cursor is the timestamp of the last stored record
  (`SenderRecord.last_scanned_datalog_timestamp`); the index is kept only for
  information. A run reads the ring from the newest record backwards and stops
  at the first record older than the cursor. Records with the cursor timestamp
  itself are re-read and stored idempotently, which keeps records published in
  the same block safe.
- The cursor moves only after every record of a scan has been stored. If a
  sender fails mid-scan, its cursor stays in place and the next run re-reads
  the same records.
- If the ring no longer contains any record at or before the cursor, older
  reports were overwritten before being read; the run logs a gap warning. On a
  live sender reporting every 4 hours (sometimes twice in a row) the whole ring
  covered about 16 days, so polling must be far more frequent than that. Reading
  a full ring takes about 20 seconds, which is why scans stop at the cursor.
- The MVP cursor is stored directly in `SenderRecord`. A separate polling state
  model will only be needed when multiple chains, jobs, or independent
  consumers appear.
- On the first read of a sender without a cursor, only the latest available
  event is processed. Full historical backfill should be introduced as a
  separate explicitly enabled capability.
- Reading chain state is public, so the reader needs no keypair; the integrator
  key is only needed later for decryption.
- A payload that is a plain CID (CIDv0 `Qm…` or base32 CIDv1 `b…`) becomes a
  `NEW` report event; anything else is stored as `IGNORED` with its raw payload.
- Due to the behavior of the current `robonomics-interface` version, where the
  public `get_item(index=0)` returns the latest item, index `0` is read through
  a direct chain-storage request using the internal service API. This dependency
  must be rechecked when upgrading the library.

### Processing states

The primary event lifecycle is:

```text
NEW -> FETCHING -> FETCHED -> DECRYPTING -> PROCESSED
```

`FAILED` stores a processing error, while `IGNORED` represents an event that
was seen but is not supported. Transitioning to `PROCESSED` sets the
processing time; artifact paths are updated incrementally.

After collection, every run processes all `NEW`, `FETCHING`, `FETCHED`, and
`DECRYPTING` events, so a run interrupted midway resumes where it stopped:

- a failed download (all gateways and retries exhausted) is transient: the
  event goes back to `NEW` with a `download: …` error and is retried next run;
- an archive over the size limit, an invalid archive, or any member that cannot
  be decrypted is permanent: the event becomes `FAILED` with a `download: …` or
  `decrypt: …` error and is not retried;
- unexpected errors (for example, disk errors) keep the event `NEW`;
- if the integrator key cannot be loaded, events stay untouched and the run
  exits with code `3`; the key is not loaded when nothing is pending.

### Report artifacts

```text
<RRS_DATA_DIR>/reports/<client_id>/datalog_<index>_<timestamp_ms>/
  manifest.json        # the contract below; written last, means "ready"
  archive.zip          # encrypted archive as downloaded
  decrypted/
    issue_description.json
    home-assistant.log   # JSON Lines log of the HA integration (+ .log.1)
    trace.saved_traces
```

- The directory is unique per event, matching its identity.
- Decrypted files are logs from clients' homes: `reports/` and `decrypted/` are
  created with mode `0700` and files with `0600`. On a host where the admin
  layer runs as its own user and has to read them, set
  `RRS_ARTIFACT_GROUP_READABLE=true`: artifacts become `2750`/`0640`, open to
  the owning group and to nobody else. The setgid bit is set by the connector
  itself, so new files and directories inherit the group; the deployment only
  has to give the reports tree that shared group (`chgrp -R <group> reports`).
- Encrypted members are read from the zip in memory and never extracted, so
  archive entry names are never used as paths; output names come from the
  decrypted metadata and are reduced to their base name. Member count and size
  are limited.
- Decryption writes into a staging directory that replaces `decrypted/` only
  after every member was decrypted; downloads are written to a temporary file
  and renamed when complete.
- The format is pinned by `tests/fixtures/ha_report_v1.zip`, produced by
  rrs-ha-integration's own encryption code.

### Contract with the admin layer

A processed report is handed over as a file, not through the connector's
SQLite: the admin layer (the Odoo helpdesk layer) polls report directories and
reads `manifest.json`. The manifest is written atomically as the very last
step, so a directory that has one is complete and safe to read; a directory
without one is still being worked on, failed, or was interrupted.

```json
{
  "contract_version": 1,
  "report_id": "qube-block-a-301/datalog_94_1789114351000",
  "client_id": "qube-block-a-301",
  "sender_address": "4GRQ…",
  "datalog_index": 94,
  "datalog_timestamp": "2026-09-14T08:12:31+00:00",
  "cid": "QmWue3…",
  "processed_at": "2026-09-14T09:00:00+00:00",
  "archive": { "path": "archive.zip", "size_bytes": 3987123 },
  "decrypted_dir": "decrypted",
  "issue_file": "decrypted/issue_description.json",
  "files": [
    { "name": "home-assistant.log", "path": "decrypted/home-assistant.log", "size_bytes": 1048576 },
    { "name": "issue_description.json", "path": "decrypted/issue_description.json", "size_bytes": 2048 },
    { "name": "trace.saved_traces", "path": "decrypted/trace.saved_traces", "size_bytes": 65536 }
  ]
}
```

- `report_id` is the report's path under `reports/` and is unique: the datalog
  index alone is a reusable ring buffer slot, so the timestamp is part of it.
  A reader can use it as its own primary key.
- All paths are relative to the directory holding the manifest, so the data
  directory can be moved or mounted elsewhere.
- `issue_file` is `null` when the report carries logs only (a report sent
  without an issue). Its contents are produced by rrs-ha-integration
  (`type`, `email`, `schema_version`, `ts_start`, `ts_end`, `summary`,
  `details`) and are read by the admin layer, not interpreted here.
- `contract_version` is bumped only on incompatible changes; a reader must
  refuse versions it does not know.
- A report that is processed again (for example after a failure) gets its
  manifest rewritten in place, atomically.
- Nothing is deleted yet: retention of archives and decrypted files is still
  open, so a reader must not assume the files stay forever.

### Integrator key

- Only the public integrator address is configured (`RRS_INTEGRATOR_ADDRESS`).
  The seed is read with `pass-cli` from vault `RRS_PASS_VAULT`, item
  `Robonomics - <address>`, field `seed`, kept in memory only, and must derive
  exactly the configured address.
- Locally, an interactive `pass-cli login` session is enough. On a server, use a
  Proton Pass agent token limited to that item and log in with
  `PROTON_PASS_PERSONAL_ACCESS_TOKEN`; `PROTON_PASS_AGENT_REASON` is set
  automatically unless provided.

## Implemented so far

- Python package and CLI entry point `rrs-connector` with the `run-once`
  command;
- loading and validation of settings from `RRS_*`, network YAML, and a
  separate sender YAML;
- SS58 address validation and relative path normalization;
- creation of the local SQLite database schema through SQLAlchemy;
- models for senders, datalog events, and report artifacts;
- sender synchronization with creation, updates, and disabling without deleting
  history;
- `StateStore` operations for cursors, events, statuses, and artifact paths;
- `DatalogReader`: ring buffer traversal with the window size read from the
  chain, exact item reads, latest-only first reads, timestamp cursor, early stop
  at the cursor, gap detection, and skipping empty items;
- `run-once` collection: reading, CID classification, idempotent storage,
  cursor advancement after a complete scan, per-sender error isolation, and a
  run summary (new, ignored, already known events, senders with gaps);
- structured runtime logs and a non-zero exit code on processing errors;
- the `manifest.json` contract for the admin layer, written atomically as the
  last step of processing;
- report processing: gateway downloads with retries and size limits,
  multi-envelope decryption compatible with rrs-ha-integration, private
  artifact layout, resumable statuses, and the integrator seed from Proton Pass;
- unit tests for `StateStore`, `DatalogReader`, the fetcher, the decryptor,
  Proton Pass access, and the pipeline.

The schema changed in Phase 2 (event identity and timestamp cursor), and there
are no migrations yet: recreate any SQLite database created by an earlier
version.

## Configuration

The project requires Python `>=3.13,<4.0`. The build uses Hatchling; the main
libraries are Pydantic v2, pydantic-settings, PyYAML, SQLAlchemy,
`robonomics-interface`, and `substrate-interface`.

Environment variables (usually in a local `.env` file):

| Variable | Purpose |
| --- | --- |
| `RRS_INTEGRATOR_ADDRESS` | public SS58 address of the integrator (report recipient) |
| `RRS_PASS_VAULT` | Proton Pass vault with the integrator seed (default `Report Service`) |
| `RRS_DATA_DIR` | runtime artifact directory |
| `RRS_ARTIFACT_GROUP_READABLE` | artifacts `0750`/`0640` for a local reader service (default `false`) |
| `RRS_STATE_DB` | path to the SQLite database |
| `RRS_POLL_INTERVAL_SECONDS` | interval for the future periodic mode |
| `RRS_NETWORK_CONFIG_FILE` | path to the network YAML file |
| `RRS_SENDERS_CONFIG_FILE` | path to the sender registry YAML file |

`client_id` must be a site slug in lowercase with dashes (for example
`qube-block-a-301`) — the same key the field engineer's repository uses for the
site card, so a report, its ticket, and the card are found by one identifier.

The network endpoint, IPFS gateway, timeout, and retry format is shown in
`config/network.example.yaml`; the registry format is shown in
`config/senders.example.yaml`. Secrets should not be placed in YAML files or
committed to the repository.

## Installation

Python 3.13 and [uv](https://docs.astral.sh/uv/) are required. From the root of
the already-cloned repository, install runtime and development dependencies
from the lock file:

```bash
uv sync --extra dev
```

If only runtime dependencies are needed, without pytest and Ruff:

```bash
uv sync
```

Prepare the local configuration from the examples:

```bash
cp .env.example .env
cp config/network.example.yaml config/network.yaml
cp config/senders.example.yaml config/senders.yaml
```

Then set the integrator address in `.env`, fill in sender SS58 addresses and
metadata in `config/senders.yaml`, and change the network, endpoint, gateway,
timeout, and retry settings in `config/network.yaml` if necessary. The
configuration contains no secrets: the integrator seed is read from Proton
Pass (see "Integrator key"), so install `pass-cli` and log in first.

## Usage

The current one-time run is started in the project environment as follows:

```bash
uv run rrs-connector --command run-once
```

`--command run-once` is the default, so the following shorter invocation is
equivalent:

```bash
uv run rrs-connector
```

The command validates the configuration, creates or opens the SQLite database,
synchronizes the sender registry, collects new datalog events for every enabled
sender, downloads and decrypts pending reports, and logs a summary. It exits
with code `3` if any sender failed or the integrator key could not be loaded.

## Tests and static checks

Development dependencies must be installed with `uv sync --extra dev` for these
commands.

Run all tests:

```bash
uv run pytest
```

Run an individual test module, for example the Robonomics reader:

```bash
uv run pytest tests/test_robonomics.py
```

Check linting and formatting without changing files:

```bash
uv run ruff check .
uv run ruff format --check .
```

The pytest configuration disables automatic loading of third-party global
plugins so that results do not depend on packages installed outside the project
environment.

## Development roadmap

### Phase 1 — connector foundation (complete)

Configuration, the CLI skeleton, the SQLite model/store, and a tested isolated
Robonomics datalog reader are ready.

### Phase 2 — reliable datalog collection (complete)

`DatalogReader` is connected to `run-once`: ring buffer traversal, Unix
milliseconds converted to `datetime` at the pipeline boundary, CID
classification with `IGNORED` events keeping the raw payload, idempotent
storage by `(sender, index, timestamp)`, a timestamp cursor that moves only
after a complete scan, gap detection, and a run summary, all covered by tests.

### Phase 3 — end-to-end report processing (complete)

Archives are downloaded through the configured gateways in order (Pinata
first) with retries, backoff, and a size limit; decrypted in memory without
extraction into a per-event artifact directory with private permissions; state
transitions are resumable, with transient download failures retried and
permanent failures marked `FAILED`. The integrator seed comes from Proton Pass.
Retention of downloaded archives and decrypted files is not handled yet.

### Phase 4 — operational CLI and network resilience (planned)

- periodic mode;
- commands for viewing state and reprocessing;
- retry/failover for multiple WSS endpoints;
- applying the configured datalog request timeout and retry parameters;
- retention of report archives and decrypted files;
- a deployment option for a small Linux host (for example, systemd or a
  container).

Currently, the reader selects only the first WSS endpoint, and the request
timeout it accepts is not applied. IPFS gateway failover, timeouts, and retries
are already applied by the fetcher.

### Phase 5 — stable transfer to admin systems (contract defined)

`manifest.json` (see "Contract with the admin layer") is the supported
interface for reading processed reports. The admin/Odoo layer itself lives in
its own repository: the UI and the administrative business processes stay out
of the connector.
