# rrs-connector

`rrs-connector` is a console component for collecting Robonomics Report Service
data. Its purpose is to find links to reports published by configured Home
Assistant senders in the Robonomics datalog, store processing state locally, and
prepare decrypted artifacts for subsequent transfer to a separate
administrative system.

> **Current status:** Phase 1 (connector foundation) is complete, and Phase 2
> (reliable datalog collection) is in progress. The `run-once` command already
> loads the configuration, initializes SQLite, and synchronizes senders, but it
> does not yet call the ready-to-use `DatalogReader`, classify events, or store
> them. Report downloading and decryption have not been implemented yet either.

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
- `reports/fetcher.py` — future archive downloading, with no knowledge of the
  encryption format;
- `reports/decryptor.py` — future secure extraction, envelope decryption, and
  file name restoration, without access to Robonomics;
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

- An event is uniquely identified by the pair `(sender_id, datalog_index)`. A
  CID is not unique: the same content can be published multiple times.
- The MVP cursor is stored directly in `SenderRecord`. A separate polling state
  model will only be needed when multiple chains, jobs, or independent
  consumers appear.
- On the first read of a sender without a cursor, only the latest available
  event is processed. Full historical backfill should be introduced as a
  separate explicitly enabled capability.
- `Datalog.get_index()` returns the exclusive upper bound `end`, so the last
  index is `end - 1`.
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
was seen but is not supported. Retry policy and cursor advancement on errors
must be defined in the pipeline. Transitioning to `PROCESSED` sets the
processing time; artifact paths are updated incrementally.

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
- `DatalogReader`: index ranges, exact item reads, latest-only first reads,
  continuation after a cursor, and skipping empty items;
- structured runtime logs and a non-zero exit code on processing errors;
- unit tests for `StateStore` and `DatalogReader` (they were not run as part of
  this documentation update).

At present, `run-once` only loads the configuration, creates the database,
synchronizes the sender registry, and iterates over enabled senders while
logging. The existence of read and storage methods does not yet mean that
end-to-end collection is complete.

## Configuration

The project requires Python `>=3.13,<4.0`. The build uses Hatchling; the main
libraries are Pydantic v2, pydantic-settings, PyYAML, SQLAlchemy,
`robonomics-interface`, and `substrate-interface`.

Environment variables (usually in a local `.env` file):

| Variable | Purpose |
| --- | --- |
| `RRS_INTEGRATOR_SEED` | integrator seed for the Robonomics account |
| `RRS_DATA_DIR` | runtime artifact directory |
| `RRS_STATE_DB` | path to the SQLite database |
| `RRS_POLL_INTERVAL_SECONDS` | interval for the future periodic mode |
| `RRS_NETWORK_CONFIG_FILE` | path to the network YAML file |
| `RRS_SENDERS_CONFIG_FILE` | path to the sender registry YAML file |

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

Then set the integrator seed in `.env`, fill in sender SS58 addresses and
metadata in `config/senders.yaml`, and change the network, endpoint, gateway,
timeout, and retry settings in `config/network.yaml` if necessary. `.env`
contains a secret and must not be added to version control.

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

At the current stage, the command validates the configuration, creates or opens
the SQLite database, synchronizes the sender registry, and logs the iteration
over enabled senders. It does not yet perform end-to-end report downloading
and processing.

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

### Phase 2 — reliable datalog collection (in progress)

Remaining tasks:

- connect `DatalogReader` to `run-once` for each enabled sender;
- convert Unix milliseconds to `datetime` at the adapter/pipeline boundary;
- recognize supported report payloads/CIDs and store all other events with
  status `IGNORED`, preserving the raw payload;
- store new events with uniqueness by sender/index and advance the cursor with
  explicitly defined error behavior;
- produce meaningful final run statistics and cover the orchestration pipeline
  with tests.

### Phase 3 — end-to-end report processing (planned)

Before implementation, the artifact directory layout (CID or sender/datalog
index) and gateway policy must be defined. Then:

- implement archive downloading from IPFS;
- extract archives securely, preventing path traversal and other unsafe paths;
- decrypt files using the integrator key and restore names from metadata;
- store archive/raw/decrypted/meta paths and all state transitions;
- implement retries or recovery after `FAILED` and provide a useful console
  summary.

### Phase 4 — operational CLI and network resilience (planned)

- periodic mode;
- commands for viewing state and reprocessing;
- retry/failover for multiple WSS endpoints and IPFS gateways;
- actual application of configured timeout/retry parameters;
- a deployment option for a small Linux host (for example, systemd or a
  container).

Currently, the reader selects only the first WSS endpoint, and the request
timeout it accepts is not applied. Gateway selection/failover will be added
along with the fetcher.

### Phase 5 — stable transfer to admin systems (planned)

After the artifacts and Phase 3 model have stabilized, a supported contract for
reading processed reports by a separate admin/Odoo layer must be defined,
without moving the UI and administrative business processes into the connector.
