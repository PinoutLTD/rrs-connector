# Simplification audit

October 2026, at `a828e1c`. Read-only pass over `src/` hunting over-engineering
only: what to delete, simplify, or replace with the standard library. Nothing
here is applied yet; pick items by number.

The connector is lean on the whole: little inherited code, most modules shaped
by one task. `watchdog`, `permissions` and `manifest` are left alone.
SQLAlchemy and pydantic-settings stay: replacing them is a rewrite, not a cut.

## Findings, biggest cut first

1. **delete** four `StateStore` methods only tests call:
   `get_sender_record_by_address`, `get_datalog_entry_record`,
   `get_datalog_entry_record_by_id`, `get_report_artifact_record`; and the
   `raw_dir` parameter of `upsert_report_artifact`, which nobody passes (the
   column stays). Not used by `rrs-admin`, the bridge or `anastasis` either
   (~60 lines of code, as many of tests). [state/store.py]
2. **shrink** the repeated shape in `StateStore`: `with session` →
   `session.get` → `if None: raise ValueError` → fields → `commit`, six times.
   One `_update(Model, id, **fields)` serves `mark_sender_scanned`,
   `mark_silent`, `mark_datalog_entry_status` and part of `record_signal`
   (~40 lines). [state/store.py]
3. **reuse** three copies of "SQLite returns a naive datetime → UTC":
   `watchdog.as_utc`, inside `pipeline.datetime_to_ms`, and
   `retention.older_than`. Keep `as_utc` (~8 lines).
   [watchdog.py, pipeline.py:131, reports/retention.py:47]
4. **reuse** archive validation written twice, in
   `recipients.archive_recipients` and `decryptor._decrypt_members`: open the
   zip, not empty, member count, member size. One `open_archive(path)` in
   `decryptor` (~15 lines). [reports/recipients.py, reports/decryptor.py]
5. **reuse** `fetch.fetch_report` repeats the middle of
   `pipeline.process_report` without the status marks: mkdir and chmod,
   download unless present, choose the key, decrypt. Factor the common part and
   call it from both (~20 lines). [fetch.py:105, pipeline.py:280]
6. **delete** the single-key `integrator_address` setting and its merge into
   the list. The server sets only `RRS_INTEGRATOR_ADDRESSES` (~8 lines).
   [config.py:43,69]
7. **delete** `poll_interval_seconds`. Runs are driven by the systemd timer;
   the field is only logged, yet required. The server's `.env` still sets it and
   pydantic-settings rejects unknown `.env` keys by default, so remove the line
   on the server first, then the field (~5 lines plus README).
   [config.py:55, main.py:64]
8. **shrink** `multi_envelope_decrypt_data`: it only turns `EnvelopeError` into
   `ReportDecryptionError`, and `_decrypt_members` already has that `except`
   next to the call. Its docstring points at the integration's
   `utils/encrypt_tools.py`, which the integration's audit proposes to delete
   (~10 lines). [reports/decryptor.py:38]
9. **yagni** the one-method Protocols `DatalogItems`, `DatalogClient`,
   `DatalogSource`, there for test fakes; duck typing does it without them
   (~20 lines). [robonomics/datalog_reader.py:35-45, pipeline.py:77]
10. **yagni** `network: Literal["polkadot"]` and the `wss.polkadot` nesting: one
    network is left, `wss: list[AnyUrl]` is enough. Changes `network.yaml` on
    the server, so only together with another config change (~5 lines).
    [config.py:84-101]
11. **shrink** `RetentionResult.freed_bytes` and `directory_size`: the whole
    tree is walked before each `rmtree` for one number in the log. Counters are
    enough (~10 lines). [reports/retention.py]
12. **shrink** small things: the one-element `TRANSIENT_ERRORS` tuple →
    `except TransportError`; `raise AssertionError("unreachable")` in
    `with_retries`; one-line `make_sqlite_url` and `create_session_factory`,
    one caller each (~12 lines). [robonomics/retry.py, state/db.py]
13. **native** (optional) `requests` serves one streaming GET in `fetcher`;
    `urllib.request` with a timeout does it with a few more lines. Small gain:
    only in passing. [reports/fetcher.py, pyproject.toml]

net: about −220 lines, −1 dependency possible.

## Suggested grouping

Items 1–5 and 8 fit one PR with no change on the server. Items 6, 7 and 10 go
together with editing the server's `.env` and `network.yaml`.
