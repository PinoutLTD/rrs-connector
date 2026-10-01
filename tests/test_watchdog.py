"""Silence as an event: heartbeats, the 72 h watch, and coming back."""

import json
import sqlite3
from datetime import timedelta

import pytest
from test_pipeline import (
    ADDRESS_1,
    ADDRESS_2,
    CID_1,
    TIMESTAMP_1,
    FakeReader,
    open_store,
    registry_for,
    unavailable_download,
)

from rrs_connector.pipeline import ms_to_datetime, run_once
from rrs_connector.state.db import create_db_engine, initialize_database
from rrs_connector.watchdog import (
    SILENCE_AFTER,
    SITE_BACK,
    SITE_SILENT,
    parse_heartbeat,
)

BEAT = '{"t":"hb","v":"1.1.0-beta.8","ha":"2026.9.3","ts":1780065423}'
T0 = ms_to_datetime(TIMESTAMP_1)
HOUR_MS = 3600 * 1000


@pytest.fixture
def reader() -> FakeReader:
    return FakeReader()


@pytest.fixture
def one_site():
    return registry_for(("home-a", ADDRESS_1))


def service_reports(env_settings, kind: str | None = None) -> list[dict]:
    found = []
    for manifest_path in sorted(
        (env_settings.data_dir / "reports").glob("*/service_*/manifest.json")
    ):
        manifest = json.loads(manifest_path.read_text())
        issue = json.loads((manifest_path.parent / manifest["issue_file"]).read_text())
        if kind is None or issue["type"] == kind:
            found.append({"manifest": manifest, "issue": issue})
    return found


@pytest.fixture
def run_now(env_settings, network_config, reader, recipient_account, one_site):
    def run_now(now, registry=None):
        return run_once(
            env_settings,
            network_config,
            registry or one_site,
            reader,
            load_account=lambda address: recipient_account,
            download=unavailable_download,
            now=now,
        )

    return run_now


def test_heartbeat_is_recognised_and_other_text_is_not():
    assert parse_heartbeat(BEAT)["v"] == "1.1.0-beta.8"
    assert parse_heartbeat(CID_1) is None
    assert parse_heartbeat('{"archive": "not a heartbeat"}') is None
    assert parse_heartbeat("") is None


def test_the_last_signal_and_heartbeat_are_kept(run_now, env_settings, reader):
    reader.publish(ADDRESS_1, 0, TIMESTAMP_1, BEAT)
    run_now(T0 + timedelta(minutes=5))
    reader.publish(ADDRESS_1, 1, TIMESTAMP_1 + HOUR_MS, CID_1)
    run_now(T0 + timedelta(hours=2))

    sender = open_store(env_settings).get_sender_record_by_address(ADDRESS_1)
    assert sender.last_signal_at.replace(tzinfo=None) == (
        T0 + timedelta(hours=1)
    ).replace(tzinfo=None)
    assert sender.last_heartbeat_at.replace(tzinfo=None) == T0.replace(tzinfo=None)
    assert json.loads(sender.last_heartbeat_payload)["ha"] == "2026.9.3"


def test_a_site_without_heartbeats_is_never_called_silent(
    run_now, env_settings, reader
):
    reader.publish(ADDRESS_1, 0, TIMESTAMP_1, CID_1)

    result = run_now(T0 + timedelta(days=30))

    assert result.sites_silent == 0
    assert service_reports(env_settings) == []


def test_silence_past_72_hours_is_reported_once(run_now, env_settings, reader):
    reader.publish(ADDRESS_1, 0, TIMESTAMP_1, BEAT)
    assert run_now(T0 + timedelta(hours=1)).sites_silent == 0
    assert run_now(T0 + SILENCE_AFTER - timedelta(minutes=1)).sites_silent == 0

    result = run_now(T0 + SILENCE_AFTER + timedelta(hours=3))

    assert result.sites_silent == 1
    [report] = service_reports(env_settings, SITE_SILENT)
    manifest, issue = report["manifest"], report["issue"]
    assert manifest["source"] == "connector"
    assert manifest["cid"] is None and manifest["files"] == []
    assert manifest["client_id"] == "home-a"
    assert manifest["sender_address"] == ADDRESS_1
    assert issue["summary"] == "Site silent: no signal for 75 h"
    assert issue["details"]["integration_version"] == "1.1.0-beta.8"

    # The next runs do not repeat it.
    assert run_now(T0 + timedelta(days=5)).sites_silent == 0
    assert len(service_reports(env_settings, SITE_SILENT)) == 1


def test_a_silent_site_that_speaks_again_is_reported_back(
    run_now, env_settings, reader
):
    reader.publish(ADDRESS_1, 0, TIMESTAMP_1, BEAT)
    run_now(T0 + timedelta(hours=1))
    run_now(T0 + timedelta(hours=80))
    reader.publish(ADDRESS_1, 1, TIMESTAMP_1 + 90 * HOUR_MS, BEAT)

    result = run_now(T0 + timedelta(hours=91))

    assert result.sites_back == 1
    [report] = service_reports(env_settings, SITE_BACK)
    assert report["issue"]["summary"] == "Site back: silent for 90 h"
    sender = open_store(env_settings).get_sender_record_by_address(ADDRESS_1)
    assert sender.silent_since is None
    # Silent again later: a new report.
    assert run_now(T0 + timedelta(hours=90 + 73)).sites_silent == 1


def test_an_unreachable_node_does_not_make_a_site_silent(run_now, env_settings, reader):
    reader.publish(ADDRESS_1, 0, TIMESTAMP_1, BEAT)
    run_now(T0 + timedelta(hours=1))
    reader.failing_addresses.add(ADDRESS_1)

    result = run_now(T0 + timedelta(days=10))

    assert result.failed == 1 and result.sites_silent == 0
    assert service_reports(env_settings) == []


def test_one_silent_site_does_not_touch_another(run_now, env_settings, reader):
    both = registry_for(("home-a", ADDRESS_1), ("home-b", ADDRESS_2))
    reader.publish(ADDRESS_1, 0, TIMESTAMP_1, BEAT)
    reader.publish(ADDRESS_2, 0, TIMESTAMP_1, BEAT)
    run_now(T0 + timedelta(hours=1), both)
    reader.publish(ADDRESS_2, 1, TIMESTAMP_1 + 70 * HOUR_MS, BEAT)

    result = run_now(T0 + timedelta(hours=75), both)

    assert result.sites_silent == 1
    [report] = service_reports(env_settings)
    assert report["manifest"]["client_id"] == "home-a"


def test_records_stored_before_tracking_still_count(run_now, env_settings, reader):
    reader.publish(ADDRESS_1, 0, TIMESTAMP_1, BEAT)
    run_now(T0 + timedelta(hours=1))
    # As a database from before this version: the records, no signal columns.
    with sqlite3.connect(env_settings.state_db) as connection:
        connection.execute(
            "UPDATE senders SET last_signal_at = NULL, last_heartbeat_at = NULL, "
            "last_heartbeat_payload = NULL"
        )

    result = run_now(T0 + timedelta(hours=80))

    assert result.sites_silent == 1
    sender = open_store(env_settings).get_sender_record_by_address(ADDRESS_1)
    assert sender.last_heartbeat_payload == BEAT


def test_an_old_database_gets_the_new_columns(env_settings):
    env_settings.state_db.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(env_settings.state_db) as connection:
        connection.execute(
            "CREATE TABLE senders (id INTEGER PRIMARY KEY, client_id VARCHAR, "
            "robonomics_address VARCHAR NOT NULL UNIQUE, description TEXT, "
            "enabled BOOLEAN NOT NULL, last_scanned_datalog_timestamp DATETIME, "
            "last_scanned_datalog_index INTEGER, last_scanned_at DATETIME, "
            "created_at DATETIME NOT NULL, updated_at DATETIME NOT NULL)"
        )

    initialize_database(create_db_engine(env_settings.state_db))

    with sqlite3.connect(env_settings.state_db) as connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(senders)")}
    assert {
        "last_signal_at",
        "last_heartbeat_at",
        "last_heartbeat_payload",
        "silent_since",
    } <= columns


def test_a_heartbeat_before_the_cursor_record_is_found(run_now, env_settings, reader):
    # The live case on deploy: the record at the cursor is a report, the last
    # heartbeat came before it, and signals were not tracked yet.
    reader.publish(ADDRESS_1, 0, TIMESTAMP_1, BEAT)
    run_now(T0 + timedelta(hours=1))
    reader.publish(ADDRESS_1, 1, TIMESTAMP_1 + 2 * HOUR_MS, CID_1)
    run_now(T0 + timedelta(hours=3))
    with sqlite3.connect(env_settings.state_db) as connection:
        connection.execute(
            "UPDATE senders SET last_signal_at = NULL, last_heartbeat_at = NULL, "
            "last_heartbeat_payload = NULL"
        )

    run_now(T0 + timedelta(hours=4))

    sender = open_store(env_settings).get_sender_record_by_address(ADDRESS_1)
    assert sender.last_heartbeat_payload == BEAT
    assert sender.last_signal_at.replace(tzinfo=None) == (
        T0 + timedelta(hours=2)
    ).replace(tzinfo=None)
