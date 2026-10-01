"""Silence as an event: when a site stops speaking, and when it is back.

A site sends a report only when something is wrong, so silence alone means
both "all is well" and "the site is gone". Since 1.1.0-beta.4 the integration
also writes a heartbeat into its datalog once a day:

    {"t": "hb", "v": "<integration version>", "ha": "<HA version>", "ts": <unix time>}

Every record a site writes — a report or a heartbeat — is its signal. A site
that has sent at least one heartbeat and then gives no signal for
`SILENCE_AFTER` is reported silent; when it speaks again, it is reported back.
Sites on an integration without the heartbeat are never reported: their
silence means nothing.

Both reports are service reports: written by the connector itself, with no
datalog record, CID or archive behind them. They follow the same contract as
any report (a directory with `manifest.json` and the issue file), marked
`"source": "connector"`, so the admin layer files them as tickets of the site.
"""

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from rrs_connector.reports.manifest import (
    CONTRACT_VERSION,
    ISSUE_FILE_NAME,
    report_id,
    write_manifest,
)
from rrs_connector.reports.permissions import PRIVATE, ArtifactModes

LOGGER = logging.getLogger(__name__)

HEARTBEAT_TYPE = "hb"
# Three missed daily beats: a router reboot or a night without internet does
# not raise it.
SILENCE_AFTER = timedelta(hours=72)

SOURCE_CONNECTOR = "connector"
SITE_SILENT = "site_silent"
SITE_BACK = "site_back"
SCHEMA_VERSION = 1


def parse_heartbeat(payload: str) -> dict | None:
    """The heartbeat in a datalog record, or None when it is not one."""

    try:
        data = json.loads(payload)
    except (TypeError, ValueError):
        return None
    if not isinstance(data, dict) or data.get("t") != HEARTBEAT_TYPE:
        return None
    return data


def as_utc(value: datetime | None) -> datetime | None:
    """SQLite gives naive datetimes back; everything is stored in UTC."""

    if value is None:
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value


def hours_between(start: datetime, end: datetime) -> int:
    return int((end - start).total_seconds() // 3600)


@dataclass(frozen=True)
class SiteSignal:
    """What the connector knows of a site's last word."""

    client_id: str
    sender_address: str
    last_signal_at: datetime | None
    last_heartbeat_at: datetime | None
    last_heartbeat: dict | None
    silent_since: datetime | None


def heartbeat_details(signal: SiteSignal) -> dict:
    beat = signal.last_heartbeat or {}
    return {
        "last_heartbeat": (
            signal.last_heartbeat_at.isoformat() if signal.last_heartbeat_at else None
        ),
        "integration_version": beat.get("v"),
        "ha_version": beat.get("ha"),
    }


def silence_issue(signal: SiteSignal, now: datetime) -> dict | None:
    """The site_silent issue when the site has gone quiet; None otherwise."""

    if signal.last_heartbeat_at is None or signal.last_signal_at is None:
        return None  # never sent a heartbeat: its silence means nothing
    if signal.silent_since is not None:
        return None  # already reported
    if now - signal.last_signal_at < SILENCE_AFTER:
        return None
    hours = hours_between(signal.last_signal_at, now)
    return {
        "type": SITE_SILENT,
        "schema_version": SCHEMA_VERSION,
        "ts_start": signal.last_signal_at.isoformat(),
        "ts_end": now.isoformat(),
        "summary": f"Site silent: no signal for {hours} h",
        "details": {
            "last_signal": signal.last_signal_at.isoformat(),
            "silent_hours": hours,
            "silent_after_hours": int(SILENCE_AFTER.total_seconds() // 3600),
            **heartbeat_details(signal),
        },
    }


def back_issue(signal: SiteSignal) -> dict | None:
    """The site_back issue when a site reported silent has spoken again."""

    if signal.silent_since is None or signal.last_signal_at is None:
        return None
    if signal.last_signal_at <= signal.silent_since:
        return None
    hours = hours_between(signal.silent_since, signal.last_signal_at)
    return {
        "type": SITE_BACK,
        "schema_version": SCHEMA_VERSION,
        "ts_start": signal.silent_since.isoformat(),
        "ts_end": signal.last_signal_at.isoformat(),
        "summary": f"Site back: silent for {hours} h",
        "details": {
            "silent_since": signal.silent_since.isoformat(),
            "back_at": signal.last_signal_at.isoformat(),
            "silent_hours": hours,
            **heartbeat_details(signal),
        },
    }


def write_service_report(
    reports_dir: Path,
    client_key: str,
    sender_address: str,
    issue: dict,
    now: datetime,
    modes: ArtifactModes = PRIVATE,
) -> Path:
    """A report of the connector's own, in the same contract as any report."""

    directory = (
        reports_dir / client_key / f"service_{issue['type']}_{now:%Y%m%dT%H%M%SZ}"
    )
    directory.mkdir(parents=True, exist_ok=True)
    directory.parent.chmod(modes.dir_mode)
    directory.chmod(modes.dir_mode)
    issue_path = directory / ISSUE_FILE_NAME
    issue_path.write_text(
        json.dumps(issue, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    issue_path.chmod(modes.file_mode)
    manifest = {
        "contract_version": CONTRACT_VERSION,
        "report_id": report_id(client_key, directory),
        "client_id": client_key,
        "sender_address": sender_address,
        "source": SOURCE_CONNECTOR,
        "datalog_index": None,
        "datalog_timestamp": None,
        "cid": None,
        "processed_at": now.isoformat(),
        "archive": None,
        "decrypted_dir": None,
        "issue_file": ISSUE_FILE_NAME,
        "files": [],
    }
    write_manifest(directory, manifest, modes.file_mode)
    return directory
