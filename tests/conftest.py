import json
from pathlib import Path

import pytest
from robonomicsinterface import Account
from substrateinterface import KeypairType

FIXTURES_DIR = Path(__file__).parent / "fixtures"
# Encrypted by rrs-ha-integration's own code with throwaway seeds from its tests.
HA_REPORT_ARCHIVE = FIXTURES_DIR / "ha_report_v1.zip"
HA_REPORT = json.loads((FIXTURES_DIR / "ha_report_v1.json").read_text("utf-8"))


@pytest.fixture(scope="session")
def ha_report() -> dict:
    return HA_REPORT


@pytest.fixture(scope="session")
def ha_report_archive() -> Path:
    return HA_REPORT_ARCHIVE


@pytest.fixture(scope="session")
def recipient_account() -> Account:
    return Account(HA_REPORT["recipient_seed"], crypto_type=KeypairType.ED25519)


@pytest.fixture(scope="session")
def sender_address() -> str:
    return HA_REPORT["sender_address"]
