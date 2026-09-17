import json
from pathlib import Path

import pytest
from robonomicsinterface import Account
from substrateinterface import KeypairType

from rrs_connector.config import EnvSettings, NetworkConfig

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


@pytest.fixture
def env_settings(tmp_path: Path, ha_report) -> EnvSettings:
    return EnvSettings(
        _env_file=None,
        integrator_address=ha_report["recipient_address"],
        data_dir=tmp_path / "data",
        state_db=tmp_path / "data" / "state.sqlite3",
        poll_interval_seconds=600,
        network_config_file=tmp_path / "network.yaml",
        senders_config_file=tmp_path / "senders.yaml",
    )


@pytest.fixture
def network_config() -> NetworkConfig:
    return NetworkConfig.model_validate(
        {
            "network": "polkadot",
            "wss": {
                "polkadot": ["wss://polkadot.rpc.robonomics.network/"],
                "kusama": ["wss://kusama.rpc.robonomics.network/"],
            },
            "ipfs_gateways": ["https://gateway.pinata.cloud/"],
            "timeouts": {"datalog_request_seconds": 15, "ipfs_download_seconds": 60},
            "retries": {
                "datalog_request_max_attempts": 3,
                "ipfs_download_max_attempts": 3,
                "retry_backoff_seconds": 2,
            },
        }
    )


def build_report_archive(
    path: Path,
    sender: Account,
    recipient_addresses: list[str],
    files: dict[str, str],
) -> Path:
    """A report archive in rrs-ha-integration's format, for any recipients.

    The golden fixture covers byte compatibility with the integration; this
    builds reports for key combinations the fixture cannot provide.
    """

    import secrets
    import zipfile

    from nacl.secret import SecretBox
    from substrateinterface import Keypair

    with zipfile.ZipFile(path, "w") as archive:
        for number, (name, text) in enumerate(files.items()):
            secret_key = secrets.token_bytes(32)
            payload = json.dumps({"payload": text, "meta": {"orig_file_name": name}})
            addresses = set(recipient_addresses) | {sender.get_address()}
            package = {
                "data": "0x"
                + bytes(SecretBox(secret_key).encrypt(payload.encode())).hex(),
                "keys": {
                    address: "0x"
                    + sender.keypair.encrypt_message(
                        secret_key,
                        Keypair(
                            ss58_address=address, crypto_type=KeypairType.ED25519
                        ).public_key,
                    ).hex()
                    for address in addresses
                },
            }
            archive.writestr(f"member{number}.enc", json.dumps(package))
    return path


def ed25519_account(mnemonic: str | None = None) -> Account:
    from substrateinterface import Keypair

    return Account(
        mnemonic or Keypair.generate_mnemonic(), crypto_type=KeypairType.ED25519
    )
