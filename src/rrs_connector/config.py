import re
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Annotated, Literal

import yaml
from pydantic import (
    AnyUrl,
    BaseModel,
    PositiveInt,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict
from substrateinterface.utils.ss58 import is_valid_ss58_address

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ENV_FILE = PROJECT_ROOT / ".env"

# The same slug the field engineer's repository uses for a site, so reports,
# tickets, and the site card are found by one key.
CLIENT_ID_PATTERN = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")


def validate_ss58_address(address: str) -> str:
    if not is_valid_ss58_address(address):
        raise ValueError(f"{address} is not a valid SS58 address")
    return address


class EnvSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=DEFAULT_ENV_FILE, env_file_encoding="utf-8", env_prefix="RRS_"
    )

    # Public addresses only; each seed is read from Proton Pass when a report
    # encrypted for that address is decrypted. Several recipient keys can be in
    # service at once — reports name the one they need. Order is preference.
    integrator_addresses: Annotated[list[str], NoDecode] = []
    # The single-key setting from before; merged into the list above.
    integrator_address: str | None = None
    pass_vault: str = "Report Service"
    data_dir: Path
    # True when a separate local service (the helpdesk layer) reads the
    # artifacts under its own user: artifacts open to the owning group only.
    artifact_group_readable: bool = False
    # Days to keep artifacts after processing; 0 keeps that kind forever.
    # Decrypted files are plaintext logs from a client's home, so they go
    # first; the encrypted archive can be fetched from IPFS again.
    keep_decrypted_days: int = 7
    keep_archive_days: int = 30
    state_db: Path
    poll_interval_seconds: PositiveInt
    network_config_file: Path
    senders_config_file: Path

    @field_validator("integrator_addresses", mode="before")
    @classmethod
    def split_addresses(cls, value):
        if isinstance(value, str):
            return [address.strip() for address in value.split(",") if address.strip()]
        return value

    @model_validator(mode="after")
    def collect_recipient_addresses(self) -> "EnvSettings":
        addresses = list(dict.fromkeys(self.integrator_addresses))
        if self.integrator_address and self.integrator_address not in addresses:
            addresses.append(self.integrator_address)
        if not addresses:
            raise ValueError(
                "no recipient key configured: set RRS_INTEGRATOR_ADDRESSES"
            )
        for address in addresses:
            validate_ss58_address(address)
        self.integrator_addresses = addresses
        return self


class WssConfig(BaseModel):
    polkadot: list[AnyUrl]
    kusama: list[AnyUrl]


class TimeoutsConfig(BaseModel):
    datalog_request_seconds: PositiveInt
    ipfs_download_seconds: PositiveInt


class RetriesConfig(BaseModel):
    datalog_request_max_attempts: PositiveInt
    ipfs_download_max_attempts: PositiveInt
    retry_backoff_seconds: PositiveInt


class NetworkConfig(BaseModel):
    network: Literal["polkadot", "kusama"]
    wss: WssConfig
    ipfs_gateways: list[AnyUrl]
    timeouts: TimeoutsConfig
    retries: RetriesConfig


class SenderConfig(BaseModel):
    client_id: str
    robonomics_address: str
    description: str
    enabled: bool
    # Where reading starts for a site the connector has not scanned yet. Without
    # it the first run takes only the site's latest record; with it, every
    # record from this moment on, and none before. Ignored once the site has a
    # cursor. A date means midnight UTC.
    history_from: datetime | None = None

    @field_validator("history_from", mode="before")
    @classmethod
    def date_is_midnight_utc(cls, value):
        if isinstance(value, date) and not isinstance(value, datetime):
            return datetime(value.year, value.month, value.day, tzinfo=UTC)
        return value

    @field_validator("history_from", mode="after")
    @classmethod
    def is_aware(cls, value: datetime | None) -> datetime | None:
        if value is not None and value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value

    @field_validator("client_id", mode="after")
    @classmethod
    def is_client_slug(cls, client_id: str) -> str:
        if not CLIENT_ID_PATTERN.match(client_id):
            raise ValueError(
                f"{client_id!r} is not a valid client_id: use the site slug in "
                "lowercase with dashes, for example 'qube-block-a-301'"
            )
        return client_id

    @field_validator("robonomics_address", mode="after")
    @classmethod
    def is_robonomics_address(cls, address: str) -> str:
        return validate_ss58_address(address)


class SenderRegistryConfig(BaseModel):
    senders: list[SenderConfig]


def normalize_path(path: Path) -> Path:
    if path.is_absolute():
        return path
    return (PROJECT_ROOT / path).resolve()


def load_yaml_file(path: Path) -> dict:
    with open(path, encoding="utf-8") as file:
        data = yaml.safe_load(file)

    if data is None:
        raise ValueError(f"YAML file is empty: {path}")

    if not isinstance(data, dict):
        raise ValueError(f"YAML file must contain a mapping at top level: {path}")

    return data


def load_settings() -> tuple[EnvSettings, NetworkConfig, SenderRegistryConfig]:
    env_settings = EnvSettings()
    env_settings = env_settings.model_copy(
        update={
            "data_dir": normalize_path(env_settings.data_dir),
            "state_db": normalize_path(env_settings.state_db),
            "network_config_file": normalize_path(env_settings.network_config_file),
            "senders_config_file": normalize_path(env_settings.senders_config_file),
        }
    )

    network_data = load_yaml_file(env_settings.network_config_file)
    network_config = NetworkConfig.model_validate(network_data)

    senders_data = load_yaml_file(env_settings.senders_config_file)
    sender_registry = SenderRegistryConfig.model_validate(senders_data)

    return env_settings, network_config, sender_registry
