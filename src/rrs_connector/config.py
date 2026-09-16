import re
from pathlib import Path
from typing import Literal

import yaml
from pydantic import (
    AnyUrl,
    BaseModel,
    PositiveInt,
    field_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict
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

    # Public address only; its seed is read from Proton Pass when decrypting.
    integrator_address: str
    pass_vault: str = "Report Service"
    data_dir: Path
    state_db: Path
    poll_interval_seconds: PositiveInt
    network_config_file: Path
    senders_config_file: Path

    @field_validator("integrator_address", mode="after")
    @classmethod
    def is_integrator_address(cls, address: str) -> str:
        return validate_ss58_address(address)


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
