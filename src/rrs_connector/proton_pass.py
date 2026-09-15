"""Secrets from Proton Pass via pass-cli; values are kept in memory only."""

import os
import subprocess

from pydantic import SecretStr
from robonomicsinterface import Account
from substrateinterface import KeypairType

ROBONOMICS_ITEM_PREFIX = "Robonomics - "
SEED_FIELD = "seed"


class SecretUnavailableError(RuntimeError):
    pass


def read_pass_field(vault: str, item_title: str, field: str, reason: str) -> SecretStr:
    env = os.environ.copy()
    # Required by pass-cli when running under an agent token.
    env.setdefault("PROTON_PASS_AGENT_REASON", reason)
    try:
        result = subprocess.run(
            [
                "pass-cli",
                "item",
                "view",
                "--vault-name",
                vault,
                "--item-title",
                item_title,
                "--field",
                field,
            ],
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )
    except FileNotFoundError as e:
        raise SecretUnavailableError("pass-cli is not installed") from e

    value = result.stdout.strip()
    if result.returncode != 0 or not value:
        raise SecretUnavailableError(
            f"Cannot read field '{field}' of item '{item_title}' in vault "
            f"'{vault}' (pass-cli exit {result.returncode}); check `pass-cli login`"
        )
    return SecretStr(value)


def load_integrator_account(address: str, vault: str) -> Account:
    """Return the integrator account whose seed is stored under its address."""

    seed = read_pass_field(
        vault,
        ROBONOMICS_ITEM_PREFIX + address,
        SEED_FIELD,
        reason="Decrypt Home Assistant reports",
    )
    account = Account(seed.get_secret_value(), crypto_type=KeypairType.ED25519)
    if account.get_address() != address:
        raise SecretUnavailableError(
            f"Seed in Proton Pass does not derive the integrator address {address}"
        )
    return account
