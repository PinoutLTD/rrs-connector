"""Secrets from Proton Pass via pass-cli; values are kept in memory only."""

import json
import os
import subprocess

from pydantic import SecretStr
from robonomicsinterface import Account
from substrateinterface import KeypairType

ROBONOMICS_ITEM_PREFIX = "Robonomics - "
SEED_FIELD = "seed"


class SecretUnavailableError(RuntimeError):
    pass


def run_pass_cli(
    arguments: list[str], reason: str, stdin: str | None = None
) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    # Required by pass-cli when running under an agent token.
    env.setdefault("PROTON_PASS_AGENT_REASON", reason)
    try:
        return subprocess.run(
            ["pass-cli", *arguments],
            input=stdin,
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )
    except FileNotFoundError as e:
        raise SecretUnavailableError("pass-cli is not installed") from e


def item_share_id(item_title: str, reason: str) -> str | None:
    """The item's own share id, for a token granted just that one item.

    An agent token with item-level access cannot see the vault the item lives
    in, so the vault name is not an address it can use; the item is reachable
    through its own share instead.
    """

    result = run_pass_cli(["share", "list", "--output", "json"], reason)
    if result.returncode != 0:
        return None
    try:
        shares = json.loads(result.stdout)["shares"]
    except (json.JSONDecodeError, KeyError, TypeError):
        return None
    for share in shares:
        if share.get("share_type") == "Item" and share.get("name") == item_title:
            return share.get("id")
    return None


def view_field(
    address: list[str], item_title: str, field: str, reason: str
) -> subprocess.CompletedProcess:
    return run_pass_cli(
        ["item", "view", *address, "--item-title", item_title, "--field", field],
        reason,
    )


def read_pass_field(vault: str, item_title: str, field: str, reason: str) -> SecretStr:
    result = view_field(["--vault-name", vault], item_title, field, reason)
    value = result.stdout.strip()

    if result.returncode != 0 or not value:
        share_id = item_share_id(item_title, reason)
        if share_id:
            result = view_field(["--share-id", share_id], item_title, field, reason)
            value = result.stdout.strip()

    if result.returncode != 0 or not value:
        raise SecretUnavailableError(
            f"Cannot read field '{field}' of item '{item_title}' in vault "
            f"'{vault}' (pass-cli exit {result.returncode}); check "
            "`pass-cli login` and that the token has access to the item"
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
