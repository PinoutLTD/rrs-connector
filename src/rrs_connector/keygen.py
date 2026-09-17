"""Creating a recipient key straight into Proton Pass.

The seed is generated in memory, handed to `pass-cli` through stdin, read back
once to prove the stored copy derives the same address, and never printed,
written to disk or put on the clipboard. What the person sees is the public
address and what to do with it.

The key is ED25519: report encryption requires it on both sides.
"""

import json
import logging
from dataclasses import dataclass

from substrateinterface import Keypair, KeypairType

from rrs_connector.proton_pass import (
    ROBONOMICS_ITEM_PREFIX,
    SEED_FIELD,
    SecretUnavailableError,
    read_pass_field,
    run_pass_cli,
)

LOGGER = logging.getLogger(__name__)

ROBONOMICS_SS58_FORMAT = 32
ADDRESS_FIELD = "address"
SECTION_NAME = "Robonomics"
REASON = "Create a Robonomics recipient key for Report Service"


class KeygenError(RuntimeError):
    pass


@dataclass(frozen=True)
class NewKey:
    address: str
    item_title: str
    vault: str


def item_title(address: str) -> str:
    return ROBONOMICS_ITEM_PREFIX + address


def custom_item(template: dict, title: str, seed: str, address: str) -> dict:
    """Fill pass-cli's own template, so the item matches what it expects."""

    if not isinstance(template, dict) or "title" not in template:
        raise KeygenError(
            "unexpected custom item template from pass-cli: "
            f"keys {sorted(template) if isinstance(template, dict) else type(template)}"
        )
    item = dict(template)
    item["title"] = title
    item["sections"] = [
        {
            "section_name": SECTION_NAME,
            "fields": [
                {"field_name": SEED_FIELD, "field_type": "hidden", "value": seed},
                {"field_name": ADDRESS_FIELD, "field_type": "text", "value": address},
            ],
        }
    ]
    return item


def create_recipient_key(vault: str) -> NewKey:
    mnemonic = Keypair.generate_mnemonic()
    keypair = Keypair.create_from_mnemonic(
        mnemonic, crypto_type=KeypairType.ED25519, ss58_format=ROBONOMICS_SS58_FORMAT
    )
    address = keypair.ss58_address
    title = item_title(address)

    template_result = run_pass_cli(
        ["item", "create", "custom", "--get-template"], REASON
    )
    if template_result.returncode != 0:
        raise KeygenError(
            "cannot get the custom item template; check `pass-cli login` "
            f"(exit {template_result.returncode})"
        )
    try:
        template = json.loads(template_result.stdout)
    except json.JSONDecodeError as e:
        raise KeygenError("pass-cli returned a template that is not JSON") from e

    payload = json.dumps(custom_item(template, title, mnemonic, address))
    created = run_pass_cli(
        [
            "item",
            "create",
            "custom",
            "--vault-name",
            vault,
            "--from-template",
            "-",
        ],
        REASON,
        stdin=payload,
    )
    del payload, mnemonic
    if created.returncode != 0:
        raise KeygenError(
            f"pass-cli could not create the item (exit {created.returncode}): "
            f"{created.stderr.strip()[:300]}"
        )

    # Read the stored seed back and derive again: an item that exists but holds
    # a mangled seed would only surface when a report fails to decrypt.
    try:
        stored = read_pass_field(vault, title, SEED_FIELD, reason=REASON)
    except SecretUnavailableError as e:
        raise KeygenError(f"the item was created but cannot be read back: {e}") from e
    derived = Keypair.create_from_mnemonic(
        stored.get_secret_value(),
        crypto_type=KeypairType.ED25519,
        ss58_format=ROBONOMICS_SS58_FORMAT,
    ).ss58_address
    if derived != address:
        raise KeygenError(
            f"the stored seed derives {derived}, not {address}; do not use this item"
        )

    LOGGER.info("Recipient key %s stored in vault '%s'", address, vault)
    return NewKey(address=address, item_title=title, vault=vault)
