"""The seed goes to Proton Pass through stdin and is checked on the way back."""

import json
import subprocess

import pytest
from robonomicsinterface import Keypair, address_format

from rrs_connector import proton_pass
from rrs_connector.keygen import KeygenError, create_recipient_key, custom_item

TEMPLATE = {"title": "", "note": "", "sections": []}


class FakePassCli:
    """A vault in memory: create stores the item, view reads it back."""

    def __init__(self, template=TEMPLATE, fail_create=False, mangle=False) -> None:
        self.template = template
        self.fail_create = fail_create
        self.mangle = mangle
        self.items: dict[str, dict] = {}
        self.calls: list[tuple[list[str], dict]] = []

    def __call__(self, command, **kwargs):
        self.calls.append((command, kwargs))
        if command[1:4] == ["item", "create", "custom"] and "--get-template" in command:
            return subprocess.CompletedProcess(
                command, 0, json.dumps(self.template), ""
            )
        if command[1:4] == ["item", "create", "custom"]:
            if self.fail_create:
                return subprocess.CompletedProcess(command, 1, "", "vault not found")
            item = json.loads(kwargs["input"])
            if self.mangle:
                item["sections"][0]["fields"][0]["value"] = Keypair.generate_mnemonic()
            self.items[item["title"]] = item
            return subprocess.CompletedProcess(command, 0, "created", "")
        if command[1:3] == ["item", "view"]:
            title = command[command.index("--item-title") + 1]
            field = command[command.index("--field") + 1]
            for section in self.items[title]["sections"]:
                for entry in section["fields"]:
                    if entry["field_name"] == field:
                        return subprocess.CompletedProcess(
                            command, 0, entry["value"] + "\n", ""
                        )
        return subprocess.CompletedProcess(command, 1, "", "unexpected")


@pytest.fixture
def vault(monkeypatch) -> FakePassCli:
    fake = FakePassCli()
    monkeypatch.setattr(proton_pass.subprocess, "run", fake)
    return fake


def test_key_is_created_and_verified(vault, capsys) -> None:
    key = create_recipient_key("Report Service")

    item = vault.items[key.item_title]
    fields = {f["field_name"]: f for f in item["sections"][0]["fields"]}
    derived = Keypair.from_mnemonic(fields["seed"]["value"]).address

    assert key.item_title == f"Robonomics - {key.address}"
    assert derived == key.address
    assert fields["address"]["value"] == key.address
    assert fields["seed"]["field_type"] == "hidden"


def test_seed_travels_only_through_stdin(vault, capsys) -> None:
    key = create_recipient_key("Report Service")
    seed = vault.items[key.item_title]["sections"][0]["fields"][0]["value"]

    for command, _kwargs in vault.calls:
        assert seed not in " ".join(command), "the seed must never be an argument"
    created = [
        kwargs for command, kwargs in vault.calls if "--from-template" in command
    ]
    assert seed in created[0]["input"]
    assert seed not in capsys.readouterr().out


def test_failed_creation_is_reported(monkeypatch) -> None:
    monkeypatch.setattr(proton_pass.subprocess, "run", FakePassCli(fail_create=True))

    with pytest.raises(KeygenError, match="could not create"):
        create_recipient_key("Nowhere")


def test_a_stored_seed_that_does_not_match_is_caught(monkeypatch) -> None:
    monkeypatch.setattr(proton_pass.subprocess, "run", FakePassCli(mangle=True))

    with pytest.raises(KeygenError, match="do not use this item"):
        create_recipient_key("Report Service")


def test_unexpected_template_is_refused() -> None:
    with pytest.raises(KeygenError, match="unexpected custom item template"):
        custom_item({"name": "x"}, "t", "seed", "address")


def test_generated_keys_are_ed25519_robonomics_addresses(vault) -> None:
    key = create_recipient_key("Report Service")

    assert key.address.startswith("4")
    assert address_format(key.address) == 32
