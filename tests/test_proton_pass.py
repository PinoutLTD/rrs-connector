import subprocess

import pytest

from rrs_connector import proton_pass
from rrs_connector.proton_pass import (
    SecretUnavailableError,
    load_integrator_account,
    read_pass_field,
)


class FakeRun:
    def __init__(self, stdout: str = "", returncode: int = 0, error=None) -> None:
        self.stdout = stdout
        self.returncode = returncode
        self.error = error
        self.calls: list[tuple[list[str], dict]] = []

    def __call__(self, command, **kwargs):
        self.calls.append((command, kwargs))
        if self.error:
            raise self.error
        return subprocess.CompletedProcess(command, self.returncode, self.stdout, "")


def test_read_pass_field_returns_secret_and_sets_reason(monkeypatch) -> None:
    fake = FakeRun(stdout="value\n")
    monkeypatch.setattr(proton_pass.subprocess, "run", fake)
    monkeypatch.delenv("PROTON_PASS_AGENT_REASON", raising=False)

    secret = read_pass_field("Report Service", "Item", "seed", reason="Why")

    assert secret.get_secret_value() == "value"
    assert "value" not in repr(secret)
    command, kwargs = fake.calls[0]
    assert command == [
        "pass-cli", "item", "view",
        "--vault-name", "Report Service",
        "--item-title", "Item",
        "--field", "seed",
    ]  # fmt: skip
    assert kwargs["env"]["PROTON_PASS_AGENT_REASON"] == "Why"


def test_read_pass_field_keeps_reason_from_environment(monkeypatch) -> None:
    fake = FakeRun(stdout="value")
    monkeypatch.setattr(proton_pass.subprocess, "run", fake)
    monkeypatch.setenv("PROTON_PASS_AGENT_REASON", "Set by the service unit")

    read_pass_field("Vault", "Item", "seed", reason="Default")

    assert (
        fake.calls[0][1]["env"]["PROTON_PASS_AGENT_REASON"] == "Set by the service unit"
    )


@pytest.mark.parametrize(
    "fake",
    [
        FakeRun(stdout="", returncode=1),
        FakeRun(stdout="   \n", returncode=0),
    ],
)
def test_read_pass_field_raises_without_value(monkeypatch, fake) -> None:
    monkeypatch.setattr(proton_pass.subprocess, "run", fake)

    with pytest.raises(SecretUnavailableError, match="pass-cli login"):
        read_pass_field("Vault", "Item", "seed", reason="Why")


def test_read_pass_field_raises_when_pass_cli_is_missing(monkeypatch) -> None:
    monkeypatch.setattr(
        proton_pass.subprocess, "run", FakeRun(error=FileNotFoundError("pass-cli"))
    )

    with pytest.raises(SecretUnavailableError, match="not installed"):
        read_pass_field("Vault", "Item", "seed", reason="Why")


def test_load_integrator_account_reads_item_by_address(monkeypatch, ha_report) -> None:
    fake = FakeRun(stdout=ha_report["recipient_seed"])
    monkeypatch.setattr(proton_pass.subprocess, "run", fake)

    account = load_integrator_account(ha_report["recipient_address"], "Report Service")

    assert account.get_address() == ha_report["recipient_address"]
    assert f"Robonomics - {ha_report['recipient_address']}" in fake.calls[0][0]


def test_load_integrator_account_rejects_seed_for_other_address(
    monkeypatch, ha_report
) -> None:
    monkeypatch.setattr(
        proton_pass.subprocess, "run", FakeRun(stdout=ha_report["recipient_seed"])
    )

    with pytest.raises(SecretUnavailableError, match="does not derive"):
        load_integrator_account(ha_report["sender_address"], "Report Service")
