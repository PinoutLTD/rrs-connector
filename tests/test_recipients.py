from pathlib import Path

import pytest
from conftest import build_report_archive, ed25519_account
from pydantic import ValidationError
from test_pipeline import (
    CID_1,
    TIMESTAMP_1,
    FakeReader,
    only_entry,
    open_store,
    registry_for,
)

from rrs_connector.config import EnvSettings
from rrs_connector.reports.recipients import (
    NotAddressedToUsError,
    RecipientKeys,
    archive_recipients,
)
from rrs_connector.state.models import DatalogStatus

FILES = {"issue_description.json": '{"type": "test"}', "home-assistant.log": "log\n"}


@pytest.fixture
def reader() -> FakeReader:
    return FakeReader()


@pytest.fixture(scope="module")
def site():
    return ed25519_account()


@pytest.fixture(scope="module")
def old_key():
    return ed25519_account()


@pytest.fixture(scope="module")
def new_key():
    return ed25519_account()


class CountingLoader:
    def __init__(self, *accounts) -> None:
        self.accounts = {account.address: account for account in accounts}
        self.loaded: list[str] = []

    def __call__(self, address: str):
        self.loaded.append(address)
        return self.accounts[address]


def test_envelope_names_its_recipients(tmp_path, site, new_key) -> None:
    archive = build_report_archive(
        tmp_path / "a.zip", site, [new_key.address], FILES
    )

    assert archive_recipients(archive) == {
        site.address,
        new_key.address,
    }


def test_the_key_the_report_needs_is_chosen(tmp_path, site, old_key, new_key) -> None:
    archive = build_report_archive(
        tmp_path / "a.zip", site, [new_key.address], FILES
    )
    keys = RecipientKeys(
        [old_key.address, new_key.address], CountingLoader()
    )

    assert keys.choose(archive) == new_key.address


def test_configuration_order_breaks_a_tie(tmp_path, site, old_key, new_key) -> None:
    archive = build_report_archive(
        tmp_path / "a.zip",
        site,
        [old_key.address, new_key.address],
        FILES,
    )
    keys = RecipientKeys(
        [new_key.address, old_key.address], CountingLoader()
    )

    assert keys.choose(archive) == new_key.address


def test_report_for_a_stranger_names_who_it_was_for(
    tmp_path, site, old_key, new_key
) -> None:
    stranger = ed25519_account()
    archive = build_report_archive(
        tmp_path / "a.zip", site, [stranger.address], FILES
    )
    keys = RecipientKeys([old_key.address], CountingLoader())

    with pytest.raises(NotAddressedToUsError, match=stranger.address):
        keys.choose(archive)


def test_keys_are_loaded_once_and_only_our_own(old_key, new_key) -> None:
    loader = CountingLoader(old_key, new_key)
    keys = RecipientKeys([old_key.address], loader)

    keys.account(old_key.address)
    keys.account(old_key.address)

    assert loader.loaded == [old_key.address]
    # An address from an envelope never reaches Proton Pass unless we configured it.
    with pytest.raises(ValueError, match="not a configured recipient"):
        keys.account(new_key.address)


# Through the pipeline


class ArchiveServer:
    def __init__(self, archive: Path) -> None:
        self.archive = archive

    def __call__(self, cid: str, destination: Path, settings) -> int:
        destination.write_bytes(self.archive.read_bytes())
        return destination.stat().st_size


def run_with_keys(
    env_settings, network_config, reader, site, archive, loader, addresses
):
    from rrs_connector.pipeline import run_once

    reader.publish(site.address, 0, TIMESTAMP_1, CID_1)
    settings = env_settings.model_copy(update={"integrator_addresses": addresses})
    return run_once(
        settings,
        network_config,
        registry_for(("home", site.address)),
        reader,
        load_account=loader,
        download=ArchiveServer(archive),
    )


def test_report_for_the_new_key_is_processed_without_touching_the_old(
    env_settings, network_config, reader, tmp_path, site, old_key, new_key
) -> None:
    archive = build_report_archive(
        tmp_path / "a.zip", site, [new_key.address], FILES
    )
    loader = CountingLoader(old_key, new_key)

    result = run_with_keys(
        env_settings,
        network_config,
        reader,
        site,
        archive,
        loader,
        [old_key.address, new_key.address],
    )

    assert result.reports_processed == 1
    assert loader.loaded == [new_key.address]


def test_report_for_nobody_we_know_fails_without_loading_a_key(
    env_settings, network_config, reader, tmp_path, site, old_key
) -> None:
    stranger = ed25519_account()
    archive = build_report_archive(
        tmp_path / "a.zip", site, [stranger.address], FILES
    )
    loader = CountingLoader(old_key)

    result = run_with_keys(
        env_settings,
        network_config,
        reader,
        site,
        archive,
        loader,
        [old_key.address],
    )

    entry = only_entry(open_store(env_settings))
    assert result.reports_failed == 1
    assert entry.status == DatalogStatus.FAILED
    assert "none of them is a configured recipient key" in entry.error_message
    assert loader.loaded == []


# Settings


def settings_with(tmp_path, **values) -> EnvSettings:
    return EnvSettings(
        _env_file=None,
        data_dir=tmp_path,
        state_db=tmp_path / "state.sqlite3",
        poll_interval_seconds=600,
        network_config_file=tmp_path / "n.yaml",
        senders_config_file=tmp_path / "s.yaml",
        **values,
    )


def test_addresses_come_from_a_comma_separated_list(
    tmp_path, monkeypatch, old_key, new_key
) -> None:
    monkeypatch.setenv(
        "RRS_INTEGRATOR_ADDRESSES",
        f"{old_key.address}, {new_key.address}",
    )

    assert settings_with(tmp_path).integrator_addresses == [
        old_key.address,
        new_key.address,
    ]


def test_the_single_address_setting_still_works(tmp_path, old_key, new_key) -> None:
    settings = settings_with(
        tmp_path,
        integrator_addresses=[new_key.address],
        integrator_address=old_key.address,
    )

    assert settings.integrator_addresses == [
        new_key.address,
        old_key.address,
    ]


def test_at_least_one_valid_address_is_required(tmp_path) -> None:
    with pytest.raises(ValidationError, match="no recipient key"):
        settings_with(tmp_path)
    with pytest.raises(ValidationError, match="not a valid Robonomics address"):
        settings_with(tmp_path, integrator_addresses=["not-an-address"])


def test_an_address_of_another_network_is_refused(tmp_path) -> None:
    # The well-known development account Alice in the generic Substrate format:
    # a valid address, but a report can never be encrypted for it here.
    alice = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"

    with pytest.raises(ValidationError, match="not a valid Robonomics address"):
        settings_with(tmp_path, integrator_addresses=[alice])
