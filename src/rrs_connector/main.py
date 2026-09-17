import argparse
import logging
import sys
from pathlib import Path

from rrs_connector.config import load_settings
from rrs_connector.fetch import fetch
from rrs_connector.keygen import KeygenError, create_recipient_key
from rrs_connector.logging_config import setup_logging
from rrs_connector.pipeline import RunOnceResult, run_once

LOGGER = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Collect Robonomics Report Service reports"
    )
    parser.add_argument(
        "--command",
        type=str,
        choices=["run-once", "fetch", "new-recipient-key"],
        default="run-once",
    )
    fetch_group = parser.add_argument_group(
        "fetch", "One-off decryption; leaves the state database and the pipeline alone"
    )
    fetch_group.add_argument("--sender", help="SS58 address that published the report")
    fetch_group.add_argument(
        "--cid", action="append", default=[], help="report CID (may be repeated)"
    )
    fetch_group.add_argument(
        "--last", type=int, help="fetch this many of the sender's latest reports"
    )
    fetch_group.add_argument("--output", type=Path, help="where to put the reports")
    args = parser.parse_args()
    command = args.command

    if command == "fetch":
        if not args.sender:
            parser.error("--sender is required for fetch")
        if bool(args.cid) == bool(args.last):
            parser.error("choose either --cid (one or more) or --last N")

    setup_logging()

    if command == "new-recipient-key":
        return new_recipient_key()

    LOGGER.info("Starting rrs-connector")

    try:
        LOGGER.info("Loading configuration")
        env_settings, network_config, sender_registry = load_settings()
    except Exception:
        LOGGER.exception("Application startup failed")
        return 1

    enabled_senders = sum(
        sender_config.enabled for sender_config in sender_registry.senders
    )
    LOGGER.info(
        "Configuration: network=%s, senders=%d, enabled_senders=%d, "
        "poll_interval_seconds=%d",
        network_config.network,
        len(sender_registry.senders),
        enabled_senders,
        env_settings.poll_interval_seconds,
    )

    LOGGER.info(
        "Config files: network=%s, senders=%s",
        env_settings.network_config_file,
        env_settings.senders_config_file,
    )

    try:
        if command == "fetch":
            return fetch(
                env_settings,
                network_config,
                sender_address=args.sender,
                cids=args.cid,
                last=args.last,
                output_dir=args.output,
            ).exit_code

        if command == "run-once":
            result: RunOnceResult = run_once(
                env_settings, network_config, sender_registry
            )
            return result.exit_code
    except Exception:
        LOGGER.exception("Application failed")
        return 1


def new_recipient_key() -> int:
    """Create a recipient key in Proton Pass; print only what is public."""

    import os

    vault = os.environ.get("RRS_PASS_VAULT", "Report Service")
    try:
        key = create_recipient_key(vault)
    except KeygenError as e:
        LOGGER.error("%s", e)
        return 1

    print(
        f"""
Recipient key created in vault '{key.vault}', item '{key.item_title}'.

Address: {key.address}

Next:
  1. Give the connector's agent token access to this one item:
     pass-cli agent access grant rrs-connector --vault-name "{key.vault}" \\
       --item-title "{key.item_title}" --role viewer
  2. Add the address to RRS_INTEGRATOR_ADDRESSES on the server, keeping the
     old one: sites already configured still encrypt for it.
  3. Use the address as "Problem service address" when installing new sites.
"""
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
