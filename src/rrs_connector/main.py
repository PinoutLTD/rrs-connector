import argparse
import logging
import sys
from pathlib import Path

from rrs_connector.config import load_settings
from rrs_connector.fetch import fetch
from rrs_connector.logging_config import setup_logging
from rrs_connector.pipeline import RunOnceResult, run_once

LOGGER = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Collect Robonomics Report Service reports"
    )
    parser.add_argument(
        "--command", type=str, choices=["run-once", "fetch"], default="run-once"
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


if __name__ == "__main__":
    sys.exit(main())
