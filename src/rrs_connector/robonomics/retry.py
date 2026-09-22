"""Retrying a chain read that failed for network reasons.

Reads are idempotent, so a failed one can simply be repeated. What must not be
repeated blindly is a failure that says something about the data — a missing
storage item, a malformed record — so only connection-level errors are caught
here: TLS and socket failures, timeouts, closed websockets.

Observed: of 32 runs in a day, four died on the first sender because the very
first connection of the run failed its TLS handshake. Nothing was wrong with
that site, and the run's exit code said the whole run had failed.
"""

import logging
import time
from collections.abc import Callable

from websocket import WebSocketException

LOGGER = logging.getLogger(__name__)

# OSError covers socket and TLS failures (ConnectionError and ssl.SSLError are
# subclasses); WebSocketException covers a closed or broken websocket.
TRANSIENT_ERRORS = (OSError, TimeoutError, WebSocketException)


def with_retries[T](
    operation: Callable[[], T],
    what: str,
    max_attempts: int,
    backoff_seconds: float,
    before_retry: Callable[[], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Run `operation`, repeating it while it fails for network reasons.

    `before_retry` is called between attempts, so the caller can reconnect or
    move to another endpoint. The last failure is raised as it was.
    """

    for attempt in range(1, max_attempts + 1):
        try:
            return operation()
        except TRANSIENT_ERRORS as e:
            if attempt == max_attempts:
                LOGGER.warning(
                    "%s failed %d times, giving up: %s", what, attempt, _short(e)
                )
                raise
            LOGGER.info(
                "%s failed (attempt %d of %d), retrying in %ss: %s",
                what,
                attempt,
                max_attempts,
                backoff_seconds,
                _short(e),
            )
            if before_retry is not None:
                before_retry()
            sleep(backoff_seconds)
    raise AssertionError("unreachable: the loop either returns or raises")


def _short(error: BaseException) -> str:
    text = str(error) or error.__class__.__name__
    return f"{error.__class__.__name__}: {text}"
