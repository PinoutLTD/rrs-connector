"""Retrying a chain read that failed for network reasons.

Reads are idempotent, so a failed one can simply be repeated. What must not be
repeated blindly is a failure that says something about the data — a missing
storage item, a malformed record — so only connection-level errors are caught
here: TLS and socket failures, timeouts, closed connections.

Observed: of 32 runs in a day, four died on the first sender because the very
first connection of the run failed its TLS handshake. Nothing was wrong with
that site, and the run's exit code said the whole run had failed.
"""

import logging
import time
from collections.abc import Callable

from robonomicsinterface import TransportError

LOGGER = logging.getLogger(__name__)

# The library raises TransportError when a node was not reached or stopped
# answering (TLS and socket failures, timeouts, a closed connection); an error
# the node itself answered with is not one of them.
TRANSIENT_ERRORS = (TransportError,)


def with_retries[T](
    operation: Callable[[], T],
    what: str,
    max_attempts: int,
    backoff_seconds: float,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Run `operation`, repeating it while it fails for network reasons.

    The next attempt reconnects on its own: the client drops a failed
    connection and opens one to the first endpoint that answers. The last
    failure is raised as it was.
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
            sleep(backoff_seconds)
    raise AssertionError("unreachable: the loop either returns or raises")


def _short(error: BaseException) -> str:
    text = str(error) or error.__class__.__name__
    return f"{error.__class__.__name__}: {text}"
