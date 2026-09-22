import pytest
from websocket import WebSocketConnectionClosedException

from rrs_connector.robonomics.retry import with_retries


def failing(times: int, error: BaseException, result: str = "ok"):
    """A call that fails `times` times before returning."""

    calls = {"n": 0}

    def call():
        calls["n"] += 1
        if calls["n"] <= times:
            raise error
        return result

    call.calls = calls
    return call


def test_a_connection_that_fails_once_is_simply_repeated():
    call = failing(1, ConnectionResetError("connection reset"))
    slept = []

    assert with_retries(call, "reading", 3, 2, sleep=slept.append) == "ok"
    assert call.calls["n"] == 2
    assert slept == [2]


def test_the_last_failure_is_raised_as_it_was():
    call = failing(5, TimeoutError("the node did not answer"))

    with pytest.raises(TimeoutError, match="did not answer"):
        with_retries(call, "reading", 3, 0, sleep=lambda _: None)
    assert call.calls["n"] == 3


def test_between_attempts_the_caller_may_reconnect():
    call = failing(2, WebSocketConnectionClosedException())
    reconnects = []

    with_retries(
        call, "reading", 3, 0,
        before_retry=lambda: reconnects.append(1),
        sleep=lambda _: None,
    )

    assert len(reconnects) == 2


def test_an_error_about_the_data_is_not_retried():
    # Only connection-level failures are safe to repeat: a KeyError says
    # something about what came back, and repeating it hides the problem.
    call = failing(1, KeyError("start"))

    with pytest.raises(KeyError):
        with_retries(call, "reading", 3, 0, sleep=lambda _: None)
    assert call.calls["n"] == 1
