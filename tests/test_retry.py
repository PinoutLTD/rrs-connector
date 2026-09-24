import pytest
from robonomicsinterface import (
    AllEndpointsFailed,
    ConnectionFailed,
    ConnectionLost,
    RequestTimeout,
    RpcError,
)

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
    call = failing(1, ConnectionLost("connection reset"))
    slept = []

    assert with_retries(call, "reading", 3, 2, sleep=slept.append) == "ok"
    assert call.calls["n"] == 2
    assert slept == [2]


def test_the_last_failure_is_raised_as_it_was():
    call = failing(5, RequestTimeout("the node did not answer"))

    with pytest.raises(RequestTimeout, match="did not answer"):
        with_retries(call, "reading", 3, 0, sleep=lambda _: None)
    assert call.calls["n"] == 3


def test_a_connection_that_cannot_be_opened_is_retried():
    # The observed failure: the first TLS handshake of a run fails, and the
    # library reports it as no usable node at all.
    refused = AllEndpointsFailed([ConnectionFailed("wss://node/", "cannot connect")])
    call = failing(1, refused)

    assert with_retries(call, "reading", 3, 0, sleep=lambda _: None) == "ok"


def test_an_answer_from_the_node_is_not_a_network_failure():
    call = failing(1, RpcError("state_getStorage", -32000, "bad params"))

    with pytest.raises(RpcError):
        with_retries(call, "reading", 3, 0, sleep=lambda _: None)
    assert call.calls["n"] == 1


def test_an_error_about_the_data_is_not_retried():
    # Only connection-level failures are safe to repeat: a KeyError says
    # something about what came back, and repeating it hides the problem.
    call = failing(1, KeyError("start"))

    with pytest.raises(KeyError):
        with_retries(call, "reading", 3, 0, sleep=lambda _: None)
    assert call.calls["n"] == 1
