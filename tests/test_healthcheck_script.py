"""The Healthchecks.io reporting script: it must report, or say why it did not.

It once exited without a word because the ping URL, handed over with
LoadCredential=, cannot be read in ExecStopPost= on systemd 249, and no ping
was ever sent. These tests run the real script with a stand-in for curl.
"""

import os
import subprocess
from pathlib import Path

import pytest

DEPLOY = Path(__file__).parents[1] / "deploy"
SCRIPT = DEPLOY / "healthcheck.sh"
UNIT = DEPLOY / "rrs-connector.service"
URL = "https://hc.example.test/0000-test-ping"


@pytest.fixture
def run(tmp_path):
    """Run the script as systemd would; returns (completed, curl's arguments)."""

    calls = tmp_path / "calls"
    curl = tmp_path / "bin" / "curl"
    curl.parent.mkdir()
    curl.write_text(f'#!/bin/sh\necho "$@" >> {calls}\nexit "${{CURL_EXIT:-0}}"\n')
    curl.chmod(0o755)

    def go(url_file, result="success", status="0", curl_exit="0"):
        env = {
            **os.environ,
            "PATH": f"{curl.parent}:{os.environ['PATH']}",
            "SERVICE_RESULT": result,
            "EXIT_STATUS": status,
            "CURL_EXIT": curl_exit,
        }
        done = subprocess.run(
            [str(SCRIPT), str(url_file)], env=env, capture_output=True, text=True
        )
        return done, calls.read_text() if calls.exists() else ""

    return go


@pytest.fixture
def url_file(tmp_path):
    path = tmp_path / "ping"
    path.write_text(URL + "\n")
    return path


def test_a_successful_run_pings_the_check(run, url_file):
    done, curl_args = run(url_file)

    assert done.returncode == 0
    assert curl_args.split()[-1] == URL
    assert "result=success" in curl_args
    assert "reported result=success" in done.stderr
    assert URL not in done.stderr


def test_a_failed_run_is_only_logged_on_the_check(run, url_file):
    done, curl_args = run(url_file, result="exit-code", status="1")

    assert done.returncode == 0
    assert curl_args.split()[-1] == URL + "/log"
    assert "result=exit-code exit_status=1" in curl_args


@pytest.mark.parametrize("content", [None, ""])
def test_no_url_means_nothing_sent_and_a_line_in_the_journal(run, tmp_path, content):
    path = tmp_path / "missing"
    if content is not None:
        path.write_text(content)

    done, curl_args = run(path)

    assert done.returncode == 0
    assert curl_args == ""
    assert "nothing sent" in done.stderr


def test_a_run_is_not_failed_by_an_unreachable_healthchecks(run, url_file):
    done, _ = run(url_file, curl_exit="6")

    assert done.returncode == 0
    assert "could not report" in done.stderr
    assert URL not in done.stderr


def test_the_unit_gives_the_script_the_file_and_no_credential_for_it():
    unit = UNIT.read_text()
    stop_post = [line for line in unit.splitlines() if line.startswith("ExecStopPost=")]

    # The credentials directory cannot be read in ExecStopPost= on systemd 249.
    assert "healthcheck:" not in unit
    assert len(stop_post) == 1
    script_and_file = "/deploy/healthcheck.sh /etc/rrs/connector.healthcheck"
    assert stop_post[0].endswith(script_and_file)
