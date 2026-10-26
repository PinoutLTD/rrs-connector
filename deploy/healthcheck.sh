#!/bin/sh
# Tell Healthchecks.io how this run ended. Runs as ExecStopPost=, with the file
# that holds the check's ping URL as its argument.
#
# A successful run pings the check. A failed run is only logged there
# (`/log`), without changing the check's state: the alert comes when no run
# has succeeded for the check's period plus grace time (1 h + 1 h), so one
# passing failure (a node or Odoo briefly unreachable) wakes nobody, two in a
# row do. A whole server down is caught the same way: nothing pings at all.
#
# systemd sets $SERVICE_RESULT ("success", or why the run failed) and
# $EXIT_STATUS. The ping URL is a secret: whoever knows it can report "all is
# well", so it is kept out of git and out of the unit, in a file readable by
# the service's group only. It is not handed over with LoadCredential=: on
# systemd 249 (Ubuntu 22.04) that directory cannot be read in ExecStopPost=,
# and this script used to exit there without a word, so no ping was ever sent.
#
# Only the outcome leaves the server — no logs, nothing about sites. Nothing
# here may fail the service, but every way of not reporting leaves a line in
# the journal, never the URL.
set -u

url_file="${1:-}"
if [ -z "$url_file" ] || [ ! -r "$url_file" ]; then
    echo "healthcheck: no readable ping URL file '$url_file', nothing sent" >&2
    exit 0
fi
url="$(cat "$url_file")"
if [ -z "$url" ]; then
    echo "healthcheck: '$url_file' is empty, nothing sent" >&2
    exit 0
fi

result="${SERVICE_RESULT:-unknown}"
if [ "$result" = "success" ]; then
    target="$url"
else
    target="$url/log"
fi

if curl -fsS -m 10 --retry 3 -o /dev/null \
    --data-raw "result=$result exit_status=${EXIT_STATUS:-unknown}" \
    "$target"; then
    echo "healthcheck: reported result=$result" >&2
else
    echo "healthcheck: could not report result=$result to Healthchecks.io" >&2
fi
exit 0
