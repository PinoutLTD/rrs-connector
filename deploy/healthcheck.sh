#!/bin/sh
# Tell Healthchecks.io how this run ended. Runs as ExecStopPost=.
#
# A successful run pings the check. A failed run is only logged there
# (`/log`), without changing the check's state: the alert comes when no run
# has succeeded for the check's period plus grace time (1 h + 1 h), so one
# passing failure (a node or Odoo briefly unreachable) wakes nobody, two in a
# row do. A whole server down is caught the same way: nothing pings at all.
#
# systemd sets $SERVICE_RESULT ("success", or why the run failed) and
# $EXIT_STATUS. The check's ping URL is a credential (LoadCredential=
# healthcheck:…): whoever knows it can report "all is well", so it is kept
# out of git and out of the unit. Only the outcome leaves the server — no
# logs, nothing about sites. Neither a missing URL nor an unreachable
# Healthchecks may fail the service.
set -u

url_file="${CREDENTIALS_DIRECTORY:-/nonexistent}/healthcheck"
[ -r "$url_file" ] || exit 0
url="$(cat "$url_file")"
[ -n "$url" ] || exit 0

result="${SERVICE_RESULT:-unknown}"
if [ "$result" = "success" ]; then
    target="$url"
else
    target="$url/log"
fi

curl -fsS -m 10 --retry 3 -o /dev/null \
    --data-raw "result=$result exit_status=${EXIT_STATUS:-unknown}" \
    "$target" || true
exit 0
