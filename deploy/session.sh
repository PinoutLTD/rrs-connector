#!/bin/sh
# Make sure this service has a Proton Pass session, then run the command.
#
# The agent token is never readable by the service user: systemd copies it
# into $CREDENTIALS_DIRECTORY for this unit only (LoadCredential=). The
# session lives in the service's own directory, so services do not share one.
set -eu

if [ -z "${PROTON_PASS_SESSION_DIR:-}" ]; then
    echo "session.sh: PROTON_PASS_SESSION_DIR is not set" >&2
    exit 1
fi

token_file="${CREDENTIALS_DIRECTORY:-/nonexistent}/token"

if ! pass-cli info >/dev/null 2>&1; then
    if [ ! -r "$token_file" ]; then
        echo "session.sh: no agent token at $token_file" >&2
        exit 1
    fi
    # A stale session directory would keep failing; clear it first.
    pass-cli logout --force >/dev/null 2>&1 || true
    PROTON_PASS_PERSONAL_ACCESS_TOKEN="$(cat "$token_file")" \
        pass-cli login >/dev/null
fi

exec "$@"
