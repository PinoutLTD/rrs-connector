# Deployment

The connector runs as a systemd timer under its own `nologin` user, with the
Proton Pass agent token delivered by systemd and never readable by that user.

```bash
sudo install -m 0755 deploy/session.sh /mnt/disk/pinout-report-service/rrs-connector/deploy/session.sh
sudo install -m 0644 deploy/rrs-connector.service deploy/rrs-connector.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl start rrs-connector.service   # one run, watch the journal
sudo systemctl enable --now rrs-connector.timer
```

Expected around it:

- users `rrs-connector` and `rrs-bridge`, both in the shared group `rrs-reports`;
- `/mnt/disk/pinout-report-service/data/connector` owned by `rrs-connector:rrs-reports`;
- `/etc/rrs/connector.healthcheck` root-only, holding the Healthchecks.io ping URL
  of this service (see below);
- `/etc/rrs/connector.token` root-only, holding the agent token;
- `/var/lib/rrs-connector/pass-session` owned by the service user;
- `pass-cli` in `/usr/local/bin`, and a `.venv` built from `uv sync` (uv is not
  needed at runtime).

`.env` on the host sets `RRS_ARTIFACT_GROUP_READABLE=true` so the helpdesk
layer can read the decrypted files, and the retention ages
(`RRS_KEEP_DECRYPTED_DAYS`, `RRS_KEEP_ARCHIVE_DAYS`).

## Healthchecks.io

After every run, `deploy/healthcheck.sh` (the unit's `ExecStopPost=`) reports how the run ended to the Healthchecks.io check of this service:
- a successful run pings it;
- a failed run is only logged there.

The check is set to a 1 h period with 1 h grace. So the team gets an alert when no run has succeeded for two hours: two failures in a row, or the server down. A single passing failure wakes nobody.

The ping URL is a credential: whoever knows it can report "all is well". It lives in Proton Pass (vault "Report Service", item "rrs-connector (Healthchecks)", field Password) and on the host in a root-only file, handed to the unit by `LoadCredential=`. Copy it from Proton Pass to the host without printing it:

```bash
pass-cli item view --vault-name "Report Service" --item-title "rrs-connector (Healthchecks)" --field password \
  | ssh <host> 'umask 077 && cat > /etc/rrs/connector.healthcheck'
```

The unit does not start without the file. Neither a missing URL inside it nor an unreachable Healthchecks.io fails a run.
