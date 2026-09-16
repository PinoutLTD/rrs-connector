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
- `/etc/rrs/connector.token` root-only, holding the agent token;
- `/var/lib/rrs-connector/pass-session` owned by the service user;
- `pass-cli` in `/usr/local/bin`, and a `.venv` built from `uv sync` (uv is not
  needed at runtime).

`.env` on the host sets `RRS_ARTIFACT_GROUP_READABLE=true` so the helpdesk
layer can read the decrypted files, and the retention ages
(`RRS_KEEP_DECRYPTED_DAYS`, `RRS_KEEP_ARCHIVE_DAYS`).
