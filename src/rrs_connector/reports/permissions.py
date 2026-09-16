"""File modes for report artifacts.

Decrypted reports are logs from clients' homes, so the default is owner-only.
When the admin layer runs as a separate user on the same host it still has to
read them to attach them to a ticket, and that is the only reason to widen
anything: a deployment can open the artifacts to the owning group, and the
group is shared with that one service. Other users never get access either way.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class ArtifactModes:
    dir_mode: int
    file_mode: int


PRIVATE = ArtifactModes(dir_mode=0o700, file_mode=0o600)
GROUP_READABLE = ArtifactModes(dir_mode=0o750, file_mode=0o640)


def artifact_modes(group_readable: bool) -> ArtifactModes:
    return GROUP_READABLE if group_readable else PRIVATE
