"""Guards the ``s3fs`` version floor that ADR-0006 (decision 8a) requires.

An unconstrained ``s3fs`` resolved to the 2020 release (0.4.2), incompatible
with current ``fsspec``/``botocore``: any S3 data write under a catalog that
vends a ``FsspecFileIO`` table config (Nessie's REST catalog does, for every
write -- docs/spikes/NESSIE_SPIKES.md, S1) raised ``TypeError:
botocore.session.Session.create_client() got multiple values for keyword
argument 'aws_access_key_id'``. ``pyproject.toml`` now pins ``s3fs>=2024.1.0``;
this test fails if the installed environment (or a future unpin) drifts back
below that floor.
"""

from __future__ import annotations

from importlib.metadata import version

from packaging.version import Version


def test_s3fs_is_at_least_2024_1_0():
    installed = Version(version("s3fs"))
    assert installed >= Version("2024.1.0"), (
        f"s3fs {installed} is older than 2024.1.0 -- this reintroduces the "
        "aws_access_key_id TypeError on any Nessie-backed S3 write "
        "(docs/spikes/NESSIE_SPIKES.md, S1). Check pyproject.toml's pin."
    )
