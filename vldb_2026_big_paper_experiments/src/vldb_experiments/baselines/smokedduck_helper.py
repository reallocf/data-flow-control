"""Helper functions for lineage capture via the DuckDB lineage extension."""

from __future__ import annotations

import contextlib
import os
from pathlib import Path
import platform
import ssl
import tempfile
import urllib.error
import urllib.request
import zipfile

import duckdb

LINEAGE_EXTENSION_NAME = os.getenv("LINEAGE_EXTENSION_NAME", "lineage")
LINEAGE_EXTENSION_RELEASE = os.getenv("LINEAGE_EXTENSION_RELEASE", "v0.1.0")


def _duckdb_version_tag() -> str:
    version_env = os.getenv("LINEAGE_DUCKDB_VERSION")
    if version_env:
        return version_env if version_env.startswith("v") else f"v{version_env}"
    return f"v{duckdb.__version__}"


def _ensure_supported_duckdb_version(version_tag: str) -> None:
    if os.getenv("LINEAGE_DUCKDB_VERSION"):
        return
    if os.getenv("LINEAGE_STRICT_VERSION") != "1":
        return
    if version_tag != "v1.3.0":
        raise RuntimeError(
            "Lineage extension builds are currently published for DuckDB 1.3.0. "
            "Set LINEAGE_DUCKDB_VERSION to override and ensure your DuckDB version matches."
        )


def _detect_arch() -> str:
    arch_override = os.getenv("LINEAGE_EXTENSION_ARCH")
    if arch_override:
        return arch_override

    system = platform.system().lower()
    machine = platform.machine().lower()

    if system == "darwin":
        os_part = "osx"
    elif system == "linux":
        os_part = "linux"
    elif system in {"windows", "msys", "cygwin"}:
        os_part = "windows"
    else:
        raise RuntimeError(f"Unsupported OS for lineage extension: {platform.system()}")

    if machine in {"x86_64", "amd64"}:
        arch_part = "amd64"
    elif machine in {"arm64", "aarch64"}:
        arch_part = "arm64"
    else:
        arch_part = machine

    return f"{os_part}_{arch_part}"


def _extension_dir(version_tag: str, arch: str) -> Path:
    return Path.home() / ".duckdb" / "extensions" / version_tag / arch


def _extension_path(version_tag: str, arch: str) -> Path:
    return _extension_dir(version_tag, arch) / f"{LINEAGE_EXTENSION_NAME}.duckdb_extension"


def _extension_filename(version_tag: str, arch: str) -> str:
    return f"{LINEAGE_EXTENSION_NAME}-{version_tag}-extension-{arch}.zip"


def ensure_lineage_extension() -> Path:
    version_tag = _duckdb_version_tag()
    _ensure_supported_duckdb_version(version_tag)
    arch = _detect_arch()
    extension_path = _extension_path(version_tag, arch)
    if extension_path.exists():
        return extension_path

    extension_dir = _extension_dir(version_tag, arch)
    extension_dir.mkdir(parents=True, exist_ok=True)

    filename = _extension_filename(version_tag, arch)
    url_override = os.getenv("LINEAGE_EXTENSION_URL")
    if url_override:
        download_url = url_override
    else:
        download_url = (
            f"https://github.com/haneensa/{LINEAGE_EXTENSION_NAME}/releases/download/"
            f"{LINEAGE_EXTENSION_RELEASE}/{filename}"
        )

    def _download_ssl_context() -> ssl.SSLContext | None:
        if os.getenv("LINEAGE_INSECURE_SSL") == "1":
            return ssl._create_unverified_context()
        # Use certifi bundle when available to avoid local trust-store issues.
        with contextlib.suppress(Exception):
            import certifi  # type: ignore

            return ssl.create_default_context(cafile=certifi.where())
        return None

    try:
        with urllib.request.urlopen(download_url, context=_download_ssl_context()) as response:
            data = response.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            raise RuntimeError(
                "Failed to download lineage extension: release asset not found (404). "
                f"Requested asset URL: {download_url}. "
                f"Current DuckDB version: {duckdb.__version__}. "
                "Lineage assets are typically built for specific DuckDB versions. "
                "Set LINEAGE_DUCKDB_VERSION/LINEAGE_EXTENSION_ARCH/LINEAGE_EXTENSION_URL "
                "or align DuckDB to a supported version."
            ) from exc
        raise RuntimeError(
            "Failed to download lineage extension. "
            f"Tried URL: {download_url}. "
            "Set LINEAGE_DUCKDB_VERSION, LINEAGE_EXTENSION_ARCH, or LINEAGE_EXTENSION_URL to override."
        ) from exc
    except ssl.SSLCertVerificationError as exc:
        raise RuntimeError(
            "Failed to download lineage extension due to SSL certificate verification. "
            "Try setting SSL_CERT_FILE to a CA bundle (for example certifi) or set "
            "LINEAGE_INSECURE_SSL=1 if your environment requires insecure TLS."
        ) from exc
    except Exception as exc:
        raise RuntimeError(
            "Failed to download lineage extension. "
            f"Tried URL: {download_url}. "
            "Set LINEAGE_DUCKDB_VERSION, LINEAGE_EXTENSION_ARCH, or LINEAGE_EXTENSION_URL to override."
        ) from exc

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_file:
        tmp_file.write(data)
        tmp_path = Path(tmp_file.name)

    try:
        with zipfile.ZipFile(tmp_path, "r") as archive:
            archive.extractall(extension_dir)
    finally:
        with contextlib.suppress(Exception):
            tmp_path.unlink(missing_ok=True)

    if not extension_path.exists():
        raise FileNotFoundError(
            "Lineage extension download completed but the extension file was not found at "
            f"{extension_path}"
        )

    return extension_path


def _load_lineage_extension(conn: duckdb.DuckDBPyConnection) -> None:
    extension_path = ensure_lineage_extension()
    with contextlib.suppress(Exception):
        conn.execute("SET allow_unsigned_extensions=true")
    try:
        conn.execute(f"LOAD '{extension_path.as_posix()}'")
    except Exception as exc:
        raise RuntimeError(
            "Failed to load lineage extension. "
            "Ensure allow_unsigned_extensions is enabled and the extension matches your DuckDB version."
        ) from exc


def is_smokedduck_available() -> bool:
    """Check if the lineage extension can be loaded."""
    try:
        conn = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
        _load_lineage_extension(conn)
        conn.close()
        return True
    except Exception as exc:
        raise ImportError(
            "Lineage extension is required for the physical baseline but could not be loaded. "
            "Run setup_venv.sh or set LINEAGE_DUCKDB_VERSION/LINEAGE_EXTENSION_ARCH to match your platform."
        ) from exc


def enable_lineage(conn: duckdb.DuckDBPyConnection) -> None:
    """Enable lineage capture for the provided connection."""
    is_smokedduck_available()
    _load_lineage_extension(conn)
    try:
        conn.execute("PRAGMA set_lineage(TRUE)")
        with contextlib.suppress(Exception):
            conn.execute("PRAGMA set_persist_lineage(TRUE)")
    except Exception as exc:
        raise RuntimeError("Failed to enable lineage via PRAGMA set_lineage(TRUE).") from exc


def _disable_lineage(conn: duckdb.DuckDBPyConnection) -> None:
    with contextlib.suppress(Exception):
        conn.execute("PRAGMA set_lineage(FALSE)")


def _run_without_lineage(conn: duckdb.DuckDBPyConnection, action):
    """Run a callable with lineage capture disabled to avoid polluting lineage tables."""
    _disable_lineage(conn)
    try:
        return action()
    finally:
        with contextlib.suppress(Exception):
            enable_lineage(conn)


def disable_lineage(conn: duckdb.DuckDBPyConnection) -> None:
    """Disable lineage capture for the provided connection."""
    _disable_lineage(conn)


def run_without_lineage(conn: duckdb.DuckDBPyConnection, action):
    """Public wrapper to run a callable with lineage capture disabled."""
    return _run_without_lineage(conn, action)


def build_lineage_query(
    conn: duckdb.DuckDBPyConnection,
    policy_source: str,
    query_id: int,
) -> str:
    """Build a lineage query using read_block() output."""
    def _quote_identifier(name: str) -> str:
        escaped = name.replace('"', '""')
        return f'"{escaped}"'

    cursor = conn.execute(f"SELECT * FROM read_block({query_id}) LIMIT 0")
    columns = [desc[0] for desc in cursor.description] if cursor.description else []
    if not columns:
        raise RuntimeError("read_block returned no columns; cannot build lineage query")

    output_col = None
    source_col = None
    for col in columns:
        if col.lower() == "output_id":
            output_col = col
            continue
        if policy_source.lower() in col.lower():
            source_col = col

    if output_col is None:
        raise RuntimeError(f"read_block columns missing output_id: {columns}")
    if source_col is None:
        raise RuntimeError(
            f"read_block columns missing source table {policy_source}: {columns}"
        )

    return (
        "SELECT "
        f"{_quote_identifier(output_col)} AS out_index, "
        f"{_quote_identifier(source_col)} AS {_quote_identifier(policy_source)} "
        f"FROM read_block({query_id})"
    )
