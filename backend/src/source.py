"""Source data preparation for The Networks of War preprocessing."""

from __future__ import annotations

import argparse
import json
import shutil
import ssl
import subprocess
import tempfile
import urllib.error
import urllib.request
import zipfile
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from utils import initialize_logger

logger = initialize_logger(__name__)

BACKEND_ROOT = Path(__file__).resolve().parents[1]
WORK_CSV_DIR = BACKEND_ROOT / ".work" / "csv"

DEFAULT_DATA_DIR = BACKEND_ROOT / "data"
DEFAULT_CSV_DIR = DEFAULT_DATA_DIR
PARTICIPANT_NAME_REPLACEMENTS_PATH = BACKEND_ROOT / "manual" / "participant_name_replacements.json"
SOURCE_METADATA_PATH = BACKEND_ROOT / "manual" / "source_metadata.json"
SYSTEM_CA_FILE = Path("/etc/ssl/cert.pem")


def add_source_data_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR, type=Path)
    parser.add_argument("--csv-dir", dest="data_dir", type=Path, help=argparse.SUPPRESS)
    parser.add_argument(
        "--prepare-data", action="store_true", help="Download and validate missing source data folders."
    )
    parser.add_argument("--recreate-data", action="store_true", help="Delete and recreate the entire data directory.")


def load_source_metadata() -> list[dict]:
    return json.loads(SOURCE_METADATA_PATH.read_text())


def source_prepared_files(metadata: dict) -> list[str]:
    return list(dict.fromkeys([metadata["file"], *metadata.get("prepared_files", [])]))


SOURCE_METADATA = load_source_metadata()
SOURCE_METADATA_BY_KEY = {metadata["key"]: metadata for metadata in SOURCE_METADATA}
SOURCE_FILES = {metadata["key"]: metadata["file"] for metadata in SOURCE_METADATA}
SOURCE_PREPARED_FILES = {metadata["key"]: source_prepared_files(metadata) for metadata in SOURCE_METADATA}


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


@dataclass(frozen=True)
class SourceDataIssue:
    source_key: str
    message: str

    def __str__(self) -> str:
        return f"{self.source_key}: {self.message}"


class SourceDataPreparationMixin:
    data_dir: Path

    def source_dir_for(self, source_key: str) -> Path:
        return self.data_dir / source_key

    def path_for(self, source_key: str) -> Path:
        return self.path_for_file(source_key, SOURCE_FILES[source_key])

    def path_for_file(self, source_key: str, relative: str) -> Path:
        return (self.source_dir_for(source_key) / relative).resolve()

    def paths_for(self, source_key: str) -> list[Path]:
        return [self.path_for_file(source_key, relative) for relative in SOURCE_PREPARED_FILES[source_key]]

    def download_file(self, url: str, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        request = urllib.request.Request(url, headers={"User-Agent": "the-networks-of-war-backend/0.1"})
        context = ssl.create_default_context(cafile=str(SYSTEM_CA_FILE)) if SYSTEM_CA_FILE.exists() else None

        try:
            with urllib.request.urlopen(request, timeout=60, context=context) as response:
                destination.write_bytes(response.read())
        except urllib.error.URLError as exc:
            if not isinstance(exc.reason, ssl.SSLCertVerificationError) or shutil.which("curl") is None:
                raise

            logger.warning("Python SSL verification failed for %s; retrying with curl", url)
            subprocess.run(
                ["curl", "--location", "--fail", "--silent", "--show-error", "--output", str(destination), url],
                check=True,
            )

    def extract_zip(self, archive_path: Path, destination: Path) -> None:
        with tempfile.TemporaryDirectory(prefix="networks-of-war-") as temp_name:
            temp_dir = Path(temp_name)

            with zipfile.ZipFile(archive_path) as archive:
                archive.extractall(temp_dir)

            children = [path for path in temp_dir.iterdir() if path.name != "__MACOSX"]
            extracted_root = children[0] if len(children) == 1 and children[0].is_dir() else temp_dir
            destination.mkdir(parents=True, exist_ok=True)

            for path in extracted_root.iterdir():
                target = destination / path.name
                if target.exists():
                    if target.is_dir():
                        shutil.rmtree(target)
                    else:
                        target.unlink()
                shutil.move(str(path), target)

    def extracted_file_path(self, source_dir: Path, filename: str) -> Path:
        direct_path = source_dir / filename
        if direct_path.exists():
            return direct_path

        matches = sorted(path for path in source_dir.rglob(filename) if path.is_file())
        if not matches:
            raise FileNotFoundError(f"Expected extracted file {filename!r} in {source_dir}")

        return matches[0]

    def convert_excel_to_csv(self, source_path: Path, destination_path: Path) -> None:
        excel_engines = {".xls": "xlrd", ".xlsx": "openpyxl"}
        engine = excel_engines.get(source_path.suffix.lower())

        if engine is None:
            raise ValueError(f"Excel conversion is only supported for .xls/.xlsx files: {source_path}")

        try:
            import pandas as pd
        except ImportError as exc:
            raise RuntimeError("Excel source conversion requires pandas, openpyxl, and xlrd to be installed.") from exc

        logger.info("Converting %s to %s", source_path, destination_path)
        prefix = source_path.read_bytes()[:256].lstrip().lower()
        if prefix.startswith(b"<!doctype html") or prefix.startswith(b"<html"):
            raise RuntimeError(f"Downloaded file looks like HTML, not Excel: {source_path}")

        destination_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            dataframe = pd.read_excel(source_path, sheet_name=0, engine=engine)
        except ImportError as exc:
            raise RuntimeError("Excel source conversion requires pandas, openpyxl, and xlrd to be installed.") from exc
        dataframe.to_csv(destination_path, index=False)

    def populate_source_dir(self, source_key: str) -> None:
        metadata = SOURCE_METADATA_BY_KEY[source_key]

        source_dir = self.source_dir_for(source_key)
        source_dir.mkdir(parents=True, exist_ok=True)
        downloads_dir = self.data_dir / "_downloads"

        for download in metadata["downloads"]:
            if download["kind"] == "zip":
                download_path = downloads_dir / download["filename"]
                logger.info("Downloading %s", download["url"])
                self.download_file(download["url"], download_path)
                self.extract_zip(download_path, source_dir)

                for nested_zip in download.get("nested_zips", []):
                    self.extract_zip(self.extracted_file_path(source_dir, nested_zip), source_dir)
            elif download["kind"] == "file":
                download_path = downloads_dir / download["filename"]
                logger.info("Downloading %s", download["url"])
                self.download_file(download["url"], download_path)
                source_path = source_dir / download["filename"]
                shutil.copy2(download_path, source_path)

                if converted_filename := download.get("converted_filename"):
                    self.convert_excel_to_csv(source_path, source_dir / converted_filename)
            else:
                raise ValueError(f"Unsupported download kind: {download['kind']}")

    def validate_source_dir(self, source_key: str) -> list[SourceDataIssue]:
        metadata = SOURCE_METADATA_BY_KEY[source_key]

        source_dir = self.source_dir_for(source_key)
        found = sorted(path.name for path in source_dir.rglob("*") if path.is_file())
        expected_files = set(SOURCE_PREPARED_FILES[source_key])
        expected_files.update(
            download["filename"] for download in metadata.get("downloads", []) if download["kind"] == "file"
        )
        issues = []

        for expected_file in sorted(expected_files):
            if not any(path.name == expected_file for path in source_dir.rglob("*") if path.is_file()):
                issues.append(
                    SourceDataIssue(
                        source_key,
                        f"expected {expected_file!r} but found: {', '.join(found) if found else '(no files)'}",
                    )
                )

        return issues

    def missing_download_url_issues(self) -> list[SourceDataIssue]:
        return [
            SourceDataIssue(metadata["key"], "source table has no configured download URL")
            for metadata in SOURCE_METADATA
            if not metadata.get("downloads")
        ]

    def prepare_data(self, recreate: bool = False, source_keys: Iterable[str] | None = None) -> None:
        if recreate and self.data_dir.exists():
            logger.info("Recreating data directory: %s", self.data_dir)
            shutil.rmtree(self.data_dir)

        self.data_dir.mkdir(parents=True, exist_ok=True)
        issues = self.missing_download_url_issues()
        source_keys = list(source_keys) if source_keys is not None else list(SOURCE_FILES)

        for source_key in source_keys:
            source_dir = self.source_dir_for(source_key)
            try:
                if recreate or not source_dir.exists():
                    self.populate_source_dir(source_key)
                else:
                    logger.info("Source directory already exists: %s", source_dir)

                source_issues = self.validate_source_dir(source_key)
                if source_issues and not recreate:
                    logger.info("Source directory is incomplete; refreshing: %s", source_dir)
                    self.populate_source_dir(source_key)
                    source_issues = self.validate_source_dir(source_key)
            except Exception as exc:
                issues.append(SourceDataIssue(source_key, str(exc)))
                logger.error("Failed to prepare source directory %s: %s", source_key, exc)
                continue

            issues.extend(source_issues)

        if issues:
            raise RuntimeError("Source data preparation issues:\n" + "\n".join(str(issue) for issue in issues))

    def prepared_path_for(self, source_key: str) -> Path:
        return self.prepared_path_for_file(source_key, SOURCE_FILES[source_key])

    def prepared_path_for_file(self, source_key: str, relative: str) -> Path:
        encoding = SOURCE_METADATA_BY_KEY[source_key].get("encoding")
        source_path = self.path_for_file(source_key, relative)

        if encoding is None or not source_path.exists():
            return source_path

        work_source_dir = WORK_CSV_DIR / source_key
        work_source_dir.mkdir(parents=True, exist_ok=True)
        path = work_source_dir / Path(relative).name
        path.write_text(source_path.read_text(encoding=encoding), encoding="utf-8")

        return path.resolve()

    def sql_context(self) -> dict[str, str]:
        context = {
            "participant_name_replacements_path": sql_literal(PARTICIPANT_NAME_REPLACEMENTS_PATH),
            **{f"{key}_path": sql_literal(self.prepared_path_for(key)) for key in SOURCE_FILES},
        }

        for metadata in SOURCE_METADATA:
            key = metadata["key"]
            context[f"{key}_source_file"] = sql_literal(Path(metadata["file"]).name)
            context[f"{key}_source_version"] = sql_literal(metadata["version"])
            for index, relative in enumerate(SOURCE_PREPARED_FILES[key], start=1):
                context[f"{key}_path_{index}"] = sql_literal(self.prepared_path_for_file(key, relative))

        return context

    def require_inputs(self, source_keys: Iterable[str] | None = None) -> None:
        source_keys = list(source_keys) if source_keys is not None else list(SOURCE_FILES)
        missing = [str(path) for key in source_keys for path in self.paths_for(key) if not path.exists()]

        if missing:
            raise FileNotFoundError("Missing required CSV inputs:\n" + "\n".join(missing))
