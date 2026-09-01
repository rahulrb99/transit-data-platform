"""Validate production configuration without contacting AWS or other services."""
from __future__ import annotations

import argparse
import os
import re
import stat
from pathlib import Path

from dotenv import dotenv_values
from pydantic import ValidationError

from ingestion.config import Settings
from ingestion.s3_archive import normalize_prefix

IMAGE_DIGEST_PATTERN = re.compile(r"^[^\s]+@sha256:[0-9a-f]{64}$")
AWS_REGION_PATTERN = re.compile(r"^[a-z]{2}(?:-gov)?-[a-z]+-\d$")
BUCKET_PATTERN = re.compile(r"^(?=.{3,63}$)(?!\d+\.\d+\.\d+\.\d+$)[a-z0-9][a-z0-9.-]*[a-z0-9]$")
HOSTNAME_PATTERN = re.compile(
    r"^(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+"
    r"[a-z](?:[a-z0-9-]{0,61}[a-z0-9])?$"
)
PROHIBITED_CREDENTIAL_KEYS = {
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
}
COMPOSE_KEYS = {
    "APP_IMAGE",
    "PUBLIC_HOSTNAME",
    "CADDY_ACME_EMAIL",
}


def load_effective_values(path: Path) -> tuple[dict[str, str], set[str]]:
    file_values = {
        key: value or "" for key, value in dotenv_values(path).items() if key is not None
    }
    setting_keys = {name.upper() for name in Settings.model_fields}
    keys = setting_keys | COMPOSE_KEYS | PROHIBITED_CREDENTIAL_KEYS
    effective = {
        key: os.environ.get(key, file_values.get(key, "")).strip() for key in keys
    }
    return effective, set(file_values)


def validate_values(values: dict[str, str], file_keys: set[str] | None = None) -> list[str]:
    errors: list[str] = []
    if values.get("APP_ENVIRONMENT") != "production":
        errors.append("APP_ENVIRONMENT must be production")

    payload = {
        name: values[name.upper()]
        for name in Settings.model_fields
        if values.get(name.upper(), "") != ""
    }
    try:
        settings = Settings(_env_file=None, **payload)
    except ValidationError as error:
        settings = None
        for issue in error.errors(include_input=False, include_url=False):
            location = ".".join(str(part) for part in issue["loc"])
            errors.append(f"{location}: {issue['msg']}")

    image = values.get("APP_IMAGE", "")
    if not IMAGE_DIGEST_PATTERN.fullmatch(image):
        errors.append("APP_IMAGE must use an immutable image digest ending @sha256:<64 hex>")

    hostname = values.get("PUBLIC_HOSTNAME", "").lower()
    reserved_suffixes = (".example.com", ".example.org", ".example.net", ".invalid")
    if not HOSTNAME_PATTERN.fullmatch(hostname) or hostname.endswith(reserved_suffixes):
        errors.append("PUBLIC_HOSTNAME must be a real DNS hostname, not a placeholder")

    email = values.get("CADDY_ACME_EMAIL", "").lower()
    if (
        email.count("@") != 1
        or email.endswith(("@example.com", "@example.org", "@example.net"))
        or any(character.isspace() for character in email)
    ):
        errors.append("CADDY_ACME_EMAIL must be a real operator email address")

    region = values.get("AWS_REGION", "")
    if not AWS_REGION_PATTERN.fullmatch(region):
        errors.append("AWS_REGION is missing or invalid")
    bucket = values.get("S3_BUCKET_NAME", "")
    if not BUCKET_PATTERN.fullmatch(bucket):
        errors.append("S3_BUCKET_NAME is missing or is not a valid bucket name")

    for key in ("S3_RAW_ARCHIVE_PREFIX", "S3_POSTGRES_BACKUP_PREFIX"):
        try:
            normalize_prefix(values.get(key, ""))
        except ValueError as error:
            errors.append(f"{key}: {error}")

    present_keys = file_keys or set()
    for key in sorted(PROHIBITED_CREDENTIAL_KEYS):
        if key in present_keys or values.get(key):
            errors.append(
                f"{key} must not be stored in production configuration; use the EC2 role"
            )

    if settings is not None and settings.archive_backend != "s3":
        errors.append("ARCHIVE_BACKEND must be s3")
    return errors


def validate_file_permissions(path: Path) -> list[str]:
    if os.name == "nt":
        return []
    mode = stat.S_IMODE(path.stat().st_mode)
    if mode & 0o077:
        return [f"{path} must not be readable or writable by group/other; run chmod 600"]
    return []


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=Path(".env.prod"))
    args = parser.parse_args()
    if not args.env_file.is_file():
        raise SystemExit(f"Production environment file not found: {args.env_file}")
    values, file_keys = load_effective_values(args.env_file)
    errors = validate_values(values, file_keys) + validate_file_permissions(args.env_file)
    if errors:
        print("Production configuration is invalid:")
        for error in errors:
            print(f"- {error}")
        raise SystemExit(1)
    print(
        "Production configuration is valid: immutable image digest, S3 archive, "
        "EC2-role credentials, DNS, and secret-file permissions are enforced."
    )


if __name__ == "__main__":
    main()
