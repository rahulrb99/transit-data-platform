from scripts.validate_production_environment import validate_values


def valid_values():
    return {
        "APP_ENVIRONMENT": "production",
        "APP_IMAGE": "123456789012.dkr.ecr.us-east-1.amazonaws.com/transit@sha256:"
        + "a" * 64,
        "POSTGRES_HOST": "postgres",
        "POSTGRES_PASSWORD": "a-long-production-password",
        "ARCHIVE_BACKEND": "s3",
        "S3_BUCKET_NAME": "rahul-private-transit-data",
        "AWS_REGION": "us-east-1",
        "S3_RAW_ARCHIVE_PREFIX": "raw-archives",
        "S3_POSTGRES_BACKUP_PREFIX": "postgres-backups",
        "PUBLIC_HOSTNAME": "transit.rahul-data.net",
        "CADDY_ACME_EMAIL": "operations@rahul-data.net",
    }


def test_valid_production_environment_passes_without_aws_credentials():
    assert validate_values(valid_values()) == []


def test_mutable_image_and_placeholder_identity_are_rejected():
    values = valid_values()
    values.update(
        APP_IMAGE="registry.example/transit:latest",
        PUBLIC_HOSTNAME="transit.example.com",
        CADDY_ACME_EMAIL="admin@example.com",
    )
    errors = "\n".join(validate_values(values))
    assert "immutable image digest" in errors
    assert "real DNS hostname" in errors
    assert "real operator email" in errors


def test_missing_s3_configuration_is_rejected():
    values = valid_values()
    values.update(ARCHIVE_BACKEND="local", S3_BUCKET_NAME="", AWS_REGION="")
    errors = "\n".join(validate_values(values))
    assert "ARCHIVE_BACKEND" in errors
    assert "S3_BUCKET_NAME" in errors
    assert "AWS_REGION" in errors


def test_aws_access_keys_are_rejected_even_when_blank_in_file():
    errors = validate_values(valid_values(), {"AWS_ACCESS_KEY_ID"})
    assert any("EC2 role" in error for error in errors)


def test_unsafe_archive_prefix_is_rejected():
    values = valid_values()
    values["S3_RAW_ARCHIVE_PREFIX"] = "../other-data"
    assert any("S3_RAW_ARCHIVE_PREFIX" in error for error in validate_values(values))
