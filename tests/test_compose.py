"""Validate the resolved Compose contract without requiring a Docker daemon."""

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def compose():
    if not shutil.which("docker"):
        pytest.skip("Docker Compose CLI is not installed")
    result = subprocess.run(
        [
            "docker", "compose", "--env-file", ".env.example", "--profile", "tools",
            "config", "--format", "json",
        ],
        cwd=ROOT,
        env={
            **os.environ,
            "POSTGRES_PASSWORD": "compose-contract-test-only",
            "POSTGRES_HOST": "host-setting-must-not-be-used",
            "KAFKA_BOOTSTRAP_SERVERS": "host-setting-must-not-be-used:19092",
        },
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    )
    return json.loads(result.stdout)


@pytest.mark.parametrize("name", ["producer", "consumer", "dashboard"])
def test_shared_app_image_and_internal_networking(compose, name):
    service = compose["services"][name]
    assert service["image"] == compose["services"]["dashboard"]["image"]
    assert service["build"] == compose["services"]["dashboard"]["build"]
    assert service["environment"]["POSTGRES_HOST"] == "postgres"
    assert str(service["environment"]["POSTGRES_PORT"]) == "5432"
    assert service["environment"]["KAFKA_BOOTSTRAP_SERVERS"] == "redpanda:9092"
    assert service["environment"]["POSTGRES_PASSWORD"] == "compose-contract-test-only"
    assert service["init"] is True


@pytest.mark.parametrize("name", ["postgres", "redpanda", "producer", "consumer", "dashboard"])
def test_continuous_services_have_restart_and_health_policies(compose, name):
    service = compose["services"][name]
    assert service["restart"] == "unless-stopped"
    assert service["healthcheck"]["test"]
    assert service["stop_grace_period"]
    assert service["logging"]["options"]["max-size"] == "10m"
    assert service["logging"]["options"]["max-file"] == "3"


@pytest.mark.parametrize("name", ["producer", "consumer"])
def test_streaming_services_wait_for_ready_dependencies(compose, name):
    service = compose["services"][name]
    assert service["command"] == ["python", "-m", f"ingestion.realtime.{name}"]
    assert service["depends_on"]["postgres"]["condition"] == "service_healthy"
    assert service["depends_on"]["redpanda"]["condition"] == "service_healthy"
    assert service["depends_on"]["redpanda-init"]["condition"] == "service_completed_successfully"
    assert not service.get("ports")


def test_durable_state_uses_named_volumes(compose):
    for service_name, volume_name, target in [
        ("postgres", "postgres_data", "/var/lib/postgresql/data"),
        ("redpanda", "redpanda_data", "/var/lib/redpanda/data"),
        ("producer", "raw_data", "/app/data/raw"),
        ("producer", "raw_archive", "/app/data/archive/raw"),
    ]:
        mounts = compose["services"][service_name]["volumes"]
        assert any(
            mount["type"] == "volume"
            and mount["source"] == volume_name
            and mount["target"] == target
            for mount in mounts
        )
        assert volume_name in compose["volumes"]


def test_runtime_and_infrastructure_images_are_pinned():
    dockerfile = (ROOT / "dashboard" / "Dockerfile").read_text(encoding="utf-8")
    assert dockerfile.splitlines()[0].startswith("FROM python:3.11-slim@sha256:")
    assert "-r requirements-runtime.lock" in dockerfile
    assert "--no-deps ." in dockerfile
    assert "pip uninstall --yes pip setuptools wheel" in dockerfile

    compose_source = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    production_source = (ROOT / "docker-compose.prod.yml").read_text(encoding="utf-8")
    for image in ("postgres:16", "redpandadata/redpanda:v24.3.4",
                  "redpandadata/console:v2.8.3"):
        assert f"{image}@sha256:" in compose_source
    assert "caddy:2.8@sha256:" in production_source


def test_redpanda_topic_has_explicit_retention_bounds(compose):
    service = compose["services"]["redpanda-init"]
    assert service["environment"]["REDPANDA_TOPIC_RETENTION_MS"] == "604800000"
    assert service["environment"]["REDPANDA_TOPIC_RETENTION_BYTES"] == "1073741824"
    command = " ".join(service["command"])
    assert "retention.ms" in command
    assert "retention.bytes" in command


def test_host_ports_are_loopback_only_and_console_is_optional(compose):
    for service in compose["services"].values():
        for port in service.get("ports", []):
            assert port["host_ip"] == "127.0.0.1"
            assert port["target"] != 9644
    assert compose["services"]["redpanda-console"]["profiles"] == ["tools"]


def test_compose_refuses_an_empty_database_password():
    if not shutil.which("docker"):
        pytest.skip("Docker Compose CLI is not installed")
    result = subprocess.run(
        ["docker", "compose", "--env-file", ".env.example", "config", "--quiet"],
        cwd=ROOT,
        env={**os.environ, "POSTGRES_PASSWORD": ""},
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode != 0
    assert "Set POSTGRES_PASSWORD" in result.stderr


def test_production_removes_inherited_ports_and_enforces_environment():
    if not shutil.which("docker"):
        pytest.skip("Docker Compose CLI is not installed")
    result = subprocess.run(
        ["docker", "compose", "--env-file", ".env.prod.example", "-f", "docker-compose.yml",
         "-f", "docker-compose.prod.yml", "--profile", "tools", "--profile", "maintenance",
         "config", "--format", "json"], cwd=ROOT,
        env={**os.environ, "POSTGRES_PASSWORD": "contract-only-secret",
             "APP_IMAGE": "registry.example/transit@sha256:" + "a" * 64,
             "S3_BUCKET_NAME": "compose-contract-test",
             "AWS_REGION": "us-east-1"},
        capture_output=True, text=True, timeout=30, check=True,
    )
    services = json.loads(result.stdout)["services"]
    for name, service in services.items():
        if name != "caddy":
            assert not service.get("ports"), name
    for name in ("producer", "consumer", "dashboard", "migrate", "retention"):
        assert services[name]["environment"]["APP_ENVIRONMENT"] == "production"
        assert services[name]["environment"]["ARCHIVE_BACKEND"] == "s3"
        assert services[name]["environment"]["AWS_REGION"] == "us-east-1"
        assert services[name]["environment"]["S3_BUCKET_NAME"] == "compose-contract-test"
    for name in ("producer", "consumer", "dashboard"):
        assert services[name]["depends_on"]["migrate"]["condition"] == "service_completed_successfully"
    redpanda_command = " ".join(services["redpanda"]["command"])
    assert "localhost" not in redpanda_command
    assert "dev-container" not in redpanda_command
    assert services["caddy"]["healthcheck"]["test"]
    assert services["caddy"]["logging"]["options"]["max-size"] == "10m"


def test_production_requires_an_explicit_release_image():
    if not shutil.which("docker"):
        pytest.skip("Docker Compose CLI is not installed")
    environment = {
        **os.environ,
        "POSTGRES_PASSWORD": "contract-only-secret",
        "APP_IMAGE": "",
        "S3_BUCKET_NAME": "compose-contract-test",
        "AWS_REGION": "us-east-1",
    }
    result = subprocess.run(
        ["docker", "compose", "--env-file", ".env.prod.example", "-f", "docker-compose.yml",
         "-f", "docker-compose.prod.yml", "config", "--quiet"], cwd=ROOT,
        env=environment, capture_output=True, text=True, timeout=30, check=False,
    )
    assert result.returncode != 0
    assert "APP_IMAGE" in result.stderr


def test_production_requires_s3_bucket_and_region_during_render():
    if not shutil.which("docker"):
        pytest.skip("Docker Compose CLI is not installed")
    environment = {
        **os.environ,
        "POSTGRES_PASSWORD": "contract-only-secret",
        "APP_IMAGE": "registry.example/transit@sha256:" + "a" * 64,
        "S3_BUCKET_NAME": "",
        "AWS_REGION": "",
    }
    result = subprocess.run(
        ["docker", "compose", "--env-file", ".env.prod.example", "-f", "docker-compose.yml",
         "-f", "docker-compose.prod.yml", "config", "--quiet"], cwd=ROOT,
        env=environment, capture_output=True, text=True, timeout=30, check=False,
    )
    assert result.returncode != 0
    assert "S3_BUCKET_NAME" in result.stderr or "AWS_REGION" in result.stderr
