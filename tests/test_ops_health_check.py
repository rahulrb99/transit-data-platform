import json
from types import SimpleNamespace

import pytest

from scripts import ops_health_check


def service(name, *, state="running", health="healthy", exit_code=0):
    return {
        "Service": name,
        "State": state,
        "Health": health,
        "ExitCode": exit_code,
    }


def compose_output(*rows):
    return "\n".join(json.dumps(row) for row in rows)


def test_compose_health_accepts_required_production_services(monkeypatch):
    rows = [service(name) for name in ops_health_check.PRODUCTION_CONTINUOUS_SERVICES]
    rows.extend(service(name, state="exited", health="", exit_code=0)
                for name in ops_health_check.COMPLETED_SERVICES)
    monkeypatch.setattr(
        ops_health_check.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(stdout=compose_output(*rows)),
    )

    ops_health_check.check_compose_services(["docker", "compose"], production=True)


@pytest.mark.parametrize(
    "broken",
    [
        service("producer", state="exited", health="", exit_code=1),
        service("consumer", health="unhealthy"),
        service("migrate", state="exited", health="", exit_code=1),
    ],
)
def test_compose_health_fails_for_broken_required_service(monkeypatch, broken):
    rows = [service(name) for name in ops_health_check.PRODUCTION_CONTINUOUS_SERVICES]
    rows.extend(service(name, state="exited", health="", exit_code=0)
                for name in ops_health_check.COMPLETED_SERVICES)
    rows = [row for row in rows if row["Service"] != broken["Service"]] + [broken]
    monkeypatch.setattr(
        ops_health_check.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(stdout=compose_output(*rows)),
    )

    with pytest.raises(RuntimeError, match=broken["Service"]):
        ops_health_check.check_compose_services(["docker", "compose"], production=True)


def test_compose_health_fails_when_service_is_missing(monkeypatch):
    monkeypatch.setattr(
        ops_health_check.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(stdout="[]"),
    )
    with pytest.raises(RuntimeError, match="missing"):
        ops_health_check.check_compose_services(["docker", "compose"], production=False)
