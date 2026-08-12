from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch

from src.integrations.sisc_heartbeat import build_payload, send_heartbeat


def test_builds_validated_observatory_payload():
    report = {
        "status": "EXITO",
        "fases_completadas": ["looker", "normalizacion", "consolidacion"],
        "looker_result": {"requests_count": 170},
        "coverage": {"total_records": 425},
    }
    analysis = {
        "current_year": "2026",
        "previous_year": "2025",
        "cutoff_date": date(2026, 6, 30),
        "indicator_count": 12,
        "indicators_with_data": 10,
    }
    with (
        patch("src.integrations.sisc_heartbeat._load_json", return_value=report),
        patch("src.integrations.sisc_heartbeat._analyze_source", return_value=analysis),
    ):
        payload = build_payload(
            Path("pipeline_report.json"),
            Path("master.csv"),
            data_changed=True,
            now=datetime(2026, 8, 12, 12, 0, tzinfo=timezone.utc),
        )

    assert payload["status"] == "CURRENT"
    assert payload["quality_status"] == "VALIDATED"
    assert payload["source_cutoff_date"] == "2026-06-30"
    assert payload["record_count"] == 425
    assert payload["indicator_count"] == 12
    assert payload["last_change_detected_at"] == "2026-08-12T12:00:00Z"


def test_failed_workflow_preserves_failure_semantics():
    with patch("src.integrations.sisc_heartbeat._load_json", return_value=None):
        payload = build_payload(
            Path("missing.json"),
            Path("missing.csv"),
            outcome="failure",
            now=datetime(2026, 8, 12, 12, 0, tzinfo=timezone.utc),
        )

    assert payload["status"] == "ERROR"
    assert payload["quality_status"] == "ERROR"
    assert "last_success_at" not in payload


def test_missing_secret_is_a_soft_failure():
    assert send_heartbeat({"status": "CURRENT"}, service_key="", oidc_token="") is False


def test_oidc_identity_is_sent_as_bearer_token():
    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    with patch("src.integrations.sisc_heartbeat.urlopen", return_value=Response()) as mocked_urlopen:
        sent = send_heartbeat(
            {"status": "CURRENT"},
            api_url="https://example.test/api",
            service_key="",
            oidc_token="short-lived-token",
        )

    request = mocked_urlopen.call_args.args[0]
    assert sent is True
    assert request.get_header("Authorization") == "Bearer short-lived-token"
    assert request.get_header("X-sisc-source-key") is None
