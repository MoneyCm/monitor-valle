"""Reporta el estado del Observatorio del Valle al Centro de fuentes SISC."""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


BASE_DIR = Path(__file__).resolve().parents[2]
DEFAULT_API_URL = "https://sisc-backend.onrender.com/api"
DEFAULT_REPORT_PATH = BASE_DIR / "data" / "final" / "pipeline_report.json"
DEFAULT_CSV_PATH = BASE_DIR / "data" / "final" / "jamundi_analytics_master.csv"


def _utc_now(now: Optional[datetime] = None) -> datetime:
    value = now or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).replace(microsecond=0)


def _iso_datetime(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _parse_date(value: Any) -> Optional[date]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(value).strip(), fmt).date()
        except ValueError:
            continue
    return None


def _load_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def _analyze_source(csv_path: Path) -> Dict[str, Any]:
    from src.core.config import settings
    from src.reporting.data_analyzer import DataAnalyzer

    analyzer = DataAnalyzer(csv_path, municipio=settings.obs_municipio)
    frame = analyzer.load_data()
    analyzer.detect_years(frame)
    analyzer.detect_corte_month(frame)
    indicators = analyzer.extract_indicadores(frame)
    return {
        "current_year": str(analyzer.current_year),
        "previous_year": str(analyzer.prev_year),
        "cutoff_date": _parse_date(analyzer.latest_date_str),
        "indicator_count": len(indicators),
        "indicators_with_data": sum(
            1 for item in indicators if int(item.get("current") or 0) > 0
        ),
    }


def _as_non_negative_int(value: Any) -> Optional[int]:
    try:
        parsed = int(value)
        return parsed if parsed >= 0 else None
    except (TypeError, ValueError):
        return None


def build_payload(
    report_path: Path = DEFAULT_REPORT_PATH,
    csv_path: Path = DEFAULT_CSV_PATH,
    *,
    outcome: str = "success",
    data_changed: bool = False,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    checked_at = _utc_now(now)
    normalized_outcome = (outcome or "success").strip().lower()
    workflow_ok = normalized_outcome == "success"
    report = _load_json(Path(report_path)) or {}
    pipeline_status = str(report.get("status") or "SIN REPORTE").strip().upper()
    pipeline_ok = pipeline_status in {"EXITO", "EXITOSO", "OK", "SUCCESS"}

    warnings = []
    analysis: Dict[str, Any] = {}
    analysis_error = None
    if workflow_ok and pipeline_ok:
        try:
            analysis = _analyze_source(Path(csv_path))
        except Exception as error:  # El heartbeat no debe bloquear la extraccion.
            analysis_error = str(error)[:350]

    if not workflow_ok:
        warnings.append("La ejecucion del monitor del Observatorio termino con error.")
    if not report:
        warnings.append("No se encontro el reporte estructurado del pipeline.")
    elif not pipeline_ok:
        warnings.append(f"El pipeline informo el estado {pipeline_status}.")
    if analysis_error:
        warnings.append(f"No se pudo calcular el corte estadistico: {analysis_error}")

    cutoff = analysis.get("cutoff_date")
    indicator_count = _as_non_negative_int(analysis.get("indicator_count")) or 0
    indicators_with_data = _as_non_negative_int(analysis.get("indicators_with_data")) or 0
    if workflow_ok and pipeline_ok and not cutoff:
        warnings.append("La extraccion termino, pero no fue posible establecer la fecha de corte.")
    if workflow_ok and pipeline_ok and indicator_count and not indicators_with_data:
        warnings.append("Los indicadores analizados no contienen valores para el periodo actual.")

    if not workflow_ok or (report and not pipeline_ok):
        status, quality = "ERROR", "ERROR"
    elif not report or not analysis:
        status, quality = "NEEDS_REVIEW", "INCOMPLETE"
    elif not cutoff or not indicators_with_data:
        status, quality = "NEEDS_REVIEW", "WARNING"
    else:
        status, quality = "CURRENT", "VALIDATED"

    coverage = report.get("coverage") if isinstance(report.get("coverage"), dict) else {}
    looker = report.get("looker_result") if isinstance(report.get("looker_result"), dict) else {}
    record_count = _as_non_negative_int(coverage.get("total_records"))
    request_count = _as_non_negative_int(looker.get("requests_count"))
    phases = report.get("fases_completadas")
    phases_count = len(phases) if isinstance(phases, list) else 0

    payload: Dict[str, Any] = {
        "connector_code": "OBSERVATORIO_VALLE",
        "status": status,
        "quality_status": quality,
        "last_checked_at": _iso_datetime(checked_at),
        "indicator_count": indicator_count,
        "warnings": [warning[:500] for warning in warnings[:30]],
        "details": {
            "workflow": os.getenv("GITHUB_WORKFLOW", "monitor-valle"),
            "run_id": os.getenv("GITHUB_RUN_ID"),
            "outcome": normalized_outcome,
            "pipeline_status": pipeline_status,
            "completed_phases": phases_count,
            "requests": request_count,
            "indicators_with_data": indicators_with_data,
            "data_changed": bool(data_changed),
            "current_year": analysis.get("current_year"),
            "previous_year": analysis.get("previous_year"),
        },
    }
    if cutoff:
        payload["source_cutoff_date"] = cutoff.isoformat()
        payload["period_label"] = f"Corte al {cutoff.isoformat()} - {indicator_count} indicadores"
    elif indicator_count:
        payload["period_label"] = f"{indicator_count} indicadores revisados"
    if record_count is not None:
        payload["record_count"] = record_count
    if workflow_ok and pipeline_ok:
        payload["last_success_at"] = _iso_datetime(checked_at)
    if workflow_ok and pipeline_ok and data_changed:
        payload["last_change_detected_at"] = _iso_datetime(checked_at)
    return payload


def _heartbeat_url(api_url: str) -> str:
    base = api_url.strip().rstrip("/")
    if base.endswith("/source-center/heartbeat"):
        return base
    return f"{base}/source-center/heartbeat"


def _request_github_oidc_token(audience: str = "sisc-source-center") -> Optional[str]:
    request_url = os.getenv("ACTIONS_ID_TOKEN_REQUEST_URL", "").strip()
    request_token = os.getenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "").strip()
    if not request_url or not request_token:
        return None
    separator = "&" if "?" in request_url else "?"
    oidc_request = Request(
        f"{request_url}{separator}{urlencode({'audience': audience})}",
        headers={"Authorization": f"Bearer {request_token}"},
    )
    try:
        with urlopen(oidc_request, timeout=10) as response:
            result = json.loads(response.read().decode("utf-8"))
        token = result.get("value") if isinstance(result, dict) else None
        return str(token) if token else None
    except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError) as error:
        print(f"[AVISO] No se pudo obtener la identidad OIDC de GitHub: {error}.")
        return None


def send_heartbeat(
    payload: Dict[str, Any],
    *,
    api_url: Optional[str] = None,
    service_key: Optional[str] = None,
    oidc_token: Optional[str] = None,
    timeout: int = 20,
) -> bool:
    token = _request_github_oidc_token() if oidc_token is None else oidc_token.strip()
    key = (service_key if service_key is not None else os.getenv("SISC_SOURCE_MONITOR_KEY", "")).strip()
    if not token and not key:
        print("[AVISO] Heartbeat SISC omitido: no hay identidad OIDC ni clave de servicio.")
        return False

    endpoint = _heartbeat_url(api_url or os.getenv("SISC_API_URL", DEFAULT_API_URL))
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "monitor-valle/1.0",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    else:
        headers["X-SISC-SOURCE-KEY"] = key
    request = Request(
        endpoint,
        data=json.dumps(payload, ensure_ascii=True).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            accepted = 200 <= response.status < 300
        print(f"[INFO] Heartbeat SISC enviado ({payload['status']}).")
        return accepted
    except HTTPError as error:
        print(f"[AVISO] El API SISC rechazo el heartbeat (HTTP {error.code}).")
    except (URLError, TimeoutError, OSError) as error:
        print(f"[AVISO] No se pudo enviar el heartbeat SISC: {error}.")
    return False


def main() -> int:
    changed = os.getenv("DATA_CHANGED", "false").strip().lower() == "true"
    payload = build_payload(
        outcome=os.getenv("SISC_MONITOR_OUTCOME", "success"),
        data_changed=changed,
    )
    print(
        "[INFO] Estado para Centro de fuentes: "
        f"{payload['status']} / {payload['quality_status']} / "
        f"{payload['indicator_count']} indicadores."
    )
    return 0 if send_heartbeat(payload) else 1


if __name__ == "__main__":
    raise SystemExit(main())
