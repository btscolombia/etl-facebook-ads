"""Cliente mínimo de la API Graph de Instagram (inicio de sesión con Facebook)."""

import json
import re
import time
from typing import Any, Dict, Iterator, List, Sequence

from dlt.sources.helpers.requests import Client

# Límite de uso de Meta: esperar y reintentar (4 = app, 17 = usuario, 32/613 = página, 80002 = Instagram)
RATE_LIMIT_CODES = (4, 17, 32, 613, 80002)
RATE_LIMIT_WAITS_SECONDS = (60, 180, 300, 600)
# Porcentaje de cupo (x-business-use-case-usage) a partir del cual se pausa antes de que Meta frene
USAGE_PAUSE_PERCENT = 85
# Métrica no soportada para ese objeto (100) o sin suficientes datos para mostrarla (10)
NOT_AVAILABLE_CODES = (10, 100)


class InstagramApiError(Exception):
    def __init__(self, code: int, message: str):
        super().__init__(f"({code}) {message}")
        self.code = code
        self.message = message


class InstagramApi:
    def __init__(self, access_token: str, api_version: str, request_timeout: float = 60.0):
        self.base_url = f"https://graph.facebook.com/{api_version}"
        self.access_token = access_token
        self.session = Client(request_timeout=request_timeout, raise_for_status=False).session

    def _pause_if_near_limit(self, headers: Any) -> None:
        raw = headers.get("x-business-use-case-usage") or headers.get("x-app-usage")
        if not raw:
            return
        try:
            usage = json.loads(raw)
        except ValueError:
            return
        entries = [e for v in usage.values() for e in v] if isinstance(usage, dict) and "call_count" not in usage else [usage]
        for e in entries:
            pct = max(e.get("call_count", 0), e.get("total_cputime", 0), e.get("total_time", 0))
            if pct >= USAGE_PAUSE_PERCENT:
                wait = max(60, int(e.get("estimated_time_to_regain_access") or 5) * 60)
                print(f"Instagram: cupo de la API al {pct}%, pausa de {wait}s", flush=True)
                time.sleep(wait)
                return

    def get(self, path_or_url: str, **params: Any) -> Dict[str, Any]:
        url = path_or_url if path_or_url.startswith("http") else f"{self.base_url}/{path_or_url}"
        if not path_or_url.startswith("http"):
            params["access_token"] = self.access_token
        for attempt, wait in enumerate((*RATE_LIMIT_WAITS_SECONDS, None), start=1):
            response = self.session.get(url, params=params)
            self._pause_if_near_limit(response.headers)
            data = response.json()
            error = data.get("error") if isinstance(data, dict) else None
            if not error:
                return data
            code = error.get("code")
            if code in RATE_LIMIT_CODES and wait is not None:
                print(
                    f"Límite de la API de Instagram (código {code}): esperando {wait}s "
                    f"antes de reintentar ({attempt}/{len(RATE_LIMIT_WAITS_SECONDS)})",
                    flush=True,
                )
                time.sleep(wait)
                continue
            raise InstagramApiError(code, error.get("message", ""))

    def paginate(self, path: str, **params: Any) -> Iterator[Dict[str, Any]]:
        page = self.get(path, **params)
        while True:
            yield from page.get("data", [])
            next_url = page.get("paging", {}).get("next")
            if not next_url:
                return
            page = self.get(next_url)

    def insights(self, object_id: str, metrics: Sequence[str], **params: Any) -> List[Dict[str, Any]]:
        return self.get(f"{object_id}/insights", metric=",".join(metrics), **params).get("data", [])

    def insights_safe(self, object_id: str, metrics: Sequence[str], **params: Any) -> Dict[str, Dict[str, Any]]:
        """Como insights(), pero quita las métricas que Meta no soporta para ese objeto en vez de fallar."""
        pending = list(metrics)
        while pending:
            try:
                return {d["name"]: d for d in self.insights(object_id, pending, **params)}
            except InstagramApiError as e:
                if e.code not in NOT_AVAILABLE_CODES:
                    raise
                unsupported = re.search(r"support the (\w+) metric", e.message)
                if not unsupported or unsupported.group(1) not in pending:
                    # Sin estadísticas para este objeto (p. ej. publicado antes de ser cuenta profesional)
                    return {}
                pending.remove(unsupported.group(1))
        return {}


def metric_value(item: Dict[str, Any]) -> Any:
    """Valor de una métrica, venga como total_value o como values[0]."""
    if "total_value" in item:
        return item["total_value"].get("value")
    values = item.get("values") or [{}]
    return values[0].get("value")


def breakdown_values(item: Dict[str, Any]) -> Dict[str, Any]:
    """{valor_de_dimensión: valor} del primer breakdown de una métrica."""
    return {
        r["dimension_values"][0]: r.get("value")
        for b in item.get("total_value", {}).get("breakdowns", [])[:1]
        for r in b.get("results", [])
    }
