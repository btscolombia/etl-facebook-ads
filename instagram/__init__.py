"""Carga estadísticas orgánicas de Instagram (cuenta, perfil, publicaciones, historias y audiencia)."""

import datetime as dt
from typing import Iterator, Sequence
from zoneinfo import ZoneInfo

import dlt
from dlt.common.typing import TDataItems
from dlt.sources import DltResource

from .helpers import InstagramApi, breakdown_values, metric_value

# Meta cierra los días de Instagram a medianoche hora del Pacífico (end_time 07:00/08:00 UTC)
META_TZ = ZoneInfo("America/Los_Angeles")
# Meta solo entrega 2 años de métricas de cuenta
MAX_HISTORY_DAYS = 729
# Días que se vuelven a pedir en cada ejecución porque Meta los sigue ajustando
REPROCESS_DAYS = 3

ACCOUNT_METRICS = {
    "reach": "alcance",
    "views": "visualizaciones",
    "accounts_engaged": "cuentas_interactuaron",
    "total_interactions": "interacciones",
    "likes": "likes",
    "comments": "comentarios",
    "saves": "guardados",
    "shares": "compartidos",
    "replies": "respuestas",
    "reposts": "reposteos",
    "profile_links_taps": "toques_enlaces_perfil",
}
FOLLOW_TYPE_METRICS = ("reach", "views", "follows_and_unfollows")

MEDIA_FIELDS = "id,caption,media_type,media_product_type,timestamp,permalink,thumbnail_url,media_url,like_count,comments_count"
MEDIA_METRICS = {
    "reach": "alcance",
    "views": "visualizaciones",
    "saved": "guardados",
    "shares": "compartidos",
    "total_interactions": "interacciones",
    "reposts": "reposteos",
    "follows": "seguidores_ganados",
    "profile_visits": "visitas_perfil",
    "ig_reels_avg_watch_time": "reels_tiempo_promedio_ms",
    "ig_reels_video_view_total_time": "reels_tiempo_total_ms",
}
# Métricas que Meta solo da para reels o solo para publicaciones del feed
REELS_ONLY = {"ig_reels_avg_watch_time", "ig_reels_video_view_total_time"}
FEED_ONLY = {"follows", "profile_visits"}

STORY_FIELDS = "id,media_type,timestamp,permalink,thumbnail_url,media_url"
STORY_METRICS = {
    "reach": "alcance",
    "views": "visualizaciones",
    "replies": "respuestas",
    "shares": "compartidos",
    "total_interactions": "interacciones",
    "follows": "seguidores_ganados",
    "profile_visits": "visitas_perfil",
}
STORY_NAVIGATION = {
    "tap_forward": "toques_adelante",
    "tap_back": "toques_atras",
    "tap_exit": "salidas",
    "swipe_forward": "deslizamientos",
}

AUDIENCE_BREAKDOWNS = {"edad": "age", "genero": "gender", "ciudad": "city", "pais": "country"}


def _columns(**types: str) -> dict:
    return {name: {"data_type": data_type} for name, data_type in types.items()}


def _bigints(*names: str) -> dict:
    return _columns(**{n: "bigint" for n in names})


# Tipos explícitos: dlt crea así todas las columnas aunque Meta no mande una métrica, y las vistas no fallan
ACCOUNT_COLUMNS = {
    **_columns(fecha="date"),
    **_bigints(*ACCOUNT_METRICS.values(), "seguidores_nuevos", "dejaron_de_seguir"),
    **_bigints(*(f"{ACCOUNT_METRICS[m]}_{s}" for m in ("reach", "views") for s in ("seguidores", "no_seguidores"))),
}
PROFILE_COLUMNS = {**_columns(fecha="date", usuario="text"), **_bigints("seguidores", "seguidos", "publicaciones")}
MEDIA_COLUMNS = {
    **_columns(
        id="text", fecha="date", publicado_en="timestamp", tipo="text", producto="text",
        texto="text", enlace="text", miniatura="text", actualizado_en="timestamp",
    ),
    **_bigints("likes", "comentarios", *MEDIA_METRICS.values()),
}
STORY_COLUMNS = {
    **_columns(
        id="text", fecha="date", publicada_en="timestamp", tipo="text", enlace="text", miniatura="text", leida_en="timestamp"
    ),
    **_bigints(*STORY_METRICS.values(), *STORY_NAVIGATION.values()),
}
AUDIENCE_COLUMNS = {**_columns(fecha="date", tipo="text", valor="text"), **_bigints("seguidores")}


def _parse_timestamp(value: str) -> dt.datetime:
    return dt.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S%z")


@dlt.source(name="instagram")
def instagram_source(
    ig_user_id: str = dlt.config.value,
    access_token: str = dlt.secrets.value,
    initial_load_past_days: int = 90,
    media_refresh_days: int = 30,
    full_media_refresh: bool = False,
    report_timezone: str = "America/Bogota",
    api_version: str = "v26.0",
) -> Sequence[DltResource]:
    """Recursos de Instagram. Seleccionar con .with_resources() según la frecuencia de cada uno."""

    api = InstagramApi(access_token, api_version)
    report_tz = ZoneInfo(report_timezone)

    @dlt.resource(primary_key="fecha", write_disposition="merge", columns=ACCOUNT_COLUMNS)
    def cuenta_diario() -> Iterator[TDataItems]:
        state = dlt.current.resource_state()
        today = dt.datetime.now(META_TZ).date()
        if state.get("ultimo_dia"):
            start = dt.date.fromisoformat(state["ultimo_dia"]) - dt.timedelta(days=REPROCESS_DAYS)
        else:
            start = today - dt.timedelta(days=initial_load_past_days)
        start = max(start, today - dt.timedelta(days=MAX_HISTORY_DAYS))

        day = start
        total_days = (today - start).days
        while day < today:
            print(f"Instagram cuenta: día {(day - start).days + 1}/{total_days} ({day})", flush=True)
            since = dt.datetime.combine(day, dt.time(), META_TZ)
            params = {
                "period": "day",
                "metric_type": "total_value",
                "since": int(since.timestamp()),
                "until": int((since + dt.timedelta(days=1)).timestamp()),
            }
            row = {"fecha": day}
            for name, item in api.insights_safe(ig_user_id, list(ACCOUNT_METRICS), **params).items():
                row[ACCOUNT_METRICS[name]] = metric_value(item)
            by_follow_type = api.insights_safe(ig_user_id, FOLLOW_TYPE_METRICS, breakdown="follow_type", **params)
            for name, item in by_follow_type.items():
                values = breakdown_values(item)
                if name == "follows_and_unfollows":
                    # En esta métrica FOLLOWER = empezó a seguir y NON_FOLLOWER = dejó de seguir
                    row["seguidores_nuevos"] = values.get("FOLLOWER", 0)
                    row["dejaron_de_seguir"] = values.get("NON_FOLLOWER", 0)
                else:
                    label = ACCOUNT_METRICS[name]
                    row[f"{label}_seguidores"] = values.get("FOLLOWER", 0)
                    row[f"{label}_no_seguidores"] = values.get("NON_FOLLOWER", 0)
            yield row
            state["ultimo_dia"] = day.isoformat()
            day += dt.timedelta(days=1)

    @dlt.resource(primary_key="fecha", write_disposition="merge", columns=PROFILE_COLUMNS)
    def perfil_diario() -> Iterator[TDataItems]:
        # Meta no guarda historial del total de seguidores: se toma una foto por día
        profile = api.get(ig_user_id, fields="username,followers_count,follows_count,media_count")
        yield {
            "fecha": dt.datetime.now(report_tz).date(),
            "usuario": profile.get("username"),
            "seguidores": profile.get("followers_count"),
            "seguidos": profile.get("follows_count"),
            "publicaciones": profile.get("media_count"),
        }

    @dlt.resource(primary_key="id", write_disposition="merge", columns=MEDIA_COLUMNS)
    def publicaciones() -> Iterator[TDataItems]:
        state = dlt.current.resource_state()
        loaded = state.setdefault("con_estadisticas", {})
        refresh_since = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=media_refresh_days)
        now = dt.datetime.now(dt.timezone.utc)

        for media in api.paginate(f"{ig_user_id}/media", fields=MEDIA_FIELDS, limit=100):
            published = _parse_timestamp(media["timestamp"])
            # Solo se emiten las que se refrescan: un merge sin estadísticas las dejaría en NULL
            if not (full_media_refresh or media["id"] not in loaded or published >= refresh_since):
                continue
            is_reel = media.get("media_product_type") == "REELS"
            metrics = [m for m in MEDIA_METRICS if m not in (FEED_ONLY if is_reel else REELS_ONLY)]
            row = {
                "id": media["id"],
                "fecha": published.astimezone(report_tz).date(),
                "publicado_en": published,
                "tipo": media.get("media_type"),
                "producto": media.get("media_product_type"),
                "texto": media.get("caption"),
                "enlace": media.get("permalink"),
                "miniatura": media.get("thumbnail_url") or media.get("media_url"),
                "likes": media.get("like_count"),
                "comentarios": media.get("comments_count"),
                "actualizado_en": now,
            }
            for name, item in api.insights_safe(media["id"], metrics).items():
                row[MEDIA_METRICS[name]] = metric_value(item)
            loaded[media["id"]] = now.date().isoformat()
            yield row

    @dlt.resource(primary_key="id", write_disposition="merge", columns=STORY_COLUMNS)
    def historias() -> Iterator[TDataItems]:
        # Meta solo da estadísticas de historias mientras están publicadas (24 h): se leen cada pocas horas
        now = dt.datetime.now(dt.timezone.utc)
        for story in api.paginate(f"{ig_user_id}/stories", fields=STORY_FIELDS):
            published = _parse_timestamp(story["timestamp"])
            row = {
                "id": story["id"],
                "fecha": published.astimezone(report_tz).date(),
                "publicada_en": published,
                "tipo": story.get("media_type"),
                "enlace": story.get("permalink"),
                "miniatura": story.get("thumbnail_url") or story.get("media_url"),
                "leida_en": now,
            }
            for name, item in api.insights_safe(story["id"], list(STORY_METRICS)).items():
                row[STORY_METRICS[name]] = metric_value(item)
            navigation = api.insights_safe(story["id"], ["navigation"], breakdown="story_navigation_action_type")
            values = breakdown_values(navigation["navigation"]) if "navigation" in navigation else {}
            for action, label in STORY_NAVIGATION.items():
                row[label] = values.get(action, 0)
            yield row

    @dlt.resource(primary_key=("fecha", "tipo", "valor"), write_disposition="merge", columns=AUDIENCE_COLUMNS)
    def audiencia() -> Iterator[TDataItems]:
        today = dt.datetime.now(report_tz).date()
        for kind, breakdown in AUDIENCE_BREAKDOWNS.items():
            data = api.insights_safe(
                ig_user_id, ["follower_demographics"], period="lifetime", metric_type="total_value", breakdown=breakdown
            )
            values = breakdown_values(data["follower_demographics"]) if "follower_demographics" in data else {}
            for value, followers in values.items():
                yield {"fecha": today, "tipo": kind, "valor": value, "seguidores": followers}

    return cuenta_diario, perfil_diario, publicaciones, historias, audiencia
