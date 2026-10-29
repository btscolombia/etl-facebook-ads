"""
Pipeline dlt para Instagram (estadísticas orgánicas).
Carga en la misma base de datos de cada cliente, esquema instagram_data.

Modos:
  python instagram_pipeline.py              # Diario: cuenta, perfil, publicaciones recientes e historias
  python instagram_pipeline.py --historias  # Cada 4 h: solo historias (Meta las borra a las 24 h)
  python instagram_pipeline.py --semanal    # Semanal: todas las publicaciones y la audiencia

Clientes (mismas variables que el pipeline de anuncios, más el ID de Instagram):
  CLIENT_IDS=cliente_a
  CLIENT_CLIENTE_A_ACCESS_TOKEN=...   (con instagram_basic e instagram_manage_insights)
  CLIENT_CLIENTE_A_DATABASE=...
  CLIENT_CLIENTE_A_IG_USER_ID=1784...  (si falta, el cliente se omite)
"""
import os
import sys
import time

import dlt
import sentry_sdk

from facebook_ads_pipeline import apply_custom_metrics_views, get_postgres_credentials, init_sentry, postgres_url
from instagram import instagram_source

DAILY_RESOURCES = ["cuenta_diario", "perfil_diario", "publicaciones", "historias"]
STORIES_RESOURCES = ["historias"]
WEEKLY_RESOURCES = ["publicaciones", "audiencia"]


def load_instagram_clients_from_env() -> list:
    clients = []
    for cid in [c.strip() for c in os.environ.get("CLIENT_IDS", "").split(",") if c.strip()]:
        prefix = f"CLIENT_{cid.upper().replace('-', '_')}_"
        ig_user_id = os.environ.get(f"{prefix}IG_USER_ID")
        access_token = os.environ.get(f"{prefix}IG_ACCESS_TOKEN") or os.environ.get(f"{prefix}ACCESS_TOKEN")
        if not ig_user_id or not access_token:
            print(f"Cliente {cid}: sin {prefix}IG_USER_ID o token, se omite Instagram")
            continue
        clients.append({
            "id": cid,
            "ig_user_id": ig_user_id,
            "access_token": access_token,
            "database": os.environ.get(f"{prefix}DATABASE", f"fb_{cid}"),
        })
    return clients


def run_instagram_for_client(client: dict, resources: list, full_media_refresh: bool):
    creds = get_postgres_credentials()
    creds["database"] = client["database"]
    pipeline = dlt.pipeline(
        pipeline_name=f"instagram_{client['id']}",
        destination=dlt.destinations.postgres(postgres_url(creds)),
        dataset_name="instagram_data",
    )
    source = instagram_source(
        ig_user_id=client["ig_user_id"],
        access_token=client["access_token"],
        initial_load_past_days=int(os.environ.get("IG_INITIAL_LOAD_DAYS", "90")),
        media_refresh_days=int(os.environ.get("IG_MEDIA_REFRESH_DAYS", "30")),
        full_media_refresh=full_media_refresh,
        report_timezone=os.environ.get("REPORT_TIMEZONE", "America/Bogota"),
        api_version=os.environ.get("IG_API_VERSION", "v26.0"),
    ).with_resources(*resources)
    load_info = pipeline.run(source)
    print(f"Cliente {client['id']} - Instagram ({', '.join(resources)}): {load_info}")
    if os.environ.get("SKIP_CUSTOM_VIEWS") != "1":
        apply_custom_metrics_views(client["database"], creds, sql_file="instagram_views.sql")


def main():
    init_sentry()
    if "--historias" in sys.argv:
        resources, full_media_refresh = STORIES_RESOURCES, False
    elif "--semanal" in sys.argv:
        resources, full_media_refresh = WEEKLY_RESOURCES, True
    else:
        resources, full_media_refresh = DAILY_RESOURCES, False

    clients = load_instagram_clients_from_env()
    if not clients:
        print(__doc__)
        sys.exit(1)

    pause_clients = int(os.environ.get("PAUSE_BETWEEN_CLIENTS_SECONDS", "0"))
    try:
        for i, client in enumerate(clients):
            if i > 0 and pause_clients > 0:
                print(f"Pausa {pause_clients}s antes del cliente {client['id']}...")
                time.sleep(pause_clients)
            run_instagram_for_client(client, resources, full_media_refresh)
    except Exception as e:
        sentry_sdk.capture_exception(e)
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()
