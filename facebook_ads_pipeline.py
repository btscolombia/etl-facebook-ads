"""
Pipeline dlt para Facebook Ads e Insights.
Carga datos por cliente en bases de datos Postgres independientes.
Integrado con Sentry para monitoreo.
"""
import os
import sys
from pathlib import Path

import yaml
import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration
import psycopg2

import dlt
from facebook_ads import facebook_ads_source, facebook_insights_source


def init_sentry():
    """Inicializa Sentry si está configurado el DSN."""
    dsn = os.environ.get("SENTRY_DSN")
    if dsn:
        sentry_sdk.init(
            dsn=dsn,
            environment=os.environ.get("SENTRY_ENVIRONMENT", "production"),
            traces_sample_rate=0.2,
            integrations=[
                LoggingIntegration(level=None, event_level=None),
            ],
            # Identificar el pipeline en Sentry
            release=os.environ.get("SENTRY_RELEASE", "dlt-facebook-ads@1.0"),
        )


def get_postgres_credentials():
    """Obtiene credenciales de Postgres desde variables de entorno."""
    return {
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": int(os.environ.get("POSTGRES_PORT", "5432")),
        "username": os.environ.get("POSTGRES_USER", "postgres"),
        "password": os.environ.get("POSTGRES_PASSWORD", ""),
        "database": os.environ.get("POSTGRES_DATABASE", "facebook_ads"),
    }


def get_initial_load_days(client_id: str) -> int:
    """Días históricos a cargar. Desde env: CLIENT_X_INITIAL_LOAD_DAYS o INITIAL_LOAD_PAST_DAYS."""
    prefix = f"CLIENT_{client_id.upper().replace('-', '_')}_"
    return int(os.environ.get(f"{prefix}INITIAL_LOAD_DAYS")
               or os.environ.get("INITIAL_LOAD_PAST_DAYS", "365"))


def run_pipeline_for_client(
    client_id: str,
    account_id: str,
    access_token: str,
    database_name: str,
    load_ads: bool = True,
    load_insights: bool = True,
    initial_load_past_days: int = None,
):
    """
    Ejecuta el pipeline dlt para un cliente específico.
    Los datos se cargan en la base de datos indicada.
    """
    creds = get_postgres_credentials()
    creds["database"] = database_name

    credentials = (
        f"postgresql://{creds['username']}:{creds['password']}"
        f"@{creds['host']}:{creds['port']}/{creds['database']}"
    )

    pipeline_name = f"facebook_ads_{client_id}"
    dataset_name = "facebook_ads_data"

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=dlt.destinations.postgres(credentials),
        dataset_name=dataset_name,
    )

    if load_ads:
        ads_data = facebook_ads_source(
            account_id=account_id,
            access_token=access_token,
        )
        load_info = pipeline.run(ads_data)
        print(f"Cliente {client_id} - Ads: {load_info}")

    if load_insights:
        days = initial_load_past_days or get_initial_load_days(client_id)
        print(f"Cliente {client_id} - Cargando Insights (histórico: {days} días, incremental en siguientes ejecuciones)")
        insights_data = facebook_insights_source(
            account_id=account_id,
            access_token=access_token,
            initial_load_past_days=days,
        )
        load_info = pipeline.run(insights_data)
        print(f"Cliente {client_id} - Insights: {load_info}")

    # Aplicar vistas de métricas personalizadas tras cargar Insights
    if load_insights and os.environ.get("SKIP_CUSTOM_VIEWS") != "1":
        apply_custom_metrics_views(database_name, creds)


def apply_custom_metrics_views(database_name: str, creds: dict):
    """Crea/actualiza vistas de métricas personalizadas en Postgres."""
    sql_path = Path(__file__).parent / "sql" / "custom_metrics_views.sql"
    if not sql_path.exists():
        print("  (sql/custom_metrics_views.sql no encontrado, omitiendo vistas)")
        return
    sql = sql_path.read_text()
    conn = None
    try:
        conn = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=database_name,
            user=creds["username"],
            password=creds["password"],
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)
        print(f"  Vistas de métricas personalizadas aplicadas en {database_name}")
    except Exception as e:
        print(f"  Advertencia: no se pudieron crear vistas personalizadas: {e}")
    finally:
        if conn:
            conn.close()


def load_clients_from_yaml() -> list:
    """Carga clientes desde clients.yaml si existe."""
    yaml_path = Path(__file__).parent / "clients.yaml"
    if not yaml_path.exists():
        return []

    with open(yaml_path) as f:
        data = yaml.safe_load(f)
    if not data or "clients" not in data:
        return []

    clients = []
    for c in data["clients"]:
        if c.get("account_id") and c.get("access_token"):
            clients.append({
                "id": c.get("id", "unknown"),
                "account_id": c["account_id"],
                "access_token": c["access_token"],
                "database": c.get("database", f"fb_{c.get('id', 'unknown')}"),
            })
    return clients


def load_clients_from_env():
    """
    Carga la lista de clientes desde variables de entorno.
    Formato: CLIENT_IDS=cliente_a,cliente_b
            CLIENT_cliente_a_ACCOUNT_ID=123
            CLIENT_cliente_a_ACCESS_TOKEN=xxx
            CLIENT_cliente_a_DATABASE=fb_cliente_a
    """
    client_ids_str = os.environ.get("CLIENT_IDS", "")
    if not client_ids_str:
        return []

    client_ids = [c.strip() for c in client_ids_str.split(",") if c.strip()]
    clients = []
    for cid in client_ids:
        prefix = f"CLIENT_{cid.upper().replace('-', '_')}_"
        account_id = os.environ.get(f"{prefix}ACCOUNT_ID")
        access_token = os.environ.get(f"{prefix}ACCESS_TOKEN")
        database = os.environ.get(f"{prefix}DATABASE", f"fb_{cid}")

        if account_id and access_token:
            clients.append({
                "id": cid,
                "account_id": account_id,
                "access_token": access_token,
                "database": database,
            })
    return clients


def verify_sentry():
    """Provoca un error intencional para verificar que Sentry recibe eventos."""
    print("Verificando Sentry: provocando error intencional...")
    division_by_zero = 1 / 0  # noqa: F841
    print(division_by_zero)  # nunca se ejecuta


def main():
    init_sentry()

    # Modo verificación: python facebook_ads_pipeline.py --verify-sentry
    if "--verify-sentry" in sys.argv or os.environ.get("SENTRY_VERIFY") == "1":
        verify_sentry()
        return

    # Cron Monitoring: registrar inicio en Sentry
    cron_slug = os.environ.get("SENTRY_CRON_SLUG", "dlt-facebook-ads")
    check_in_id = None
    import time
    start_time = time.time()
    if os.environ.get("SENTRY_DSN"):
        try:
            check_in_id = sentry_sdk.capture_checkin(
                slug=cron_slug,
                status="in_progress",
            )
        except Exception:
            pass

    # Modo: argumentos CLI > clients.yaml > env
    if len(sys.argv) >= 4:
        client_id = sys.argv[1]
        account_id = sys.argv[2]
        access_token = sys.argv[3]
        database = sys.argv[4] if len(sys.argv) > 4 else f"fb_{client_id}"
        clients = [{"id": client_id, "account_id": account_id, "access_token": access_token, "database": database}]
    else:
        clients = load_clients_from_yaml() or load_clients_from_env()

    if not clients:
        print(
            "Uso: python facebook_ads_pipeline.py <client_id> <account_id> <access_token> [database]\n"
            "O configure CLIENT_IDS y variables CLIENT_*_ACCOUNT_ID, CLIENT_*_ACCESS_TOKEN, CLIENT_*_DATABASE"
        )
        sys.exit(1)

    for client in clients:
        try:
            run_pipeline_for_client(
                client_id=client["id"],
                account_id=client["account_id"],
                access_token=client["access_token"],
                database_name=client["database"],
            )
        except Exception as e:
            sentry_sdk.capture_exception(e)
            print(f"Error para cliente {client['id']}: {e}")
            raise


if __name__ == "__main__":
    main()
