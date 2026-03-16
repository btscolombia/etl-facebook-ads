#!/usr/bin/env python3
"""
Verifica el estado de sincronización en una base de datos de cliente.
Uso: python verify_sync.py [database_name]
     O: CLIENT_DATABASE=altiore_group_db python verify_sync.py

Necesita: POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD
"""
import os
import sys

import psycopg2
from psycopg2.extras import RealDictCursor


def get_creds():
    return {
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": int(os.environ.get("POSTGRES_PORT", "5432")),
        "user": os.environ.get("POSTGRES_USER", "postgres"),
        "password": os.environ.get("POSTGRES_PASSWORD", ""),
    }


def verify_sync(database: str):
    creds = get_creds()
    schema = "facebook_ads_data"

    try:
        conn = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=database,
            user=creds["user"],
            password=creds["password"],
        )
    except Exception as e:
        print(f"Error conectando a {database}: {e}")
        print("Asegúrate de tener POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD")
        sys.exit(1)

    print(f"\n=== Estado de sincronización: {database} ===\n")

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # 1. ¿Existe el schema?
        cur.execute(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s",
            (schema,),
        )
        if not cur.fetchone():
            print(f"❌ El schema '{schema}' no existe. La carga aún no ha corrido.")
            conn.close()
            return

        # 2. Tablas en el schema
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """,
            (schema,),
        )
        tables = [r["table_name"] for r in cur.fetchall()]
        if not tables:
            print(f"❌ No hay tablas en '{schema}'.")
            conn.close()
            return

        print("Tablas cargadas:")
        for t in tables:
            cur.execute(
                f'SELECT COUNT(*) AS n FROM "{schema}"."{t}"',
            )
            n = cur.fetchone()["n"]
            print(f"  - {t}: {n:,} filas")

        # 3. Rango de fechas en facebook_insights (el más importante)
        if "facebook_insights" in tables:
            cur.execute(
                f"""
                SELECT
                    MIN(date_start) AS fecha_min,
                    MAX(date_start) AS fecha_max,
                    COUNT(DISTINCT date_start) AS dias_unicos,
                    COUNT(*) AS total_registros
                FROM "{schema}".facebook_insights
                """
            )
            row = cur.fetchone()
            if row and row["total_registros"]:
                print(f"\nInsights (rango de fechas):")
                print(f"  - Primer día: {row['fecha_min']}")
                print(f"  - Último día:  {row['fecha_max']}")
                print(f"  - Días con datos: {row['dias_unicos']}")
                print(f"  - Total registros: {row['total_registros']:,}")
                if row["fecha_max"]:
                    from datetime import date
                    hoy = date.today()
                    ultimo = row["fecha_max"] if isinstance(row["fecha_max"], str) else row["fecha_max"]
                    if hasattr(ultimo, "date"):
                        ultimo = ultimo.date() if hasattr(ultimo, "date") else ultimo
                    diff = (hoy - ultimo).days if hasattr(ultimo, "__sub__") else "?"
                    print(f"  - Días hasta hoy: {diff} (0 = al día)")
            else:
                print("\n⚠️  tabla facebook_insights vacía.")

        # 4. Vistas custom
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.views
            WHERE table_schema = %s
            ORDER BY table_name
            """,
            (schema,),
        )
        views = [r["table_name"] for r in cur.fetchall()]
        if views:
            print(f"\nVistas de métricas: {', '.join(views)}")
        else:
            print("\n⚠️  No hay vistas de métricas (custom_metrics_views.sql no aplicado)")

    conn.close()
    print("\n✅ Verificación completada.\n")


if __name__ == "__main__":
    db = (
        os.environ.get("CLIENT_DATABASE")
        or (sys.argv[1] if len(sys.argv) > 1 else None)
        or "altiore_group_db"
    )
    verify_sync(db)
