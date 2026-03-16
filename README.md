# dlt Facebook Ads → Postgres (Dokploy)

Pipeline de **dlt** que extrae métricas de Facebook Ads y Facebook Insights y las carga en bases de datos PostgreSQL independientes por cliente. Integrado con **Sentry** para monitoreo y desplegable en **Dokploy**.

## Requisitos

- Python 3.9+
- PostgreSQL (por ejemplo en tu Dockploy)
- Cuenta de Facebook con Ads Manager y token de acceso
- (Opcional) Proyecto Sentry para monitoreo

## Instalación en Dokploy

### 1. Crear la aplicación en Dokploy

1. En el panel de Dokploy, crea un nuevo proyecto.
2. Añade una **Application** de tipo **Docker Compose** o **Dockerfile**.
3. Apunta al repositorio o carpeta donde está este proyecto (`etl_facebook`).
4. Configura el **Build**:
   - **Dockerfile**: usa el `Dockerfile` en la raíz del proyecto.
   - O bien usa **Docker Compose** con el `docker-compose.yml`.

### 2. Variables de entorno

Define en Dokploy (Application → Environment Variables):

| Variable | Descripción | Ejemplo |
|----------|-------------|---------|
| `POSTGRES_HOST` | Host de Postgres | `postgres` o IP de tu contenedor Postgres |
| `POSTGRES_PORT` | Puerto | `5432` |
| `POSTGRES_USER` | Usuario | `postgres` |
| `POSTGRES_PASSWORD` | Contraseña | `***` |
| `CLIENT_IDS` | IDs de clientes separados por coma | `cliente_a,cliente_b` |
| `CLIENT_cliente_a_ACCOUNT_ID` | Account ID de Facebook del cliente | `123456789` |
| `CLIENT_cliente_a_ACCESS_TOKEN` | Token de acceso de Facebook | `EAAx...` |
| `CLIENT_cliente_a_DATABASE` | Nombre de la base de datos en Postgres | `fb_cliente_a` |
| `SENTRY_DSN` | DSN de Sentry (opcional) | `https://...@sentry.io/...` |
| `SENTRY_ENVIRONMENT` | Entorno para Sentry | `production` |

Para cada cliente adicional, repite el patrón: `CLIENT_{id}_ACCOUNT_ID`, `CLIENT_{id}_ACCESS_TOKEN`, `CLIENT_{id}_DATABASE`.

**Alternativa:** puedes usar `clients.yaml` (copia `clients.example.yaml` a `clients.yaml`) para definir clientes. En producción se recomienda usar variables de entorno para los tokens.

### 3. Crear bases de datos en Postgres

En tu instancia de Postgres (Dockploy), crea una base de datos por cliente:

```sql
CREATE DATABASE fb_cliente_a;
CREATE DATABASE fb_cliente_b;
```

### 4. Schedule Job (cron) en Dokploy

Para ejecutar el pipeline de forma periódica:

1. Ve a **Schedule Jobs** en Dokploy.
2. Crea un nuevo job de tipo **Application Job** (o **Compose Job** si usas compose).
3. Selecciona el contenedor `dlt-facebook-ads`.
4. **Comando**:
   ```bash
   python facebook_ads_pipeline.py
   ```
5. **Cron**: por ejemplo `0 2 * * *` para ejecutar a las 2:00 cada día.

### 5. Desplegar

1. Guarda la configuración y despliega la aplicación.
2. El contenedor se mantendrá en ejecución (`sleep infinity`).
3. Los Schedule Jobs ejecutarán `python facebook_ads_pipeline.py` en el horario configurado.

## Ejecución manual

Desde tu máquina (con credenciales locales):

```bash
pip install -r requirements.txt
export POSTGRES_HOST=localhost POSTGRES_USER=postgres POSTGRES_PASSWORD=xxx
export CLIENT_IDS=cliente_a
export CLIENT_cliente_a_ACCOUNT_ID=123
export CLIENT_cliente_a_ACCESS_TOKEN=EAAx...
export CLIENT_cliente_a_DATABASE=fb_cliente_a

python facebook_ads_pipeline.py
```

O con argumentos por cliente:

```bash
python facebook_ads_pipeline.py cliente_a 123456789 EAAx... fb_cliente_a
```

### Cargar Ads e Insights por separado

```bash
# Solo Ads (campañas, ad sets, ads, creatives, leads)
python facebook_ads_pipeline.py --ads-only

# Solo Insights (métricas por día)
python facebook_ads_pipeline.py --insights-only
```

Variables de entorno equivalentes: `LOAD_ADS_ONLY=1` o `LOAD_INSIGHTS_ONLY=1`.

### Carga histórica gradual (evitar rate limit)

Para cargas grandes (ej. 365 días), usa lotes pequeños con pausas:

```bash
# Máximo 30 días por ejecución (ejecutar varias veces hasta completar)
INITIAL_LOAD_CHUNK_DAYS=30 python facebook_ads_pipeline.py ...

# Pausa de 60 segundos cada 7 días durante la carga
SLEEP_AFTER_INSIGHTS_DAYS=7 SLEEP_BETWEEN_INSIGHTS_SECONDS=60 python facebook_ads_pipeline.py ...

# Pausa de 2 minutos entre clientes
PAUSE_BETWEEN_CLIENTS_SECONDS=120 python facebook_ads_pipeline.py ...
```

## Credenciales de Facebook

1. **Account ID**: en Ads Manager → Overview del menú de cuentas.
2. **Access Token**: en [developers.facebook.com](https://developers.facebook.com) → tu app → Herramientas → Graph API Explorer. Permisos: `ads_read`, `leads_retrieval` para leads.
3. Usa un **token de larga duración** (60 días). Ver [documentación dlt Facebook Ads](https://dlthub.com/docs/dlt-ecosystem/verified-sources/facebook_ads).

## Looker Studio

Conecta Looker Studio a cada base de datos Postgres de cliente:

1. Nuevo informe → Añadir datos → Conector **PostgreSQL**.
2. Host, puerto, usuario y contraseña de tu Postgres.
3. Base de datos: `fb_cliente_a` (o la del cliente).
4. Tablas generadas por dlt: `campaigns`, `ad_sets`, `ads`, `ad_creatives`, `facebook_insights`, etc.

## Sentry

Si configuras `SENTRY_DSN`, los errores del pipeline se envían a Sentry. Útil para ver fallos y progreso sin interfaz gráfica propia en dlt.

## Observador (estado del pipeline)

Para ver si el pipeline está ejecutándose y el resultado de la última carrera:

```bash
python observer.py              # Estado actual
python observer.py --watch      # Actualiza cada 5 segundos
python observer.py --serve      # Servidor HTTP en puerto 8080 (GET /status devuelve JSON)
python observer.py --json       # Salida en JSON
```

Con `--serve` puedes exponer el puerto y consultar desde fuera: `curl http://tu-servidor:8080/status`

## Estructura del proyecto

```
etl_facebook/
├── facebook_ads/          # Verified source de dlt
├── facebook_ads_pipeline.py
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── .dlt/
│   ├── config.toml.example
│   └── secrets.toml.example
└── README.md
```
