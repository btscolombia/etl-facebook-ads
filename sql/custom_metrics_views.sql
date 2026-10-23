-- Vistas de métricas personalizadas para Facebook Ads/Insights
-- Ejecutar una vez por base de datos después de la primera carga.
-- Schema: facebook_ads_data (dataset_name de dlt)

-- 1. Vista con métricas calculadas que Meta no entrega directamente
CREATE OR REPLACE VIEW facebook_ads_data.v_insights_con_metricas AS
SELECT
    campaign_id,
    adset_id,
    ad_id,
    date_start,
    date_stop,
    reach,
    impressions,
    frequency,
    clicks,
    unique_clicks,
    spend,
    ctr,
    cpc,
    cpm,
    -- Métricas personalizadas no estándar del API
    CASE WHEN NULLIF(impressions, 0) > 0 THEN ROUND((spend::numeric / NULLIF(impressions, 0) * 1000)::numeric, 4) ELSE NULL END AS cpm_calculado,
    CASE WHEN NULLIF(clicks, 0) > 0 THEN ROUND((spend::numeric / NULLIF(clicks, 0))::numeric, 4) ELSE NULL END AS cpc_calculado,
    CASE WHEN NULLIF(reach, 0) > 0 THEN ROUND((spend::numeric / NULLIF(reach, 0))::numeric, 4) ELSE NULL END AS cpp_calculado,
    CASE WHEN NULLIF(impressions, 0) > 0 THEN ROUND((clicks::numeric / NULLIF(impressions, 0) * 100)::numeric, 4) ELSE NULL END AS ctr_porcentaje,
    CASE WHEN NULLIF(clicks, 0) > 0 THEN ROUND((reach::numeric / NULLIF(clicks, 0))::numeric, 2) ELSE NULL END AS clicks_per_reach
FROM facebook_ads_data.facebook_insights;

-- 2. Agregado por campaña y día
CREATE OR REPLACE VIEW facebook_ads_data.v_metricas_por_campana_dia AS
SELECT
    campaign_id,
    date_start,
    SUM(reach)::bigint AS reach_total,
    SUM(impressions)::bigint AS impressions_total,
    SUM(clicks)::bigint AS clicks_total,
    SUM(spend)::numeric AS spend_total,
    ROUND(SUM(spend)::numeric / NULLIF(SUM(impressions), 0) * 1000, 4) AS cpm_promedio,
    ROUND(SUM(spend)::numeric / NULLIF(SUM(clicks), 0), 4) AS cpc_promedio
FROM facebook_ads_data.facebook_insights
WHERE campaign_id IS NOT NULL AND campaign_id::text NOT LIKE 'no_%'
GROUP BY campaign_id, date_start;

-- 3. Resumen de performance diario (para dashboards)
CREATE OR REPLACE VIEW facebook_ads_data.v_resumen_diario AS
SELECT
    date_start,
    SUM(impressions)::bigint AS impressions,
    SUM(clicks)::bigint AS clicks,
    SUM(reach)::bigint AS reach,
    SUM(spend)::numeric AS spend,
    ROUND(SUM(clicks)::numeric / NULLIF(SUM(impressions), 0) * 100, 4) AS ctr_global,
    ROUND(SUM(spend)::numeric / NULLIF(SUM(clicks), 0), 4) AS cpc_global
FROM facebook_ads_data.facebook_insights
GROUP BY date_start;

-- 4. VISTA MAESTRA: Insights + Campañas + Ad Sets + Ads + Nuevas Conversaciones + Leads (todo en uno)
-- Usar esta vista en Looker Studio como fuente principal.
-- Incluye: nombres, métricas, métricas personalizadas, acciones (conversaciones, leads), filtro por fechas.

-- Eliminamos para asegurar que cambios en la estructura no causen error
DROP VIEW IF EXISTS public.v_metricas_completas CASCADE;
DROP VIEW IF EXISTS facebook_ads_data.v_metricas_completas CASCADE;

CREATE VIEW facebook_ads_data.v_metricas_completas AS
WITH conv AS (
    SELECT i.campaign_id, i.adset_id, i.ad_id, i.date_start, SUM((act.value)::bigint)::bigint AS nuevas_conversaciones
    FROM facebook_ads_data.facebook_insights i
    JOIN facebook_ads_data.facebook_insights__actions act ON act._dlt_parent_id = i._dlt_id
    WHERE act.action_type = 'onsite_conversion.messaging_conversation_started_7d'
    GROUP BY i.campaign_id, i.adset_id, i.ad_id, i.date_start
),
lea AS (
    SELECT i.campaign_id, i.adset_id, i.ad_id, i.date_start, SUM((act.value)::bigint)::bigint AS leads
    FROM facebook_ads_data.facebook_insights i
    JOIN facebook_ads_data.facebook_insights__actions act ON act._dlt_parent_id = i._dlt_id
    WHERE act.action_type IN ('lead', 'onsite_web_lead', 'onsite_conversion.lead', 'contact', 'submit_application', 'schedule')
    GROUP BY i.campaign_id, i.adset_id, i.ad_id, i.date_start
),
pur AS (
    SELECT i.campaign_id, i.adset_id, i.ad_id, i.date_start, SUM((act.value)::bigint)::bigint AS purchases
    FROM facebook_ads_data.facebook_insights i
    JOIN facebook_ads_data.facebook_insights__actions act ON act._dlt_parent_id = i._dlt_id
    WHERE act.action_type IN ('purchase', 'onsite_conversion.purchase')
    GROUP BY i.campaign_id, i.adset_id, i.ad_id, i.date_start
),
vid AS (
    SELECT i.campaign_id, i.adset_id, i.ad_id, i.date_start, SUM((act.value)::bigint)::bigint AS video_views
    FROM facebook_ads_data.facebook_insights i
    JOIN facebook_ads_data.facebook_insights__actions act ON act._dlt_parent_id = i._dlt_id
    WHERE act.action_type = 'video_view'
    GROUP BY i.campaign_id, i.adset_id, i.ad_id, i.date_start
)
SELECT
    -- IDs como texto y fechas como date: Looker los detecta solos (sin sumar IDs ni mostrar hora)
    i.account_id::text AS account_id,
    i.campaign_id::text AS campaign_id,
    COALESCE(c.name, 'Campaña ' || i.campaign_id::text) AS campaign_name,
    c.objective AS campaign_objective,
    c.status AS campaign_status,
    i.adset_id::text AS adset_id,
    COALESCE(s.name, 'AdSet ' || i.adset_id::text) AS adset_name,
    s.status AS adset_status,
    i.ad_id::text AS ad_id,
    COALESCE(a.name, 'Ad ' || i.ad_id::text) AS ad_name,
    a.status AS ad_status,
    (i.date_start AT TIME ZONE 'UTC')::date AS date_start,
    (i.date_stop AT TIME ZONE 'UTC')::date AS date_stop,
    i.reach, i.impressions, i.frequency, i.clicks, i.unique_clicks,
    i.spend, i.ctr, i.cpc, i.cpm, i.cpp,
    CASE WHEN NULLIF(i.impressions, 0) > 0 THEN ROUND((i.spend::numeric / NULLIF(i.impressions, 0) * 1000)::numeric, 4) ELSE NULL END AS cpm_calculado,
    CASE WHEN NULLIF(i.clicks, 0) > 0 THEN ROUND((i.spend::numeric / NULLIF(i.clicks, 0))::numeric, 4) ELSE NULL END AS cpc_calculado,
    CASE WHEN NULLIF(i.reach, 0) > 0 THEN ROUND((i.spend::numeric / NULLIF(i.reach, 0))::numeric, 4) ELSE NULL END AS cpp_calculado,
    CASE WHEN NULLIF(i.impressions, 0) > 0 THEN ROUND((i.clicks::numeric / NULLIF(i.impressions, 0) * 100)::numeric, 4) ELSE NULL END AS ctr_porcentaje,
    CASE WHEN NULLIF(i.impressions, 0) > 0 THEN ROUND((i.reach::numeric / NULLIF(i.impressions, 0))::numeric, 2) ELSE NULL END AS reach_per_impression,
    COALESCE(conv.nuevas_conversaciones, 0)::bigint AS nuevas_conversaciones,
    COALESCE(lea.leads, 0)::bigint AS leads,
    COALESCE(pur.purchases, 0)::bigint AS compras,
    COALESCE(vid.video_views, 0)::bigint AS video_views,
    CASE
        WHEN c.objective = 'OUTCOME_LEADS' THEN COALESCE(lea.leads, 0)::bigint
        WHEN c.objective = 'OUTCOME_SALES' THEN COALESCE(pur.purchases, 0)::bigint
        WHEN c.objective IN ('OUTCOME_ENGAGEMENT', 'OUTCOME_AWARENESS') THEN GREATEST(COALESCE(conv.nuevas_conversaciones, 0), COALESCE(vid.video_views, 0), COALESCE(lea.leads, 0))::bigint
        ELSE GREATEST(COALESCE(conv.nuevas_conversaciones, 0), COALESCE(lea.leads, 0), COALESCE(vid.video_views, 0), COALESCE(pur.purchases, 0))::bigint
    END AS resultado,
    CASE
        WHEN c.objective = 'OUTCOME_LEADS' THEN 'Leads'
        WHEN c.objective = 'OUTCOME_SALES' THEN 'Ventas'
        WHEN c.objective = 'OUTCOME_TRAFFIC' THEN 'Clics'
        ELSE 
            CASE 
                WHEN COALESCE(conv.nuevas_conversaciones, 0) >= GREATEST(COALESCE(lea.leads, 0), COALESCE(pur.purchases, 0), 1) THEN 'Conversaciones'
                WHEN COALESCE(lea.leads, 0) >= GREATEST(COALESCE(conv.nuevas_conversaciones, 0), COALESCE(pur.purchases, 0), 1) THEN 'Leads'
                WHEN COALESCE(pur.purchases, 0) >= GREATEST(COALESCE(conv.nuevas_conversaciones, 0), COALESCE(lea.leads, 0), 1) THEN 'Ventas'
                WHEN COALESCE(vid.video_views, 0) > 0 THEN 'Video views'
                ELSE 'Otros'
            END
    END AS resultado_tipo,
    CASE 
        WHEN (
            CASE
                WHEN c.objective = 'OUTCOME_LEADS' THEN COALESCE(lea.leads, 0)::bigint
                WHEN c.objective = 'OUTCOME_SALES' THEN COALESCE(pur.purchases, 0)::bigint
                WHEN c.objective IN ('OUTCOME_ENGAGEMENT', 'OUTCOME_AWARENESS') THEN GREATEST(COALESCE(conv.nuevas_conversaciones, 0), COALESCE(vid.video_views, 0), COALESCE(lea.leads, 0))::bigint
                ELSE GREATEST(COALESCE(conv.nuevas_conversaciones, 0), COALESCE(lea.leads, 0), COALESCE(vid.video_views, 0), COALESCE(pur.purchases, 0))::bigint
            END
        ) > 0 THEN 
            ROUND((i.spend::numeric / (
                CASE
                    WHEN c.objective = 'OUTCOME_LEADS' THEN COALESCE(lea.leads, 0)
                    WHEN c.objective = 'OUTCOME_SALES' THEN COALESCE(pur.purchases, 0)
                    WHEN c.objective IN ('OUTCOME_ENGAGEMENT', 'OUTCOME_AWARENESS') THEN GREATEST(COALESCE(conv.nuevas_conversaciones, 0), COALESCE(vid.video_views, 0), COALESCE(lea.leads, 0))
                    ELSE GREATEST(COALESCE(conv.nuevas_conversaciones, 0), COALESCE(lea.leads, 0), COALESCE(vid.video_views, 0), COALESCE(pur.purchases, 0))
                END
            )::numeric)::numeric, 4)
        ELSE NULL
    END AS costo_por_resultado
FROM facebook_ads_data.facebook_insights i
LEFT JOIN facebook_ads_data.campaigns c ON c.id::text = i.campaign_id::text
LEFT JOIN facebook_ads_data.ad_sets s ON s.id::text = i.adset_id::text
LEFT JOIN facebook_ads_data.ads a ON a.id::text = i.ad_id::text
LEFT JOIN conv ON conv.campaign_id::text = i.campaign_id::text AND conv.adset_id::text = i.adset_id::text AND conv.ad_id::text = i.ad_id::text AND conv.date_start = i.date_start
LEFT JOIN lea ON lea.campaign_id::text = i.campaign_id::text AND lea.adset_id::text = i.adset_id::text AND lea.ad_id::text = i.ad_id::text AND lea.date_start = i.date_start
LEFT JOIN pur ON pur.campaign_id::text = i.campaign_id::text AND pur.adset_id::text = i.adset_id::text AND pur.ad_id::text = i.ad_id::text AND pur.date_start = i.date_start
LEFT JOIN vid ON vid.campaign_id::text = i.campaign_id::text AND vid.adset_id::text = i.adset_id::text AND vid.ad_id::text = i.ad_id::text AND vid.date_start = i.date_start;

-- Vista pública para Looker Studio
CREATE OR REPLACE VIEW public.v_metricas_completas AS SELECT * FROM facebook_ads_data.v_metricas_completas;
