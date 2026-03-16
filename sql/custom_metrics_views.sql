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
