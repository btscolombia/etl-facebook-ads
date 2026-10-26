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
),
lnk AS (
    SELECT i.campaign_id, i.adset_id, i.ad_id, i.date_start, SUM((act.value)::bigint)::bigint AS link_clicks
    FROM facebook_ads_data.facebook_insights i
    JOIN facebook_ads_data.facebook_insights__actions act ON act._dlt_parent_id = i._dlt_id
    WHERE act.action_type = 'link_click'
    GROUP BY i.campaign_id, i.adset_id, i.ad_id, i.date_start
),
eng AS (
    SELECT i.campaign_id, i.adset_id, i.ad_id, i.date_start, SUM((act.value)::bigint)::bigint AS post_engagement
    FROM facebook_ads_data.facebook_insights i
    JOIN facebook_ads_data.facebook_insights__actions act ON act._dlt_parent_id = i._dlt_id
    WHERE act.action_type = 'post_engagement'
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
    n.conv_n AS nuevas_conversaciones,
    n.leads_n AS leads,
    n.pur_n AS compras,
    n.vid_n AS video_views,
    n.lnk_n AS clics_enlace,
    n.eng_n AS interacciones,
    r.valor AS resultado,
    t.tipo AS resultado_tipo,
    ROUND((i.spend::numeric / NULLIF(r.valor, 0))::numeric, 4) AS costo_por_resultado
FROM facebook_ads_data.facebook_insights i
LEFT JOIN facebook_ads_data.campaigns c ON c.id::text = i.campaign_id::text
LEFT JOIN facebook_ads_data.ad_sets s ON s.id::text = i.adset_id::text
LEFT JOIN facebook_ads_data.ads a ON a.id::text = i.ad_id::text
LEFT JOIN conv ON conv.campaign_id::text = i.campaign_id::text AND conv.adset_id::text = i.adset_id::text AND conv.ad_id::text = i.ad_id::text AND conv.date_start = i.date_start
LEFT JOIN lea ON lea.campaign_id::text = i.campaign_id::text AND lea.adset_id::text = i.adset_id::text AND lea.ad_id::text = i.ad_id::text AND lea.date_start = i.date_start
LEFT JOIN pur ON pur.campaign_id::text = i.campaign_id::text AND pur.adset_id::text = i.adset_id::text AND pur.ad_id::text = i.ad_id::text AND pur.date_start = i.date_start
LEFT JOIN vid ON vid.campaign_id::text = i.campaign_id::text AND vid.adset_id::text = i.adset_id::text AND vid.ad_id::text = i.ad_id::text AND vid.date_start = i.date_start
LEFT JOIN lnk ON lnk.campaign_id::text = i.campaign_id::text AND lnk.adset_id::text = i.adset_id::text AND lnk.ad_id::text = i.ad_id::text AND lnk.date_start = i.date_start
LEFT JOIN eng ON eng.campaign_id::text = i.campaign_id::text AND eng.adset_id::text = i.adset_id::text AND eng.ad_id::text = i.ad_id::text AND eng.date_start = i.date_start
CROSS JOIN LATERAL (
    SELECT
        COALESCE(conv.nuevas_conversaciones, 0)::bigint AS conv_n,
        COALESCE(lea.leads, 0)::bigint AS leads_n,
        COALESCE(pur.purchases, 0)::bigint AS pur_n,
        COALESCE(vid.video_views, 0)::bigint AS vid_n,
        COALESCE(lnk.link_clicks, 0)::bigint AS lnk_n,
        COALESCE(eng.post_engagement, 0)::bigint AS eng_n
) n
-- Resultado = lo que optimiza el conjunto de anuncios (como "Resultados" en el Administrador de anuncios).
-- Si no hay optimization_goal reconocido, se usa el objetivo de la campaña.
CROSS JOIN LATERAL (
    SELECT CASE
        WHEN s.optimization_goal = 'CONVERSATIONS' THEN 'Conversaciones'
        WHEN s.optimization_goal IN ('LEAD_GENERATION', 'QUALITY_LEAD') THEN 'Leads'
        WHEN s.optimization_goal IN ('OFFSITE_CONVERSIONS', 'VALUE') AND c.objective = 'OUTCOME_SALES' THEN 'Ventas'
        WHEN s.optimization_goal IN ('OFFSITE_CONVERSIONS', 'VALUE') AND c.objective = 'OUTCOME_LEADS' THEN 'Leads'
        WHEN s.optimization_goal IN ('THRUPLAY', 'VIDEO_VIEWS') THEN 'Video views'
        WHEN s.optimization_goal IN ('LINK_CLICKS', 'LANDING_PAGE_VIEWS') THEN 'Clics en enlace'
        WHEN s.optimization_goal IN ('REACH', 'IMPRESSIONS', 'AD_RECALL_LIFT') THEN 'Alcance'
        -- Visitas al perfil no vienen en Insights: se reportan las interacciones con la publicación
        WHEN s.optimization_goal IN ('PROFILE_AND_PAGE_ENGAGEMENT', 'POST_ENGAGEMENT', 'PAGE_LIKES', 'PROFILE_VISIT') THEN 'Interacciones'
        WHEN c.objective = 'OUTCOME_LEADS' THEN 'Leads'
        WHEN c.objective = 'OUTCOME_SALES' THEN 'Ventas'
        WHEN c.objective = 'OUTCOME_TRAFFIC' THEN 'Clics en enlace'
        WHEN c.objective = 'OUTCOME_AWARENESS' THEN 'Alcance'
        WHEN n.conv_n > 0 THEN 'Conversaciones'
        WHEN n.leads_n > 0 THEN 'Leads'
        WHEN n.vid_n > 0 THEN 'Video views'
        ELSE 'Otros'
    END AS tipo
) t
CROSS JOIN LATERAL (
    SELECT (CASE t.tipo
        WHEN 'Conversaciones' THEN n.conv_n
        WHEN 'Leads' THEN n.leads_n
        WHEN 'Ventas' THEN n.pur_n
        WHEN 'Video views' THEN n.vid_n
        WHEN 'Clics en enlace' THEN n.lnk_n
        WHEN 'Alcance' THEN i.reach
        WHEN 'Interacciones' THEN n.eng_n
        ELSE 0
    END)::bigint AS valor
) r;

-- Vista pública para Looker Studio
CREATE OR REPLACE VIEW public.v_metricas_completas AS SELECT * FROM facebook_ads_data.v_metricas_completas;
