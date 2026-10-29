-- Vistas de Instagram para Looker Studio (esquema instagram_data creado por instagram_pipeline.py).
-- Cada vista se crea solo si su tabla ya existe: una carga de solo historias no rompe las demás.
-- Proporciones (tasa de interacción, % de seguidores...) van como campos calculados en Looker, no aquí.

-- 1. Diario de la cuenta + foto diaria del perfil (total de seguidores)
DO $$
BEGIN
    IF to_regclass('instagram_data.cuenta_diario') IS NOT NULL AND to_regclass('instagram_data.perfil_diario') IS NOT NULL THEN
        DROP VIEW IF EXISTS public.v_instagram_diario;
        CREATE VIEW public.v_instagram_diario AS
        SELECT
            COALESCE(c.fecha, p.fecha) AS fecha,
            c.alcance, c.alcance_seguidores, c.alcance_no_seguidores,
            c.visualizaciones, c.visualizaciones_seguidores, c.visualizaciones_no_seguidores,
            c.cuentas_interactuaron, c.interacciones,
            c.likes, c.comentarios, c.guardados, c.compartidos, c.respuestas, c.reposteos,
            c.toques_enlaces_perfil,
            c.seguidores_nuevos, c.dejaron_de_seguir,
            COALESCE(c.seguidores_nuevos, 0) - COALESCE(c.dejaron_de_seguir, 0) AS seguidores_netos,
            -- Foto tomada al correr el ETL: total de seguidores al inicio de ese día
            p.seguidores AS seguidores_totales,
            p.publicaciones AS publicaciones_totales
        FROM instagram_data.cuenta_diario c
        FULL OUTER JOIN instagram_data.perfil_diario p ON p.fecha = c.fecha;
    END IF;
END $$;

-- 2. Publicaciones y reels (una fila por publicación, métricas acumuladas a la última lectura)
DO $$
BEGIN
    IF to_regclass('instagram_data.publicaciones') IS NOT NULL THEN
        DROP VIEW IF EXISTS public.v_instagram_publicaciones;
        CREATE VIEW public.v_instagram_publicaciones AS
        SELECT
            p.id,
            p.fecha,
            p.publicado_en,
            CASE
                WHEN p.producto = 'REELS' THEN 'Reel'
                WHEN p.tipo = 'CAROUSEL_ALBUM' THEN 'Carrusel'
                WHEN p.tipo = 'VIDEO' THEN 'Video'
                ELSE 'Imagen'
            END AS formato,
            p.texto, p.enlace, p.miniatura,
            p.alcance, p.visualizaciones,
            p.likes, p.comentarios, p.guardados, p.compartidos, p.reposteos, p.interacciones,
            p.seguidores_ganados, p.visitas_perfil,
            ROUND(p.reels_tiempo_promedio_ms / 1000.0, 1) AS reels_segundos_promedio,
            ROUND(p.reels_tiempo_total_ms / 3600000.0, 2) AS reels_horas_reproducidas,
            p.actualizado_en
        FROM instagram_data.publicaciones p;
    END IF;
END $$;

-- 3. Historias (métricas de la última lectura antes de que expiraran)
DO $$
BEGIN
    IF to_regclass('instagram_data.historias') IS NOT NULL THEN
        DROP VIEW IF EXISTS public.v_instagram_historias;
        CREATE VIEW public.v_instagram_historias AS
        SELECT
            h.id,
            h.fecha,
            h.publicada_en,
            CASE WHEN h.tipo = 'VIDEO' THEN 'Video' ELSE 'Imagen' END AS formato,
            h.enlace, h.miniatura,
            h.alcance, h.visualizaciones,
            h.respuestas, h.compartidos, h.interacciones, h.seguidores_ganados, h.visitas_perfil,
            h.toques_adelante, h.toques_atras, h.salidas, h.deslizamientos,
            h.leida_en
        FROM instagram_data.historias h;
    END IF;
END $$;

-- 4. Audiencia: solo el último corte (edad, género, ciudad, país)
DO $$
BEGIN
    IF to_regclass('instagram_data.audiencia') IS NOT NULL THEN
        DROP VIEW IF EXISTS public.v_instagram_audiencia;
        CREATE VIEW public.v_instagram_audiencia AS
        SELECT
            a.fecha AS fecha_corte,
            a.tipo,
            CASE
                WHEN a.tipo = 'genero' AND a.valor = 'F' THEN 'Mujer'
                WHEN a.tipo = 'genero' AND a.valor = 'M' THEN 'Hombre'
                WHEN a.tipo = 'genero' THEN 'Sin especificar'
                ELSE a.valor
            END AS valor,
            a.seguidores
        FROM instagram_data.audiencia a
        WHERE a.fecha = (SELECT MAX(fecha) FROM instagram_data.audiencia);
    END IF;
END $$;
