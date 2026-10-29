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

-- 5. VISTA ÚNICA para Looker Studio: las 4 anteriores en una sola fuente de datos.
-- Cada fila trae datos solo en su grupo de columnas (prefijo cuenta_, publicacion_, historia_, audiencia_)
-- y NULL en los demás, así que sumar cualquier columna no mezcla grupos ni necesita filtros.
DO $$
BEGIN
    IF to_regclass('public.v_instagram_diario') IS NOT NULL AND to_regclass('public.v_instagram_publicaciones') IS NOT NULL
       AND to_regclass('public.v_instagram_historias') IS NOT NULL AND to_regclass('public.v_instagram_audiencia') IS NOT NULL THEN
        DROP VIEW IF EXISTS public.v_instagram;
        CREATE VIEW public.v_instagram AS
        SELECT
            CASE
                WHEN d.fecha IS NOT NULL THEN 'Cuenta (día)'
                WHEN p.id IS NOT NULL THEN 'Publicación'
                WHEN h.id IS NOT NULL THEN 'Historia'
                ELSE 'Audiencia'
            END AS tipo_registro,
            COALESCE(d.fecha, p.fecha, h.fecha, a.fecha_corte) AS fecha,
            -- Cuenta (una fila por día)
            d.alcance AS cuenta_alcance,
            d.alcance_seguidores AS cuenta_alcance_seguidores,
            d.alcance_no_seguidores AS cuenta_alcance_no_seguidores,
            d.visualizaciones AS cuenta_visualizaciones,
            d.visualizaciones_seguidores AS cuenta_visualizaciones_seguidores,
            d.visualizaciones_no_seguidores AS cuenta_visualizaciones_no_seguidores,
            d.cuentas_interactuaron AS cuenta_cuentas_interactuaron,
            d.interacciones AS cuenta_interacciones,
            d.likes AS cuenta_likes,
            d.comentarios AS cuenta_comentarios,
            d.guardados AS cuenta_guardados,
            d.compartidos AS cuenta_compartidos,
            d.respuestas AS cuenta_respuestas_historias,
            d.reposteos AS cuenta_reposteos,
            d.toques_enlaces_perfil AS cuenta_toques_enlaces_perfil,
            d.seguidores_nuevos,
            d.dejaron_de_seguir AS seguidores_perdidos,
            CASE WHEN d.fecha IS NOT NULL THEN d.seguidores_netos END AS seguidores_netos,
            d.seguidores_totales,
            -- Publicaciones y reels (una fila por publicación)
            p.id AS publicacion_id,
            p.formato AS publicacion_formato,
            p.texto AS publicacion_texto,
            p.enlace AS publicacion_enlace,
            p.miniatura AS publicacion_miniatura,
            CASE WHEN p.id IS NOT NULL THEN 1 END AS publicacion_cantidad,
            p.alcance AS publicacion_alcance,
            p.visualizaciones AS publicacion_visualizaciones,
            p.likes AS publicacion_likes,
            p.comentarios AS publicacion_comentarios,
            p.guardados AS publicacion_guardados,
            p.compartidos AS publicacion_compartidos,
            p.reposteos AS publicacion_reposteos,
            p.interacciones AS publicacion_interacciones,
            p.seguidores_ganados AS publicacion_seguidores_ganados,
            p.visitas_perfil AS publicacion_visitas_perfil,
            p.reels_segundos_promedio AS reel_segundos_promedio,
            p.reels_horas_reproducidas AS reel_horas_reproducidas,
            -- Historias (una fila por historia)
            h.id AS historia_id,
            h.formato AS historia_formato,
            h.enlace AS historia_enlace,
            h.miniatura AS historia_miniatura,
            CASE WHEN h.id IS NOT NULL THEN 1 END AS historia_cantidad,
            h.alcance AS historia_alcance,
            h.visualizaciones AS historia_visualizaciones,
            h.respuestas AS historia_respuestas,
            h.compartidos AS historia_compartidos,
            h.interacciones AS historia_interacciones,
            h.seguidores_ganados AS historia_seguidores_ganados,
            h.visitas_perfil AS historia_visitas_perfil,
            h.toques_adelante AS historia_toques_adelante,
            h.toques_atras AS historia_toques_atras,
            h.salidas AS historia_salidas,
            h.deslizamientos AS historia_deslizamientos,
            -- Audiencia (último corte)
            a.tipo AS audiencia_tipo,
            a.valor AS audiencia_valor,
            a.seguidores AS audiencia_seguidores
        FROM public.v_instagram_diario d
        FULL JOIN public.v_instagram_publicaciones p ON false
        FULL JOIN public.v_instagram_historias h ON false
        FULL JOIN public.v_instagram_audiencia a ON false;
    END IF;
END $$;
