
---------------------------
--Q1. Filter Easy Recipes--
---------------------------


WITH time_slots AS (
    SELECT unnest(ARRAY[
        1,5,10,15,30,45,60,
        120,180,360,540,720,1440,2160,2880
    ]) AS return_time
),

filtered_recipes AS (
    SELECT r.*
    FROM recipes_all_restaurant r
    LEFT JOIN appliances_all_restaurant a
        ON r.appl_name = a.appl_name
    WHERE 
        TRIM(LOWER(COALESCE(r.rcp_labels, ''))) IN ('', 'nan')
        AND TRIM(LOWER(COALESCE(a.appl_labels, ''))) IN ('', 'nan')
),

base AS (
    SELECT 
        t.return_time,
        r.rcp_name,
        r.rcp_xp,
        r.rcp_profit,
        r.time_min
    FROM time_slots t
    JOIN filtered_recipes r
        ON r.time_min ~ '^[0-9]+$'          -- ensures numeric
        AND r.time_min::INT <= t.return_time
),

ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY return_time 
            ORDER BY rcp_xp DESC
        ) AS xp_rank,
        ROW_NUMBER() OVER (
            PARTITION BY return_time 
            ORDER BY rcp_profit DESC
        ) AS profit_rank
    FROM base
),

final AS (
    SELECT 
        return_time,

        MAX(CASE WHEN xp_rank = 1 THEN rcp_name END) AS best_xp_recipe,
        MAX(CASE WHEN xp_rank = 1 THEN rcp_xp END) AS best_xp,

        MAX(CASE WHEN profit_rank = 1 THEN rcp_name END) AS best_profit_recipe,
        MAX(CASE WHEN profit_rank = 1 THEN rcp_profit END) AS best_profit

    FROM ranked
    GROUP BY return_time
)

SELECT 
    return_time,

    best_xp_recipe,
    best_xp,
    ROUND((best_xp * 60.0) / return_time, 2) AS xp_per_hr,

    best_profit_recipe,
    best_profit,
    ROUND((best_profit * 60.0) / return_time, 2) AS profit_per_hr

FROM final
ORDER BY return_time;








-----------------------------------
--Q2. Filter Easy+ Medium Recipes--
-----------------------------------


WITH time_slots AS (
    SELECT unnest(ARRAY[
        1,5,10,15,30,45,60,
        120,180,360,540,720,1440,2160,2880
    ]) AS return_time
),

filtered_recipes AS (
    SELECT r.*
    FROM recipes_all_restaurant r
    LEFT JOIN appliances_all_restaurant a
        ON r.appl_name = a.appl_name
    WHERE 
        TRIM(LOWER(COALESCE(r.rcp_labels, ''))) IN ('', 'nan')
        AND (
            COALESCE(a.appl_labels, '') ILIKE '%build%'
            OR COALESCE(a.appl_labels, '') ILIKE '%gem%'
            OR TRIM(LOWER(COALESCE(a.appl_labels, ''))) IN ('', 'nan')
        )
),

base AS (
    SELECT 
        t.return_time,
        r.rcp_name,
        r.rcp_xp,
        r.rcp_profit,
        r.time_min
    FROM time_slots t
    JOIN filtered_recipes r
        ON r.time_min ~ '^[0-9]+$'          -- ensures numeric
        AND r.time_min::INT <= t.return_time
),

ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY return_time 
            ORDER BY rcp_xp DESC
        ) AS xp_rank,
        ROW_NUMBER() OVER (
            PARTITION BY return_time 
            ORDER BY rcp_profit DESC
        ) AS profit_rank
    FROM base
),

final AS (
    SELECT 
        return_time,

        MAX(CASE WHEN xp_rank = 1 THEN rcp_name END) AS best_xp_recipe,
        MAX(CASE WHEN xp_rank = 1 THEN rcp_xp END) AS best_xp,

        MAX(CASE WHEN profit_rank = 1 THEN rcp_name END) AS best_profit_recipe,
        MAX(CASE WHEN profit_rank = 1 THEN rcp_profit END) AS best_profit

    FROM ranked
    GROUP BY return_time
)

SELECT 
    return_time,

    best_xp_recipe,
    best_xp,
    ROUND((best_xp * 60.0) / return_time, 2) AS xp_per_hr,

    best_profit_recipe,
    best_profit,
    ROUND((best_profit * 60.0) / return_time, 2) AS profit_per_hr

FROM final
ORDER BY return_time;







-------------------------------------------
--Q3. Filter Easy + Medium + Hard Recipes--
-------------------------------------------



WITH time_slots AS (
    SELECT unnest(ARRAY[
        1,5,10,15,30,45,60,
        120,180,360,540,720,1440,2160,2880
    ]) AS return_time
),

filtered_recipes AS (
    SELECT r.*
    FROM recipes_all_restaurant r
    LEFT JOIN appliances_all_restaurant a
        ON r.appl_name = a.appl_name
--    WHERE 
--        TRIM(LOWER(COALESCE(r.labels, ''))) IN ('', 'nan')
--    AND TRIM(LOWER(COALESCE(a.labels, ''))) IN ('', 'nan')
),

base AS (
    SELECT 
        t.return_time,
        r.rcp_name,
        r.rcp_xp,
        r.rcp_profit,
        r.time_min
    FROM time_slots t
    JOIN filtered_recipes r
        ON r.time_min ~ '^[0-9]+$'          -- ensures numeric
        AND r.time_min::INT <= t.return_time
),

ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY return_time 
            ORDER BY rcp_xp DESC
        ) AS xp_rank,
        ROW_NUMBER() OVER (
            PARTITION BY return_time 
            ORDER BY rcp_profit DESC
        ) AS profit_rank
    FROM base
),

final AS (
    SELECT 
        return_time,

        MAX(CASE WHEN xp_rank = 1 THEN rcp_name END) AS best_xp_recipe,
        MAX(CASE WHEN xp_rank = 1 THEN rcp_xp END) AS best_xp,

        MAX(CASE WHEN profit_rank = 1 THEN rcp_name END) AS best_profit_recipe,
        MAX(CASE WHEN profit_rank = 1 THEN rcp_profit END) AS best_profit

    FROM ranked
    GROUP BY return_time
)

SELECT 
    return_time,

    best_xp_recipe,
    best_xp,
    ROUND((best_xp * 60.0) / return_time, 2) AS xp_per_hr,

    best_profit_recipe,
    best_profit,
    ROUND((best_profit * 60.0) / return_time, 2) AS profit_per_hr

FROM final
ORDER BY return_time;



------------------------------
--Q4. Final
------------------------------

WITH time_slots AS (
    SELECT unnest(ARRAY[
        5, 10, 15, 30, 45, 60,
        120, 180, 360, 540, 720, 1440, 2160, 2880
    ]) AS return_time
),

easy_recipes AS (
    SELECT r.*
    FROM recipes_all_restaurant r
    LEFT JOIN appliances_all_restaurant a
        ON r.appl_name = a.appl_name
    WHERE 
        TRIM(LOWER(COALESCE(r.rcp_labels, ''))) IN ('', 'nan')
    AND TRIM(LOWER(COALESCE(a.appl_labels, ''))) IN ('', 'nan')
),

medium_recipes AS (
    SELECT r.*
    FROM recipes_all_restaurant r
    LEFT JOIN appliances_all_restaurant a
        ON r.appl_name = a.appl_name
    WHERE 
        TRIM(LOWER(COALESCE(r.rcp_labels, ''))) IN ('', 'nan')
        AND (
            COALESCE(a.appl_labels, '') ILIKE '%build%'
            OR COALESCE(a.appl_labels, '') ILIKE '%gem%'
            OR TRIM(LOWER(COALESCE(a.appl_labels, ''))) IN ('', 'nan')
        )
),

hard_recipes AS (
    SELECT * FROM recipes_all_restaurant
),

easy_base AS (
    SELECT 
        t.return_time,
        'easy' AS recipe_type,
        r.rcp_name,
        r.rcp_xp,
        r.rcp_profit,
        r.time_min
    FROM time_slots t
    JOIN easy_recipes r
        ON CAST(r.time_min AS INTEGER) <= t.return_time
),

medium_base AS (
    SELECT 
        t.return_time,
        'medium' AS recipe_type,
        r.rcp_name,
        r.rcp_xp,
        r.rcp_profit,
        r.time_min
    FROM time_slots t
    JOIN medium_recipes r
        ON CAST(r.time_min AS INTEGER) <= t.return_time
),

hard_base AS (
    SELECT 
        t.return_time,
        'hard' AS recipe_type,
        r.rcp_name,
        r.rcp_xp,
        r.rcp_profit,
        r.time_min
    FROM time_slots t
    JOIN hard_recipes r
        ON CAST(r.time_min AS INTEGER) <= t.return_time
),

combined AS (
    SELECT * FROM easy_base
    UNION ALL
    SELECT * FROM medium_base
    UNION ALL
    SELECT * FROM hard_base
),

ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY return_time, recipe_type ORDER BY rcp_xp DESC) AS xp_rank,
        ROW_NUMBER() OVER (PARTITION BY return_time, recipe_type ORDER BY rcp_profit DESC) AS profit_rank
    FROM combined
),

final AS (
    SELECT 
        return_time,
        recipe_type,

        MAX(CASE WHEN xp_rank = 1 THEN rcp_name END) AS best_xp_recipe,
        MAX(CASE WHEN xp_rank = 1 THEN rcp_xp END) AS best_xp,

        MAX(CASE WHEN profit_rank = 1 THEN rcp_name END) AS best_profit_recipe,
        MAX(CASE WHEN profit_rank = 1 THEN rcp_profit END) AS best_profit

    FROM ranked
    GROUP BY return_time, recipe_type
)

SELECT 
    return_time,
    recipe_type,

    best_xp_recipe,
    best_xp,
    ROUND((best_xp * 60.0) / return_time, 2) AS xp_per_hr,

    best_profit_recipe,
    best_profit,
    ROUND((best_profit * 60.0) / return_time, 2) AS profit_per_hr

FROM final
ORDER BY 
    return_time,
    CASE 
        WHEN recipe_type = 'easy' THEN 1
        WHEN recipe_type = 'medium' THEN 2
        WHEN recipe_type = 'hard' THEN 3
    END;