-- ============================================================
-- 1️⃣ Create Database & Schema
-- ============================================================
CREATE OR REPLACE DATABASE COST_OPT;
CREATE OR REPLACE SCHEMA COST_OPT.RT;

-- ============================================================
-- 2️⃣ Warehouse Usage Pattern Table
--    Holds per-hour query workload metrics for warehouses
-- ============================================================
CREATE OR REPLACE TABLE COST_OPT.RT.WH_USAGE_PATTERN (
    DATABASE_NAME STRING,
    SCHEMA_NAME STRING,
    WAREHOUSE_NAME STRING,
    HOUR_BLOCK INT,
    AVG_QPS NUMBER,
    MEDIAN_QPS NUMBER,
    PEAK_FLAG BOOLEAN,
    LAST_REFRESHED TIMESTAMP_NTZ
);

-- ============================================================
-- 3️⃣ FinOps Logging Tables
--    a) FINOPS_WH_LOG        → Summary of warehouse actions
--    b) FINOPS_WH_LOG_DETAIL → Step-by-step detailed logs
-- ============================================================
CREATE OR REPLACE TABLE COST_OPT.RT.FINOPS_WH_LOG (
    RUN_ID STRING,
    ACTION_TS_NY TIMESTAMP_NTZ,
    WAREHOUSE_NAME STRING,
    PREV_SIZE STRING,
    NEW_SIZE STRING,
    PROJECTED_CREDITS_SAVED NUMBER,
    PROJECTED_DOLLARS_SAVED NUMBER,
    ROLLING_30D_CREDITS NUMBER,
    ROLLING_30D_DOLLARS NUMBER
);

CREATE OR REPLACE TABLE COST_OPT.RT.FINOPS_WH_LOG_DETAIL (
    RUN_ID STRING,
    ACTION_TS_NY TIMESTAMP_NTZ,
    STEP_NAME STRING,
    WAREHOUSE_NAME STRING,
    MESSAGE STRING
);

-- ============================================================
-- 4️⃣ Combined Savings & Debug View
--    - PROJECTED rows: resizing actions + savings
--    - DEBUG/ERROR rows: operational logs
--    Includes rolling 30-day cumulative metrics
-- ============================================================
CREATE OR REPLACE VIEW COST_OPT.RT.VW_WH_SAVINGS_COMBINED AS
WITH projected AS (
    SELECT
        RUN_ID,
        WAREHOUSE_NAME,
        DATE_TRUNC('DAY', ACTION_TS_NY) AS SAVINGS_DATE,
        PROJECTED_CREDITS_SAVED AS SAVED_CREDITS,
        PROJECTED_DOLLARS_SAVED AS SAVED_DOLLARS,
        'PROJECTED' AS SOURCE_TYPE,
        CONCAT('Prev=', COALESCE(PREV_SIZE,'?'), ' New=', COALESCE(NEW_SIZE,'?')) AS MESSAGE
    FROM COST_OPT.RT.FINOPS_WH_LOG
),
ops AS (
    SELECT
        RUN_ID,
        COALESCE(WAREHOUSE_NAME, '(GLOBAL)') AS WAREHOUSE_NAME,
        DATE_TRUNC('DAY', ACTION_TS_NY) AS SAVINGS_DATE,
        0 AS SAVED_CREDITS,
        0 AS SAVED_DOLLARS,
        CASE
          WHEN UPPER(STEP_NAME) LIKE '%ERROR%' OR UPPER(MESSAGE) LIKE '%ERROR%' THEN 'ERROR'
          ELSE 'DEBUG'
        END AS SOURCE_TYPE,
        MESSAGE
    FROM COST_OPT.RT.FINOPS_WH_LOG_DETAIL
)
SELECT
    RUN_ID,
    WAREHOUSE_NAME,
    SAVINGS_DATE,
    SOURCE_TYPE,
    MESSAGE,
    SAVED_CREDITS,
    SAVED_DOLLARS,
    CASE WHEN SOURCE_TYPE = 'PROJECTED' THEN
        SUM(SAVED_CREDITS) OVER (
            PARTITION BY WAREHOUSE_NAME, SOURCE_TYPE
            ORDER BY SAVINGS_DATE
            RANGE BETWEEN INTERVAL '30 DAY' PRECEDING AND CURRENT ROW
        )
    END AS ROLLING_30D_CREDITS,
    CASE WHEN SOURCE_TYPE = 'PROJECTED' THEN
        SUM(SAVED_DOLLARS) OVER (
            PARTITION BY WAREHOUSE_NAME, SOURCE_TYPE
            ORDER BY SAVINGS_DATE
            RANGE BETWEEN INTERVAL '30 DAY' PRECEDING AND CURRENT ROW
        )
    END AS ROLLING_30D_DOLLARS
FROM (
    SELECT * FROM projected
    UNION ALL
    SELECT * FROM ops
)
ORDER BY SAVINGS_DATE DESC, SOURCE_TYPE DESC;

-- ============================================================
-- 5️⃣ Procedure: BUILD_WH_USAGE_PATTERN
--     - Scans query history (last 90 days)
--     - Builds hourly workload pattern per warehouse
--     - Flags peak hours
-- ============================================================
CREATE OR REPLACE PROCEDURE COST_OPT.RT.BUILD_WH_USAGE_PATTERN()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
import snowflake.snowpark as snowpark
from datetime import datetime

def log_step(session, warehouse_name, step, status, message=None):
    """Helper to insert log into COST_OPT.RT.PROC_LOG"""
    session.sql(f"""
        INSERT INTO COST_OPT.RT.PROC_LOG (PROC_NAME, WAREHOUSE_NAME, STEP, STATUS, MESSAGE, LOG_TS)
        VALUES ('BUILD_WH_USAGE_PATTERN',
                '{warehouse_name}',
                '{step}',
                '{status}',
                {f"'{message}'" if message else 'NULL'},
                CURRENT_TIMESTAMP())
    """).collect()

def main(session: snowpark.Session) -> str:
    # Ensure log table exists
    session.sql("""
    CREATE TABLE IF NOT EXISTS COST_OPT.RT.PROC_LOG (
        PROC_NAME STRING,
        WAREHOUSE_NAME STRING,
        STEP STRING,
        STATUS STRING,
        MESSAGE STRING,
        LOG_TS TIMESTAMP_NTZ
    )
    """).collect()

    # Ensure pattern table exists
    session.sql("""
    CREATE TABLE IF NOT EXISTS COST_OPT.RT.WH_USAGE_PATTERN (
        DATABASE_NAME STRING,
        SCHEMA_NAME STRING,
        WAREHOUSE_NAME STRING,
        HOUR_BLOCK INT,
        AVG_QPS NUMBER,
        MEDIAN_QPS NUMBER,
        PEAK_FLAG BOOLEAN,
        LAST_REFRESHED TIMESTAMP_NTZ
    )
    """).collect()

    # Get all warehouses
    wh_list = session.sql("SHOW WAREHOUSES").collect()
    wh_names = [row["name"] for row in wh_list]

    summary = []

    for wh in wh_names:
        try:
            log_step(session, wh, "START", "RUNNING", "Processing warehouse usage pattern")

            # Clear old entries
            session.sql(f"DELETE FROM COST_OPT.RT.WH_USAGE_PATTERN WHERE WAREHOUSE_NAME = '{wh}'").collect()
            log_step(session, wh, "CLEANUP", "SUCCESS", "Old pattern removed")

            # Insert fresh baseline pattern (last 90 days)
            session.sql(f"""
                INSERT INTO COST_OPT.RT.WH_USAGE_PATTERN
                (DATABASE_NAME, SCHEMA_NAME, WAREHOUSE_NAME, HOUR_BLOCK, AVG_QPS, MEDIAN_QPS, PEAK_FLAG, LAST_REFRESHED)
                WITH base AS (
                    SELECT
                        DATABASE_NAME,
                        SCHEMA_NAME,
                        CONVERT_TIMEZONE('UTC','America/New_York', START_TIME) AS ny_time
                    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                    WHERE WAREHOUSE_NAME = '{wh}'
                      AND START_TIME >= DATEADD(day, -90, CURRENT_TIMESTAMP())
                ),
                per_hour_day AS (
                    SELECT
                        DATABASE_NAME,
                        SCHEMA_NAME,
                        DATE_TRUNC('day', ny_time) AS day,
                        EXTRACT(HOUR FROM ny_time) AS hour_block,
                        COUNT(*) AS q_count
                    FROM base
                    GROUP BY 1,2,3,4
                ),
                hour_stats AS (
                    SELECT
                        DATABASE_NAME,
                        SCHEMA_NAME,
                        hour_block,
                        AVG(q_count)    AS avg_qps,
                        MEDIAN(q_count) AS median_qps
                    FROM per_hour_day
                    GROUP BY 1,2,3
                ),
                threshold AS (
                    SELECT MEDIAN(median_qps) AS med_of_medians FROM hour_stats
                )
                SELECT
                    h.DATABASE_NAME              AS database_name,
                    h.SCHEMA_NAME                AS schema_name,
                    '{wh}'                       AS warehouse_name,
                    h.hour_block::INT            AS hour_block,
                    h.avg_qps::NUMBER            AS avg_qps,
                    h.median_qps::NUMBER         AS median_qps,
                    (h.median_qps > t.med_of_medians) AS peak_flag,
                    CURRENT_TIMESTAMP()          AS last_refreshed
                FROM hour_stats h
                CROSS JOIN threshold t
            """).collect()

            log_step(session, wh, "INSERT", "SUCCESS", "Pattern inserted successfully")
            summary.append(f"{wh}: SUCCESS")

        except Exception as e:
            log_step(session, wh, "ERROR", "FAILED", str(e))
            summary.append(f"{wh}: FAILED ({str(e)})")

    return " | ".join(summary)
$$;



CALL COST_OPT.RT.BUILD_WH_USAGE_PATTERN();

-- ============================================================
-- 6️⃣ Procedure: AUTO_RESIZE_WAREHOUSE
--     - Checks current NY hour & holiday
--     - Reads usage pattern table
--     - Decides resize policy (X-Small if holiday/off-peak, else Medium)
--     - Logs detailed steps into FINOPS_WH_LOG_DETAIL
--     - Logs savings into FINOPS_WH_LOG
-- ============================================================

CREATE OR REPLACE PROCEDURE COST_OPT.RT.AUTO_RESIZE_WAREHOUSE()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
import snowflake.snowpark as snowpark
import uuid
from snowflake.snowpark.functions import col

def log_step(session, run_id, step_name, message, warehouse=None):
    """Helper to log each step"""
    ts = session.sql("SELECT CONVERT_TIMEZONE('UTC','America/New_York', CURRENT_TIMESTAMP()) AS ts").collect()[0]["TS"]
    session.sql(f"""
        INSERT INTO COST_OPT.RT.FINOPS_WH_LOG_DETAIL (RUN_ID, ACTION_TS_NY, STEP_NAME, WAREHOUSE_NAME, MESSAGE)
        VALUES ('{run_id}', '{ts}', '{step_name}', {f"'{warehouse}'" if warehouse else 'NULL'}, '{message}')
    """).collect()

def nth_weekday_of_month(session, year, month, weekday, n):
    sql = f"""
        WITH days AS (
          SELECT DATE_FROM_PARTS({year}, {month}, seq4()+1) AS d
          FROM TABLE(GENERATOR(ROWCOUNT => 31))
          WHERE MONTH(DATE_FROM_PARTS({year}, {month}, seq4()+1)) = {month}
        )
        SELECT MIN(d) AS dt
        FROM (
          SELECT d, ROW_NUMBER() OVER (ORDER BY d) AS rn
          FROM days
          WHERE DAYOFWEEK(d) = {weekday}
        )
        WHERE rn = {n}
    """
    return str(session.sql(sql).collect()[0]["DT"])

def last_weekday_of_month(session, year, month, weekday):
    sql = f"""
        WITH days AS (
          SELECT DATE_FROM_PARTS({year}, {month}, seq4()+1) AS d
          FROM TABLE(GENERATOR(ROWCOUNT => 31))
          WHERE MONTH(DATE_FROM_PARTS({year}, {month}, seq4()+1)) = {month}
        )
        SELECT MAX(d) AS dt
        FROM days
        WHERE DAYOFWEEK(d) = {weekday}
    """
    return str(session.sql(sql).collect()[0]["DT"])

def easter_date(session, year):
    Y = int(year)
    a = Y % 19; b = Y // 100; c = Y % 100
    d = b // 4; e = b % 4; f = (b + 8) // 25
    g = (b - f + 1) // 3; h = (19*a + b - d - g + 15) % 30
    i = c // 4; k = c % 4
    l = (32 + 2*e + 2*i - h - k) % 7
    m = (a + 11*h + 22*l) // 451
    month = (h + l - 7*m + 114) // 31
    day = ((h + l - 7*m + 114) % 31) + 1
    return session.sql(f"SELECT DATE_FROM_PARTS({Y},{month},{day}) AS d").collect()[0]["D"]

def is_nyse_holiday(session):
    today = session.sql("SELECT CURRENT_DATE AS d").collect()[0]["D"]
    y = int(session.sql("SELECT YEAR(CURRENT_DATE) AS y").collect()[0]["Y"])

    fixed = [f"{y}-01-01", f"{y}-06-19", f"{y}-07-04", f"{y}-12-25"]
    shifted = []
    for h in fixed:
        res = session.sql(f"""
            SELECT CASE
                     WHEN DAYOFWEEK(DATE '{h}') = 0 THEN DATEADD(day, 1, DATE '{h}')
                     WHEN DAYOFWEEK(DATE '{h}') = 6 THEN DATEADD(day, -1, DATE '{h}')
                     ELSE DATE '{h}'
                   END AS d
        """).collect()[0]["D"]
        shifted.append(str(res))

    mlk = nth_weekday_of_month(session, y, 1, 1, 3)
    pres = nth_weekday_of_month(session, y, 2, 1, 3)
    memorial = last_weekday_of_month(session, y, 5, 1)
    labor = nth_weekday_of_month(session, y, 9, 1, 1)
    thanks = nth_weekday_of_month(session, y, 11, 4, 4)
    good_friday = str(session.sql("SELECT DATEADD(day,-2,%s) AS d" % f"DATE '{easter_date(session,y)}'").collect()[0]["D"])

    holidays = set(shifted + [mlk, pres, memorial, labor, thanks, good_friday])
    return str(today) in holidays

def main(session: snowpark.Session) -> str:
    # Ensure log tables
    session.sql("""
    CREATE TABLE IF NOT EXISTS COST_OPT.RT.FINOPS_WH_LOG (
        RUN_ID STRING,
        ACTION_TS_NY TIMESTAMP_NTZ,
        WAREHOUSE_NAME STRING,
        PREV_SIZE STRING,
        NEW_SIZE STRING,
        PROJECTED_CREDITS_SAVED NUMBER,
        PROJECTED_DOLLARS_SAVED NUMBER,
        ROLLING_30D_CREDITS NUMBER,
        ROLLING_30D_DOLLARS NUMBER
    )
    """).collect()

    session.sql("""
    CREATE TABLE IF NOT EXISTS COST_OPT.RT.FINOPS_WH_LOG_DETAIL (
        RUN_ID STRING,
        ACTION_TS_NY TIMESTAMP_NTZ,
        STEP_NAME STRING,
        WAREHOUSE_NAME STRING,
        MESSAGE STRING
    )
    """).collect()

    run_id = str(uuid.uuid4())
    log_step(session, run_id, "START", "Procedure execution started")

    current_hour = int(session.sql("""
        SELECT EXTRACT(HOUR FROM CONVERT_TIMEZONE('UTC','America/New_York', CURRENT_TIMESTAMP())) AS hr
    """).collect()[0]["HR"])
    log_step(session, run_id, "TIME_CHECK", f"Current NY hour: {current_hour}")

    is_holiday = is_nyse_holiday(session)
    log_step(session, run_id, "HOLIDAY_CHECK", f"Today is holiday={is_holiday}")

    wh_rows = session.sql("SHOW WAREHOUSES").collect()
    log_step(session, run_id, "WAREHOUSE_FETCH", f"Found {len(wh_rows)} warehouses")

    size_cost_map = {"X-Small": 1, "Small": 2, "Medium": 4, "Large": 8, "X-Large": 16}
    results = []

    for wh in wh_rows:
        wh_name = wh["name"]
        current_size = wh["size"]
        log_step(session, run_id, "WH_LOOP_START", f"Processing warehouse {wh_name}, current size={current_size}", warehouse=wh_name)

        pat = session.table("COST_OPT.RT.WH_USAGE_PATTERN") \
            .filter((col("WAREHOUSE_NAME") == wh_name) & (col("HOUR_BLOCK") == current_hour)) \
            .select("PEAK_FLAG").collect()
        is_peak = bool(pat[0]["PEAK_FLAG"]) if pat else False
        log_step(session, run_id, "PEAK_CHECK", f"Peak={is_peak}", warehouse=wh_name)

        new_size = "X-Small" if is_holiday or not is_peak else "Medium"
        log_step(session, run_id, "SIZE_DECISION", f"New size decided={new_size}", warehouse=wh_name)

        resized_flag = False
        if new_size != current_size:
            session.sql(f"ALTER WAREHOUSE {wh_name} SET WAREHOUSE_SIZE = '{new_size}'").collect()
            resized_flag = True
            log_step(session, run_id, "RESIZE_ACTION", f"Resized from {current_size} to {new_size}", warehouse=wh_name)
        else:
            log_step(session, run_id, "RESIZE_ACTION", "No resize needed", warehouse=wh_name)

        projected_credits_saved = size_cost_map.get(current_size, 0) - size_cost_map.get(new_size, 0)
        projected_dollars_saved = projected_credits_saved * 4

        action_ts_ny = session.sql("""
            SELECT CONVERT_TIMEZONE('UTC','America/New_York', CURRENT_TIMESTAMP()) AS ts
        """).collect()[0]["TS"]

        roll = session.sql(f"""
            SELECT
              COALESCE(SUM(PROJECTED_CREDITS_SAVED),0) AS rc,
              COALESCE(SUM(PROJECTED_DOLLARS_SAVED),0) AS rd
            FROM COST_OPT.RT.FINOPS_WH_LOG
            WHERE WAREHOUSE_NAME = '{wh_name}'
              AND ACTION_TS_NY >= DATEADD(day, -30, CONVERT_TIMEZONE('UTC','America/New_York', CURRENT_TIMESTAMP()))
        """).collect()[0]

        rolling_credits = float(roll["RC"]) + projected_credits_saved
        rolling_dollars = float(roll["RD"]) + projected_dollars_saved

        session.sql(f"""
            INSERT INTO COST_OPT.RT.FINOPS_WH_LOG (
                RUN_ID, ACTION_TS_NY, WAREHOUSE_NAME, PREV_SIZE, NEW_SIZE,
                PROJECTED_CREDITS_SAVED, PROJECTED_DOLLARS_SAVED,
                ROLLING_30D_CREDITS, ROLLING_30D_DOLLARS
            ) VALUES (
                '{run_id}', '{action_ts_ny}', '{wh_name}', '{current_size}', '{new_size}',
                {projected_credits_saved}, {projected_dollars_saved},
                {rolling_credits}, {rolling_dollars}
            )
        """).collect()
        log_step(session, run_id, "SUMMARY_LOG", f"Logged savings for {wh_name}: credits={projected_credits_saved}, dollars={projected_dollars_saved}", warehouse=wh_name)

        results.append(f"{wh_name}: final size {new_size} (holiday={is_holiday}, resized={resized_flag})")

    log_step(session, run_id, "END", "Procedure execution completed")
    return f"Run {run_id} completed. Results: " + " | ".join(results)
$$;

-- ============================================================
-- 7️⃣ Scheduled Task
--     - Runs AUTO_RESIZE_WAREHOUSE hourly
-- ============================================================
CREATE OR REPLACE TASK COST_OPT.RT.TASK_AUTO_RESIZE
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 0 * * * * UTC'
AS
CALL COST_OPT.RT.AUTO_RESIZE_WAREHOUSE();

ALTER TASK COST_OPT.RT.TASK_AUTO_RESIZE RESUME;




-- ==================================================================
-- 📊 Combined Dashboard View
--    - Shows per-warehouse monthly savings
--    - Adds company-wide monthly + cumulative totals
-- ==================================================================
CREATE OR REPLACE VIEW COST_OPT.RT.VW_FINOPS_DASHBOARD AS
WITH wh_monthly AS (
    SELECT
        WAREHOUSE_NAME,
        DATE_TRUNC('month', SAVINGS_DATE) AS SAVINGS_MONTH,
        SUM(COALESCE(SAVED_CREDITS,0)) AS MONTHLY_CREDITS_SAVED,
        SUM(COALESCE(SAVED_DOLLARS,0)) AS MONTHLY_DOLLARS_SAVED
    FROM COST_OPT.RT.VW_WH_SAVINGS_COMBINED
    WHERE SOURCE_TYPE = 'PROJECTED'
    GROUP BY WAREHOUSE_NAME, DATE_TRUNC('month', SAVINGS_DATE)
),
company_monthly AS (
    SELECT
        DATE_TRUNC('month', SAVINGS_DATE) AS SAVINGS_MONTH,
        SUM(COALESCE(SAVED_CREDITS,0)) AS MONTHLY_CREDITS_SAVED,
        SUM(COALESCE(SAVED_DOLLARS,0)) AS MONTHLY_DOLLARS_SAVED
    FROM COST_OPT.RT.VW_WH_SAVINGS_COMBINED
    WHERE SOURCE_TYPE = 'PROJECTED'
    GROUP BY DATE_TRUNC('month', SAVINGS_DATE)
),
company_cumulative AS (
    SELECT
        SAVINGS_MONTH,
        MONTHLY_CREDITS_SAVED,
        MONTHLY_DOLLARS_SAVED,
        SUM(MONTHLY_CREDITS_SAVED) OVER (ORDER BY SAVINGS_MONTH ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            AS CUMULATIVE_CREDITS_SAVED,
        SUM(MONTHLY_DOLLARS_SAVED) OVER (ORDER BY SAVINGS_MONTH ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            AS CUMULATIVE_DOLLARS_SAVED
    FROM company_monthly
)
SELECT
    wm.WAREHOUSE_NAME,
    wm.SAVINGS_MONTH,
    wm.MONTHLY_CREDITS_SAVED,
    wm.MONTHLY_DOLLARS_SAVED,
    cc.CUMULATIVE_CREDITS_SAVED,
    cc.CUMULATIVE_DOLLARS_SAVED
FROM wh_monthly wm
JOIN company_cumulative cc
  ON wm.SAVINGS_MONTH = cc.SAVINGS_MONTH
ORDER BY wm.SAVINGS_MONTH DESC, wm.WAREHOUSE_NAME;



-- Manual test run
CALL COST_OPT.RT.AUTO_RESIZE_WAREHOUSE();


select * from COST_OPT.RT.WH_USAGE_PATTERN;

select * from COST_OPT.RT.FINOPS_WH_LOG;

select * from COST_OPT.RT.FINOPS_WH_LOG_DETAIL;

select * from COST_OPT.RT.VW_WH_SAVINGS_COMBINED;

select * from COST_OPT.RT.VW_FINOPS_DASHBOARD;
