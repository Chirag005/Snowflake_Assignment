USE DATABASE PLANNING_DB;
USE WAREHOUSE COMPUTE_WH;
USE SCHEMA PLANNING;

CREATE TABLE IF NOT EXISTS PLANNING.NOTIFICATION_LOG (
    NOTIFICATIONID  BIGINT           NOT NULL AUTOINCREMENT PRIMARY KEY,
    EVENTTYPE       VARCHAR(50)      NOT NULL,
    ENTITYTYPE      VARCHAR(30)      NULL,
    ENTITYID        INTEGER          NULL,
    MESSAGE         VARCHAR(16777216) NULL,
    STATUSCODE      VARCHAR(20)      NOT NULL DEFAULT 'SENT',
    CREATEDDATETIME TIMESTAMP_NTZ(9) NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE PROCEDURE PLANNING.USP_PERFORMFINANCIALCLOSE (
    CLOSE_PERIOD_ID           INTEGER,
    CLOSE_SCENARIO_TYPE       VARCHAR  DEFAULT 'BASE',
    INCLUDE_CONSOLIDATION     BOOLEAN  DEFAULT TRUE,
    INCLUDE_COST_ALLOCATION   BOOLEAN  DEFAULT TRUE,
    INCLUDE_IC_RECONCILIATION BOOLEAN  DEFAULT TRUE,
    GENERATE_ROLLING_FORECAST BOOLEAN  DEFAULT FALSE,
    VALIDATE_BEFORE_CLOSE     BOOLEAN  DEFAULT TRUE,
    DEBUG_MODE                BOOLEAN  DEFAULT FALSE
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    v_run_id        VARCHAR  := UUID_STRING();
    v_fiscal_year   SMALLINT := 0;
    v_period_name   VARCHAR  := '';
    v_is_closed     BOOLEAN  := FALSE;
    v_active_budget INTEGER  := 0;
    v_steps         ARRAY    := ARRAY_CONSTRUCT();
    v_period_exists INTEGER  := 0;
    v_consol_result VARIANT;
    v_alloc_result  VARIANT;
    v_ic_result     VARIANT;
    v_fc_result     VARIANT;

BEGIN
    v_period_exists := (
        SELECT COUNT(*) FROM PLANNING.FISCALPERIOD
        WHERE FISCALPERIODID = :CLOSE_PERIOD_ID
    );

    IF (v_period_exists = 0) THEN
        RETURN OBJECT_CONSTRUCT(
            'STATUS',        'ERROR',
            'ERROR_MESSAGE', 'Fiscal period not found: ' || CLOSE_PERIOD_ID::VARCHAR
        )::VARIANT;
    END IF;

    v_fiscal_year := (
        SELECT FISCALYEAR FROM PLANNING.FISCALPERIOD
        WHERE FISCALPERIODID = :CLOSE_PERIOD_ID
    );

    v_period_name := (
        SELECT PERIODNAME FROM PLANNING.FISCALPERIOD
        WHERE FISCALPERIODID = :CLOSE_PERIOD_ID
    );

    v_is_closed := (
        SELECT ISCLOSED FROM PLANNING.FISCALPERIOD
        WHERE FISCALPERIODID = :CLOSE_PERIOD_ID
    );

    IF (v_is_closed = TRUE) THEN
        RETURN OBJECT_CONSTRUCT(
            'STATUS',        'ERROR',
            'ERROR_MESSAGE', 'Period ' || v_period_name || ' is already closed.'
        )::VARIANT;
    END IF;

    IF (VALIDATE_BEFORE_CLOSE) THEN
        LET v_pending INTEGER := (
            SELECT COUNT(*) FROM PLANNING.CONSOLIDATIONJOURNAL
            WHERE FISCALPERIODID = :CLOSE_PERIOD_ID
              AND STATUSCODE IN ('DRAFT', 'SUBMITTED')
        );

        IF (v_pending > 0) THEN
            v_steps := ARRAY_APPEND(v_steps, OBJECT_CONSTRUCT(
                'Step',    'Validation',
                'Status',  'WARNING',
                'Message', v_pending::VARCHAR || ' pending journal(s) not posted'
            ));
        ELSE
            v_steps := ARRAY_APPEND(v_steps, OBJECT_CONSTRUCT(
                'Step',   'Validation',
                'Status', 'COMPLETED'
            ));
        END IF;
    END IF;

    v_active_budget := (
        SELECT MAX(BH.BUDGETHEADERID)
        FROM PLANNING.BUDGETHEADER BH
        WHERE BH.STARTPERIODID <= :CLOSE_PERIOD_ID
          AND BH.ENDPERIODID   >= :CLOSE_PERIOD_ID
          AND BH.STATUSCODE IN ('APPROVED', 'LOCKED')
          AND BH.SCENARIOTYPE = :CLOSE_SCENARIO_TYPE
    );

    IF (INCLUDE_CONSOLIDATION AND v_active_budget IS NOT NULL AND v_active_budget > 0) THEN
        -- Positional arguments: (SOURCE_BUDGET_HEADER_ID, P_TARGET_BUDGET_HEADER_ID, CONSOLIDATION_TYPE, INCLUDE_ELIMINATIONS, RECALCULATE_ALLOCATIONS, PROCESSING_OPTIONS, USER_ID, DEBUG_MODE)
        v_consol_result := (CALL PLANNING.USP_PROCESSBUDGETCONSOLIDATION(:v_active_budget, NULL, 'FULL', TRUE, FALSE, NULL, NULL, :DEBUG_MODE));
        v_steps := ARRAY_APPEND(v_steps, OBJECT_CONSTRUCT(
            'Step',   'Consolidation',
            'Status', v_consol_result:STATUS::VARCHAR
        ));
    END IF;

    IF (INCLUDE_COST_ALLOCATION AND v_active_budget IS NOT NULL AND v_active_budget > 0) THEN
        -- Positional arguments: (BUDGET_HEADER_ID, ALLOCATION_RULE_IDS, P_FISCAL_PERIOD_ID, DRY_RUN, MAX_ITERATIONS)
        v_alloc_result := (CALL PLANNING.USP_EXECUTECOSTALLOCATION(:v_active_budget, NULL, :CLOSE_PERIOD_ID, FALSE, 50));
        v_steps := ARRAY_APPEND(v_steps, OBJECT_CONSTRUCT(
            'Step',   'CostAllocation',
            'Status', v_alloc_result:STATUS::VARCHAR
        ));
    END IF;

    IF (INCLUDE_IC_RECONCILIATION AND v_active_budget IS NOT NULL AND v_active_budget > 0) THEN
        -- Positional arguments: (BUDGET_HEADER_ID, RECONCILIATION_DATE, ENTITY_CODES, TOLERANCE_AMOUNT, TOLERANCE_PERCENT, AUTO_CREATE_ADJUSTMENTS)
        v_ic_result := (CALL PLANNING.USP_RECONCILEINTERCOMPANYBALANCES(:v_active_budget, NULL, NULL, 0.01, 0.001, FALSE));
        v_steps := ARRAY_APPEND(v_steps, OBJECT_CONSTRUCT(
            'Step',             'ICReconciliation',
            'Status',           v_ic_result:STATUS::VARCHAR,
            'UnreconciledCount', v_ic_result:UNRECONCILED_COUNT::INTEGER
        ));
    END IF;

    IF (GENERATE_ROLLING_FORECAST AND v_active_budget IS NOT NULL AND v_active_budget > 0) THEN
        -- Positional arguments: (BASE_BUDGET_HEADER_ID, HISTORICAL_PERIODS, FORECAST_PERIODS, FORECAST_METHOD, SEASONALITY_JSON, GROWTH_RATE_OVERRIDE, OUTPUT_FORMAT)
        v_fc_result := (CALL PLANNING.USP_GENERATEROLLINGFORECAST(:v_active_budget, 3, 3, 'WEIGHTED_AVERAGE', NULL, NULL, 'SUMMARY'));
        v_steps := ARRAY_APPEND(v_steps, OBJECT_CONSTRUCT(
            'Step',   'RollingForecast',
            'Status', v_fc_result:STATUS::VARCHAR
        ));
    END IF;

    BEGIN TRANSACTION;

    UPDATE PLANNING.FISCALPERIOD
    SET ISCLOSED = TRUE,
        CLOSEDDATETIME = CURRENT_TIMESTAMP()
    WHERE FISCALPERIODID = :CLOSE_PERIOD_ID;

    UPDATE PLANNING.BUDGETHEADER
    SET STATUSCODE        = 'LOCKED',
        LOCKEDDATETIME    = CURRENT_TIMESTAMP(),
        MODIFIEDDATETIME  = CURRENT_TIMESTAMP()
    WHERE STATUSCODE   = 'APPROVED'
      AND STARTPERIODID <= :CLOSE_PERIOD_ID
      AND ENDPERIODID   >= :CLOSE_PERIOD_ID;

    INSERT INTO PLANNING.NOTIFICATION_LOG (EVENTTYPE, ENTITYTYPE, ENTITYID, MESSAGE)
    VALUES (
        'PERIOD_CLOSED',
        'FISCALPERIOD',
        :CLOSE_PERIOD_ID,
        'Period ' || :v_period_name || ' closed via run ' || :v_run_id
    );

    COMMIT;

    v_steps := ARRAY_APPEND(v_steps, OBJECT_CONSTRUCT(
        'Step',   'LockPeriod',
        'Status', 'COMPLETED'
    ));

    RETURN OBJECT_CONSTRUCT(
        'STATUS',      'COMPLETED',
        'PERIOD_ID',   CLOSE_PERIOD_ID,
        'PERIOD_NAME', v_period_name,
        'FISCAL_YEAR', v_fiscal_year,
        'STEPS',       v_steps,
        'RUN_ID',      v_run_id
    )::VARIANT;

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RETURN OBJECT_CONSTRUCT(
            'STATUS',        'ERROR',
            'ERROR_MESSAGE', SQLERRM,
            'STEPS',         v_steps
        )::VARIANT;
END;
$$;
