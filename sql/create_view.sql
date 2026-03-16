-- =============================================================================
-- VIEW: vw_big_five_clean
-- Description: Cleaned and filtered view of raw Big Five personality test data.
--              Prepares data for downstream PySpark transformations.
--
-- Source table: big_five_raw (loaded from data-final.csv)
-- Dataset:      IPIP Big Five Factor Markers
--               http://openpsychometrics.org/_rawdata/IPIP-FFM-data-8Nov2018.zip
--
-- Output rows:  ~597k (out of ~1M raw) after quality filters.
--
-- What this view does:
--   1. Selects all 50 raw item responses (scale 1-5, NOT yet reverse-scored).
--   2. Selects key metadata: country, date, test duration, geolocation.
--   3. Computes total response time (ms) per Big Five trait from per-item timings.
--   4. Filters out invalid/incomplete responses (see WHERE clause).
--
-- What this view does NOT do (handled in PySpark):
--   - Reverse scoring of negatively-keyed items.
--   - Computing final Big Five trait scores (sum/average per trait).
--   - Statistical aggregations and correlations.
--
-- Compatibility: SQL Server 2019
-- Author:        Walery
-- Last updated:  2026-03
-- =============================================================================


USE BigFiveDB;
GO

CREATE OR ALTER VIEW vw_big_five_clean AS
SELECT
    -- -------------------------------------------------------------------------
    -- Raw item responses (1=Disagree, 3=Neutral, 5=Agree).
    -- NOTE: These are raw scores. Reverse-keyed items are flipped in PySpark.
    -- Reverse-keyed items:
    --   EXT: 2,4,6,8,10 | EST: 1,3,5,6,7,8,9,10
    --   AGR: 1,3,5,7    | CSN: 2,4,6,8 | OPN: 2,4,6
    -- -------------------------------------------------------------------------
    EXT1,EXT2,EXT3,EXT4,EXT5,EXT6,EXT7,EXT8,EXT9,EXT10,  -- Extraversion
    EST1,EST2,EST3,EST4,EST5,EST6,EST7,EST8,EST9,EST10,  -- Emotional Stability
    AGR1,AGR2,AGR3,AGR4,AGR5,AGR6,AGR7,AGR8,AGR9,AGR10, -- Agreeableness
    CSN1,CSN2,CSN3,CSN4,CSN5,CSN6,CSN7,CSN8,CSN9,CSN10, -- Conscientiousness
    OPN1,OPN2,OPN3,OPN4,OPN5,OPN6,OPN7,OPN8,OPN9,OPN10, -- Openness

    -- -------------------------------------------------------------------------
    -- Metadata
    -- -------------------------------------------------------------------------
    testelapse,                        -- Total time spent on test page (seconds)
    country,                           -- Country code (derived from IP, not self-reported)
    lat_appx_lots_of_err AS lat,       -- Approximate latitude (VERY inaccurate, see codebook)
    long_appx_lots_of_err AS lon,      -- Approximate longitude (VERY inaccurate, see codebook)
    CAST(dateload AS DATE) AS test_date, -- Date survey was started (timestamp -> date)

    -- -------------------------------------------------------------------------
    -- Response times per trait (sum of per-item timings in milliseconds).
    -- Used in PySpark to analyze whether response speed correlates with scores.
    -- ISNULL(..., 0) prevents NULL propagation if any single item time is missing.
    -- -------------------------------------------------------------------------
    (ISNULL(EXT1_E,0)+ISNULL(EXT2_E,0)+ISNULL(EXT3_E,0)+ISNULL(EXT4_E,0)+ISNULL(EXT5_E,0)+
     ISNULL(EXT6_E,0)+ISNULL(EXT7_E,0)+ISNULL(EXT8_E,0)+ISNULL(EXT9_E,0)+ISNULL(EXT10_E,0)) AS ext_response_ms,
    (ISNULL(EST1_E,0)+ISNULL(EST2_E,0)+ISNULL(EST3_E,0)+ISNULL(EST4_E,0)+ISNULL(EST5_E,0)+
     ISNULL(EST6_E,0)+ISNULL(EST7_E,0)+ISNULL(EST8_E,0)+ISNULL(EST9_E,0)+ISNULL(EST10_E,0)) AS est_response_ms,
    (ISNULL(AGR1_E,0)+ISNULL(AGR2_E,0)+ISNULL(AGR3_E,0)+ISNULL(AGR4_E,0)+ISNULL(AGR5_E,0)+
     ISNULL(AGR6_E,0)+ISNULL(AGR7_E,0)+ISNULL(AGR8_E,0)+ISNULL(AGR9_E,0)+ISNULL(AGR10_E,0)) AS agr_response_ms,
    (ISNULL(CSN1_E,0)+ISNULL(CSN2_E,0)+ISNULL(CSN3_E,0)+ISNULL(CSN4_E,0)+ISNULL(CSN5_E,0)+
     ISNULL(CSN6_E,0)+ISNULL(CSN7_E,0)+ISNULL(CSN8_E,0)+ISNULL(CSN9_E,0)+ISNULL(CSN10_E,0)) AS csn_response_ms,
    (ISNULL(OPN1_E,0)+ISNULL(OPN2_E,0)+ISNULL(OPN3_E,0)+ISNULL(OPN4_E,0)+ISNULL(OPN5_E,0)+
     ISNULL(OPN6_E,0)+ISNULL(OPN7_E,0)+ISNULL(OPN8_E,0)+ISNULL(OPN9_E,0)+ISNULL(OPN10_E,0)) AS opn_response_ms
FROM big_five_raw
WHERE
    -- -------------------------------------------------------------------------
    -- Data quality filters
    -- -------------------------------------------------------------------------

    -- All 50 items must be within valid scale range (1-5).
    -- Catches zeros, nulls, and out-of-range entries.
    EXT1 BETWEEN 1 AND 5 AND EXT2 BETWEEN 1 AND 5 AND EXT3 BETWEEN 1 AND 5
    AND EXT4 BETWEEN 1 AND 5 AND EXT5 BETWEEN 1 AND 5 AND EXT6 BETWEEN 1 AND 5
    AND EXT7 BETWEEN 1 AND 5 AND EXT8 BETWEEN 1 AND 5 AND EXT9 BETWEEN 1 AND 5
    AND EXT10 BETWEEN 1 AND 5
    AND EST1 BETWEEN 1 AND 5 AND EST2 BETWEEN 1 AND 5 AND EST3 BETWEEN 1 AND 5
    AND EST4 BETWEEN 1 AND 5 AND EST5 BETWEEN 1 AND 5 AND EST6 BETWEEN 1 AND 5
    AND EST7 BETWEEN 1 AND 5 AND EST8 BETWEEN 1 AND 5 AND EST9 BETWEEN 1 AND 5
    AND EST10 BETWEEN 1 AND 5
    AND AGR1 BETWEEN 1 AND 5 AND AGR2 BETWEEN 1 AND 5 AND AGR3 BETWEEN 1 AND 5
    AND AGR4 BETWEEN 1 AND 5 AND AGR5 BETWEEN 1 AND 5 AND AGR6 BETWEEN 1 AND 5
    AND AGR7 BETWEEN 1 AND 5 AND AGR8 BETWEEN 1 AND 5 AND AGR9 BETWEEN 1 AND 5
    AND AGR10 BETWEEN 1 AND 5
    AND CSN1 BETWEEN 1 AND 5 AND CSN2 BETWEEN 1 AND 5 AND CSN3 BETWEEN 1 AND 5
    AND CSN4 BETWEEN 1 AND 5 AND CSN5 BETWEEN 1 AND 5 AND CSN6 BETWEEN 1 AND 5
    AND CSN7 BETWEEN 1 AND 5 AND CSN8 BETWEEN 1 AND 5 AND CSN9 BETWEEN 1 AND 5
    AND CSN10 BETWEEN 1 AND 5
    AND OPN1 BETWEEN 1 AND 5 AND OPN2 BETWEEN 1 AND 5 AND OPN3 BETWEEN 1 AND 5
    AND OPN4 BETWEEN 1 AND 5 AND OPN5 BETWEEN 1 AND 5 AND OPN6 BETWEEN 1 AND 5
    AND OPN7 BETWEEN 1 AND 5 AND OPN8 BETWEEN 1 AND 5 AND OPN9 BETWEEN 1 AND 5
    AND OPN10 BETWEEN 1 AND 5
    -- Remove bots and abandoned sessions.
    -- < 60s: impossible to answer 50 questions honestly.
    -- > 3600s: session likely abandoned and resumed later.
    AND testelapse BETWEEN 60 AND 3600

    -- Country must be present (required for geographic visualizations).
    AND country IS NOT NULL
    AND country != ''

    -- IPC = 1: only one record from this IP address - as recommended in the codebook.
    AND IPC = 1;
GO