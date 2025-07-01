/*
ファイル: 03_initial_signal_calculation.sql
説明: 全期間（約4年分）の37種類のシグナルを計算してd01_signals_rawに格納
作成日: 2025-01-01
修正日: 2025-06-03 - signal_date IS NOT NULL条件を追加（LEAD関数のNULL値対策）
実行時間: 約10-15分（BigQuery並列処理）
*/

-- ============================================================================
-- 1. 既存データのクリア（初期構築時のみ）
-- ============================================================================
TRUNCATE TABLE `kabu-376213.kabu2411.d01_signals_raw`;

-- ============================================================================
-- 2. 取引対象銘柄の特定
-- ============================================================================
CREATE TEMP TABLE target_stocks AS
SELECT DISTINCT
  REGEXP_REPLACE(CAST(Code AS STRING), '0$', '') AS stock_code,
  CompanyName as stock_name
FROM
  `kabu-376213.kabu2411.listed_info`
WHERE
  MarketCode IN ('0111', '0112', '0113')  -- 東証プライム、スタンダード、グロース
  AND CAST(Code AS STRING) NOT LIKE '%-T'  -- TOPIX等の指数を除外
;

-- ============================================================================
-- 3. 全期間の株価データに技術指標を追加
-- ============================================================================
CREATE TEMP TABLE quotes_with_indicators AS
WITH base_quotes AS (
  SELECT
    Date as quote_date,
    REGEXP_REPLACE(Code, '0$', '') AS stock_code,
    Open,
    High,
    Low,
    Close,
    Volume,
    Close * Volume as Value,  -- 売買代金
    -- 前日データ
    LAG(Close, 1) OVER (PARTITION BY Code ORDER BY Date) as prev_close,
    LAG(Volume, 1) OVER (PARTITION BY Code ORDER BY Date) as prev_volume,
    LAG(Close * Volume, 1) OVER (PARTITION BY Code ORDER BY Date) as prev_value,
    -- 移動平均（Close）
    AVG(Close) OVER (PARTITION BY Code ORDER BY Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as ma3_close,
    AVG(Close) OVER (PARTITION BY Code ORDER BY Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as ma5_close,
    AVG(Close) OVER (PARTITION BY Code ORDER BY Date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as ma10_close,
    AVG(Close) OVER (PARTITION BY Code ORDER BY Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as ma20_close,
    -- 移動平均（Volume）
    AVG(Volume) OVER (PARTITION BY Code ORDER BY Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as ma3_volume,
    AVG(Volume) OVER (PARTITION BY Code ORDER BY Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as ma5_volume,
    AVG(Volume) OVER (PARTITION BY Code ORDER BY Date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as ma10_volume,
    -- 移動平均（Value）
    AVG(Close * Volume) OVER (PARTITION BY Code ORDER BY Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as ma3_value,
    AVG(Close * Volume) OVER (PARTITION BY Code ORDER BY Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as ma5_value,
    AVG(Close * Volume) OVER (PARTITION BY Code ORDER BY Date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as ma10_value,
    -- 20日最大・最小
    MAX(Close) OVER (PARTITION BY Code ORDER BY Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as max20_close,
    MIN(Close) OVER (PARTITION BY Code ORDER BY Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as min20_close,
    -- 20日標準偏差
    STDDEV(Close) OVER (PARTITION BY Code ORDER BY Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as stddev20_close,
    -- スコア計算用の追加指標
    AVG(High - Open) OVER (PARTITION BY Code ORDER BY Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as avg_high_open_3d,
    AVG(Open - Low) OVER (PARTITION BY Code ORDER BY Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as avg_open_low_3d,
    AVG(High - Open) OVER (PARTITION BY Code ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as avg_high_open_7d,
    AVG(Open - Low) OVER (PARTITION BY Code ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as avg_open_low_7d,
    AVG(High - Open) OVER (PARTITION BY Code ORDER BY Date ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) as avg_high_open_9d,
    AVG(Open - Low) OVER (PARTITION BY Code ORDER BY Date ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) as avg_open_low_9d,
    AVG(High - Open) OVER (PARTITION BY Code ORDER BY Date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as avg_high_open_14d,
    AVG(Open - Low) OVER (PARTITION BY Code ORDER BY Date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as avg_open_low_14d,
    AVG(High - Open) OVER (PARTITION BY Code ORDER BY Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as avg_high_open_20d,
    AVG(Open - Low) OVER (PARTITION BY Code ORDER BY Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as avg_open_low_20d
  FROM
    `kabu-376213.kabu2411.daily_quotes`
  WHERE
    Date >= '2021-01-01'  -- 過去4年分
    AND Open > 0 AND High > 0 AND Low > 0 AND Close > 0 AND Volume > 0
)
SELECT
  bq.*,
  ts.stock_name,
  -- 次の取引日（signal_date）を取引カレンダーから正確に取得
(
  SELECT MIN(tc.Date)
  FROM `kabu-376213.kabu2411.trading_calendar` tc
  WHERE tc.Date > bq.quote_date
    AND tc.HolidayDivision = '1'
) as signal_date
FROM
  base_quotes bq
INNER JOIN
  target_stocks ts
ON
  bq.stock_code = ts.stock_code
WHERE
  bq.prev_close IS NOT NULL  -- 最初の日を除外
;

-- ============================================================================
-- 4. 7種類のシグナル計算と格納
-- ============================================================================
INSERT INTO `kabu-376213.kabu2411.d01_signals_raw`
WITH all_signals AS (
  -- 価格系（Price）8種類
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'Close to Prev Close Ratio' as signal_type, 'Price' as signal_category,
    ROUND(Close / prev_close * 100, 4) as signal_value
  FROM quotes_with_indicators
  WHERE prev_close > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'Close Change Rate' as signal_type, 'Price' as signal_category,
    ROUND((Close - prev_close) / prev_close * 100, 4) as signal_value
  FROM quotes_with_indicators
  WHERE prev_close > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'Close MA3 Deviation' as signal_type, 'Price' as signal_category,
    ROUND(Close / ma3_close * 100, 4) as signal_value
  FROM quotes_with_indicators
  WHERE ma3_close > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'Close MA5 Deviation' as signal_type, 'Price' as signal_category,
    ROUND(Close / ma5_close * 100, 4) as signal_value
  FROM quotes_with_indicators
  WHERE ma5_close > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'Close MA10 Deviation' as signal_type, 'Price' as signal_category,
    ROUND(Close / ma10_close * 100, 4) as signal_value
  FROM quotes_with_indicators
  WHERE ma10_close > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'Close to MAX20 Ratio' as signal_type, 'Price' as signal_category,
    ROUND(Close / max20_close * 100, 4) as signal_value
  FROM quotes_with_indicators
  WHERE max20_close > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'Close to MIN20 Ratio' as signal_type, 'Price' as signal_category,
    ROUND(Close / min20_close * 100, 4) as signal_value
  FROM quotes_with_indicators
  WHERE min20_close > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'Close Volatility' as signal_type, 'Price' as signal_category,
    ROUND(SAFE_DIVIDE(stddev20_close, ma20_close) * 100, 4) as signal_value
  FROM quotes_with_indicators
  WHERE ma20_close > 0 AND stddev20_close IS NOT NULL AND signal_date IS NOT NULL
  
  -- 価格レンジ系（PriceRange）5種類
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'Close to Range Ratio' as signal_type, 'PriceRange' as signal_category,
    ROUND(SAFE_DIVIDE(Close - Low, High - Low) * 100, 4) as signal_value
  FROM quotes_with_indicators
  WHERE High > Low AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'High to Close Drop Rate' as signal_type, 'PriceRange' as signal_category,
    ROUND(SAFE_DIVIDE(High - Close, High - Low) * 100, 4) as signal_value
  FROM quotes_with_indicators
  WHERE High > Low AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'Close to Low Rise Rate' as signal_type, 'PriceRange' as signal_category,
    ROUND(SAFE_DIVIDE(Close - Low, High - Low) * 100, 4) as signal_value
  FROM quotes_with_indicators
  WHERE High > Low AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'High to Close Ratio' as signal_type, 'PriceRange' as signal_category,
    ROUND(Close / High * 100, 4) as signal_value
  FROM quotes_with_indicators
  WHERE High > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'Close to Low Ratio' as signal_type, 'PriceRange' as signal_category,
    ROUND(Close / Low * 100, 4) as signal_value
  FROM quotes_with_indicators
  WHERE Low > 0 AND signal_date IS NOT NULL
  
  -- 始値終値系（OpenClose）3種類
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'Close to Open Ratio' as signal_type, 'OpenClose' as signal_category,
    ROUND(Close / Open * 100, 4) as signal_value
  FROM quotes_with_indicators
  WHERE Open > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'Open to Close Change Rate' as signal_type, 'OpenClose' as signal_category,
    ROUND((Close - Open) / Open * 100, 4) as signal_value
  FROM quotes_with_indicators
  WHERE Open > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'Open Close Range Efficiency' as signal_type, 'OpenClose' as signal_category,
    ROUND(SAFE_DIVIDE(Close - Open, High - Low) * 100, 4) as signal_value
  FROM quotes_with_indicators
  WHERE High > Low AND signal_date IS NOT NULL
  
  -- 始値系（Open）3種類
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'Open to Range Ratio' as signal_type, 'Open' as signal_category,
    ROUND(SAFE_DIVIDE(Open - Low, High - Low) * 100, 4) as signal_value
  FROM quotes_with_indicators
  WHERE High > Low AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'High to Open Drop Rate' as signal_type, 'Open' as signal_category,
    ROUND(SAFE_DIVIDE(High - Open, High - Low) * 100, 4) as signal_value
  FROM quotes_with_indicators
  WHERE High > Low AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'Open to Low Rise Rate' as signal_type, 'Open' as signal_category,
    ROUND(SAFE_DIVIDE(Open - Low, High - Low) * 100, 4) as signal_value
  FROM quotes_with_indicators
  WHERE High > Low AND signal_date IS NOT NULL
  
  -- 出来高系（Volume）4種類
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'Volume to Prev Ratio' as signal_type, 'Volume' as signal_category,
    ROUND(Volume / prev_volume * 100, 4) as signal_value
  FROM quotes_with_indicators
  WHERE prev_volume > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'Volume MA3 Deviation' as signal_type, 'Volume' as signal_category,
    ROUND(Volume / ma3_volume * 100, 4) as signal_value
  FROM quotes_with_indicators
  WHERE ma3_volume > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'Volume MA5 Deviation' as signal_type, 'Volume' as signal_category,
    ROUND(Volume / ma5_volume * 100, 4) as signal_value
  FROM quotes_with_indicators
  WHERE ma5_volume > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'Volume MA10 Deviation' as signal_type, 'Volume' as signal_category,
    ROUND(Volume / ma10_volume * 100, 4) as signal_value
  FROM quotes_with_indicators
  WHERE ma10_volume > 0 AND signal_date IS NOT NULL
  
  -- 売買代金系（Value）4種類
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'Value to Prev Ratio' as signal_type, 'Value' as signal_category,
    ROUND(Value / prev_value * 100, 4) as signal_value
  FROM quotes_with_indicators
  WHERE prev_value > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'Value MA3 Deviation' as signal_type, 'Value' as signal_category,
    ROUND(Value / ma3_value * 100, 4) as signal_value
  FROM quotes_with_indicators
  WHERE ma3_value > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'Value MA5 Deviation' as signal_type, 'Value' as signal_category,
    ROUND(Value / ma5_value * 100, 4) as signal_value
  FROM quotes_with_indicators
  WHERE ma5_value > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'Value MA10 Deviation' as signal_type, 'Value' as signal_category,
    ROUND(Value / ma10_value * 100, 4) as signal_value
  FROM quotes_with_indicators
  WHERE ma10_value > 0 AND signal_date IS NOT NULL
  
  -- スコア系（Score）10種類 - カスタム計算式
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'High Price Score 3D' as signal_type, 'Score' as signal_category,
    ROUND(
      0.5 * SAFE_DIVIDE(avg_high_open_3d, Open) * 100 +
      0.3 * SAFE_DIVIDE(High - Low, Open) * 100 +
      0.2 * SAFE_DIVIDE(Close - Open, Open) * 100
    , 4) as signal_value
  FROM quotes_with_indicators
  WHERE Open > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'High Price Score 7D' as signal_type, 'Score' as signal_category,
    ROUND(
      0.5 * SAFE_DIVIDE(avg_high_open_7d, Open) * 100 +
      0.3 * SAFE_DIVIDE(High - Low, Open) * 100 +
      0.2 * SAFE_DIVIDE(Close - Open, Open) * 100
    , 4) as signal_value
  FROM quotes_with_indicators
  WHERE Open > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'High Price Score 9D' as signal_type, 'Score' as signal_category,
    ROUND(
      0.5 * SAFE_DIVIDE(avg_high_open_9d, Open) * 100 +
      0.3 * SAFE_DIVIDE(High - Low, Open) * 100 +
      0.2 * SAFE_DIVIDE(Close - Open, Open) * 100
    , 4) as signal_value
  FROM quotes_with_indicators
  WHERE Open > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'High Price Score 14D' as signal_type, 'Score' as signal_category,
    ROUND(
      0.5 * SAFE_DIVIDE(avg_high_open_14d, Open) * 100 +
      0.3 * SAFE_DIVIDE(High - Low, Open) * 100 +
      0.2 * SAFE_DIVIDE(Close - Open, Open) * 100
    , 4) as signal_value
  FROM quotes_with_indicators
  WHERE Open > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'High Price Score 20D' as signal_type, 'Score' as signal_category,
    ROUND(
      0.5 * SAFE_DIVIDE(avg_high_open_20d, Open) * 100 +
      0.3 * SAFE_DIVIDE(High - Low, Open) * 100 +
      0.2 * SAFE_DIVIDE(Close - Open, Open) * 100
    , 4) as signal_value
  FROM quotes_with_indicators
  WHERE Open > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'Low Price Score 3D' as signal_type, 'Score' as signal_category,
    ROUND(
      0.5 * SAFE_DIVIDE(avg_open_low_3d, Open) * 100 +
      0.3 * SAFE_DIVIDE(High - Low, Open) * 100 +
      0.2 * ABS(SAFE_DIVIDE(Close - Open, Open)) * 100
    , 4) as signal_value
  FROM quotes_with_indicators
  WHERE Open > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'Low Price Score 7D' as signal_type, 'Score' as signal_category,
    ROUND(
      0.5 * SAFE_DIVIDE(avg_open_low_7d, Open) * 100 +
      0.3 * SAFE_DIVIDE(High - Low, Open) * 100 +
      0.2 * ABS(SAFE_DIVIDE(Close - Open, Open)) * 100
    , 4) as signal_value
  FROM quotes_with_indicators
  WHERE Open > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'Low Price Score 9D' as signal_type, 'Score' as signal_category,
    ROUND(
      0.5 * SAFE_DIVIDE(avg_open_low_9d, Open) * 100 +
      0.3 * SAFE_DIVIDE(High - Low, Open) * 100 +
      0.2 * ABS(SAFE_DIVIDE(Close - Open, Open)) * 100
    , 4) as signal_value
  FROM quotes_with_indicators
  WHERE Open > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'Low Price Score 14D' as signal_type, 'Score' as signal_category,
    ROUND(
      0.5 * SAFE_DIVIDE(avg_open_low_14d, Open) * 100 +
      0.3 * SAFE_DIVIDE(High - Low, Open) * 100 +
      0.2 * ABS(SAFE_DIVIDE(Close - Open, Open)) * 100
    , 4) as signal_value
  FROM quotes_with_indicators
  WHERE Open > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
    'Low Price Score 20D' as signal_type, 'Score' as signal_category,
    ROUND(
      0.5 * SAFE_DIVIDE(avg_open_low_20d, Open) * 100 +
      0.3 * SAFE_DIVIDE(High - Low, Open) * 100 +
      0.2 * ABS(SAFE_DIVIDE(Close - Open, Open)) * 100
    , 4) as signal_value
  FROM quotes_with_indicators
  WHERE Open > 0 AND signal_date IS NOT NULL
)
SELECT
  signal_date,
  reference_date,
  stock_code,
  stock_name,
  signal_type,
  signal_value,
  signal_category,
  CURRENT_TIMESTAMP() as created_at
FROM
  all_signals
WHERE
  signal_value IS NOT NULL
  AND ABS(signal_value) < 10000  -- 異常値を除外
  AND signal_date IS NOT NULL  -- NULL値を除外（修正追加）
;

-- ============================================================================
-- 5. 処理結果の確認
-- ============================================================================
SELECT 
  'シグナル計算が完了しました' AS message,
  COUNT(*) as total_records,
  COUNT(DISTINCT signal_date) as days,
  COUNT(DISTINCT stock_code) as stocks,
  COUNT(DISTINCT signal_type) as signal_types,
  MIN(signal_date) as min_date,
  MAX(signal_date) as max_date
FROM 
  `kabu-376213.kabu2411.d01_signals_raw`;

-- カテゴリ別の統計
SELECT
  signal_category,
  COUNT(DISTINCT signal_type) as types,
  COUNT(*) as records,
  ROUND(AVG(signal_value), 2) as avg_value,
  ROUND(STDDEV(signal_value), 2) as std_value
FROM
  `kabu-376213.kabu2411.d01_signals_raw`
GROUP BY
  signal_category
ORDER BY
  signal_category;