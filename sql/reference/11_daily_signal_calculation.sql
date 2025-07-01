/*
ファイル: 11_daily_signal_calculation.sql
説明: 当日（最新営業日）のシグナルを計算してd01_signals_rawに追加
作成日: 2025-01-01
修正日: 2025-06-02 - 変数宣言を最初にまとめて配置
修正日: 2025-06-03 - signal_date IS NOT NULL条件を追加（LEAD関数のNULL値対策）
修正日: 2025-06-03 - 日付範囲を拡大してLAG関数が正常に動作するよう修正
実行時間: 約1-2分（日次処理）
*/

-- ============================================================================
-- 1. 処理対象日の特定（すべての変数宣言を最初に配置）
-- ============================================================================
DECLARE target_date DATE DEFAULT (
  SELECT MAX(Date) 
  FROM `kabu-376213.kabu2411.daily_quotes`
  WHERE Date <= CURRENT_DATE('Asia/Tokyo')
);

DECLARE signal_date DATE DEFAULT (
  -- 翌営業日を取得（土日祝日を考慮）
  SELECT MIN(Date)
  FROM `kabu-376213.kabu2411.trading_calendar`
  WHERE Date > target_date
    AND HolidayDivision = '1'  -- 営業日
);

DECLARE existing_count INT64 DEFAULT (
  SELECT COUNT(*)
  FROM `kabu-376213.kabu2411.d01_signals_raw`
  WHERE reference_date = target_date
);

-- 処理対象日の確認
SELECT 
  target_date as reference_date,
  signal_date as signal_date,
  DATE_DIFF(signal_date, target_date, DAY) as days_ahead,
  FORMAT_DATE('%A', target_date) as reference_day,
  FORMAT_DATE('%A', signal_date) as signal_day;

-- ============================================================================
-- 2. 既存データの確認と処理（重複防止）
-- ============================================================================
-- 既にデータが存在する場合は削除して再作成
IF existing_count > 0 THEN
  DELETE FROM `kabu-376213.kabu2411.d01_signals_raw`
  WHERE reference_date = target_date;
  
  SELECT 
    FORMAT('既存の%sのシグナル（%d件）を削除しました', 
           CAST(target_date AS STRING), 
           existing_count) as message;
END IF;

-- ============================================================================
-- 3. 取引対象銘柄の特定（日次更新対応）
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
-- 4. 最新の株価データに技術指標を追加
-- ============================================================================
CREATE TEMP TABLE latest_quotes_with_indicators AS
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
    -- 修正: 十分な過去データを取得（60営業日前から）
    Date >= DATE_SUB(target_date, INTERVAL 60 DAY)
    AND Date <= target_date
    AND Open > 0 AND High > 0 AND Low > 0 AND Close > 0 AND Volume > 0
)
SELECT
  bq.*,
  ts.stock_name
FROM
  base_quotes bq
INNER JOIN
  target_stocks ts
ON
  bq.stock_code = ts.stock_code
WHERE
  bq.quote_date = target_date  -- 最新日のみ
  AND bq.prev_close IS NOT NULL  -- 技術指標が計算可能
;

-- ============================================================================
-- 5. 37種類のシグナル計算と格納
-- ============================================================================
INSERT INTO `kabu-376213.kabu2411.d01_signals_raw`
WITH all_signals AS (
  -- 価格系（Price）8種類
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'Close to Prev Close Ratio' as signal_type, 'Price' as signal_category,
    ROUND(Close / prev_close * 100, 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE prev_close > 0
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'Close Change Rate' as signal_type, 'Price' as signal_category,
    ROUND((Close - prev_close) / prev_close * 100, 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE prev_close > 0
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'Close MA3 Deviation' as signal_type, 'Price' as signal_category,
    ROUND(Close / ma3_close * 100, 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE ma3_close > 0
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'Close MA5 Deviation' as signal_type, 'Price' as signal_category,
    ROUND(Close / ma5_close * 100, 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE ma5_close > 0
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'Close MA10 Deviation' as signal_type, 'Price' as signal_category,
    ROUND(Close / ma10_close * 100, 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE ma10_close > 0
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'Close to MAX20 Ratio' as signal_type, 'Price' as signal_category,
    ROUND(Close / max20_close * 100, 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE max20_close > 0
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'Close to MIN20 Ratio' as signal_type, 'Price' as signal_category,
    ROUND(Close / min20_close * 100, 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE min20_close > 0
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'Close Volatility' as signal_type, 'Price' as signal_category,
    ROUND(SAFE_DIVIDE(stddev20_close, ma20_close) * 100, 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE ma20_close > 0 AND stddev20_close IS NOT NULL
  
  -- 価格レンジ系（PriceRange）5種類
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'Close to Range Ratio' as signal_type, 'PriceRange' as signal_category,
    ROUND(SAFE_DIVIDE(Close - Low, High - Low) * 100, 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE High > Low
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'High to Close Drop Rate' as signal_type, 'PriceRange' as signal_category,
    ROUND(SAFE_DIVIDE(High - Close, High - Low) * 100, 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE High > Low
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'Close to Low Rise Rate' as signal_type, 'PriceRange' as signal_category,
    ROUND(SAFE_DIVIDE(Close - Low, High - Low) * 100, 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE High > Low
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'High to Close Ratio' as signal_type, 'PriceRange' as signal_category,
    ROUND(Close / High * 100, 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE High > 0
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'Close to Low Ratio' as signal_type, 'PriceRange' as signal_category,
    ROUND(Close / Low * 100, 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE Low > 0
  
  -- 始値終値系（OpenClose）3種類
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'Close to Open Ratio' as signal_type, 'OpenClose' as signal_category,
    ROUND(Close / Open * 100, 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE Open > 0
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'Open to Close Change Rate' as signal_type, 'OpenClose' as signal_category,
    ROUND((Close - Open) / Open * 100, 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE Open > 0
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'Open Close Range Efficiency' as signal_type, 'OpenClose' as signal_category,
    ROUND(SAFE_DIVIDE(Close - Open, High - Low) * 100, 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE High > Low
  
  -- 始値系（Open）3種類
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'Open to Range Ratio' as signal_type, 'Open' as signal_category,
    ROUND(SAFE_DIVIDE(Open - Low, High - Low) * 100, 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE High > Low
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'High to Open Drop Rate' as signal_type, 'Open' as signal_category,
    ROUND(SAFE_DIVIDE(High - Open, High - Low) * 100, 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE High > Low
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'Open to Low Rise Rate' as signal_type, 'Open' as signal_category,
    ROUND(SAFE_DIVIDE(Open - Low, High - Low) * 100, 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE High > Low
  
  -- 出来高系（Volume）4種類
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'Volume to Prev Ratio' as signal_type, 'Volume' as signal_category,
    ROUND(Volume / prev_volume * 100, 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE prev_volume > 0
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'Volume MA3 Deviation' as signal_type, 'Volume' as signal_category,
    ROUND(Volume / ma3_volume * 100, 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE ma3_volume > 0
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'Volume MA5 Deviation' as signal_type, 'Volume' as signal_category,
    ROUND(Volume / ma5_volume * 100, 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE ma5_volume > 0
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'Volume MA10 Deviation' as signal_type, 'Volume' as signal_category,
    ROUND(Volume / ma10_volume * 100, 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE ma10_volume > 0
  
  -- 売買代金系（Value）4種類
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'Value to Prev Ratio' as signal_type, 'Value' as signal_category,
    ROUND(Value / prev_value * 100, 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE prev_value > 0
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'Value MA3 Deviation' as signal_type, 'Value' as signal_category,
    ROUND(Value / ma3_value * 100, 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE ma3_value > 0
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'Value MA5 Deviation' as signal_type, 'Value' as signal_category,
    ROUND(Value / ma5_value * 100, 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE ma5_value > 0
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'Value MA10 Deviation' as signal_type, 'Value' as signal_category,
    ROUND(Value / ma10_value * 100, 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE ma10_value > 0
  
  -- スコア系（Score）10種類 - カスタム計算式
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'High Price Score 3D' as signal_type, 'Score' as signal_category,
    ROUND(
      0.5 * SAFE_DIVIDE(avg_high_open_3d, Open) * 100 +
      0.3 * SAFE_DIVIDE(High - Low, Open) * 100 +
      0.2 * SAFE_DIVIDE(Close - Open, Open) * 100
    , 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE Open > 0
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'High Price Score 7D' as signal_type, 'Score' as signal_category,
    ROUND(
      0.5 * SAFE_DIVIDE(avg_high_open_7d, Open) * 100 +
      0.3 * SAFE_DIVIDE(High - Low, Open) * 100 +
      0.2 * SAFE_DIVIDE(Close - Open, Open) * 100
    , 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE Open > 0
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'High Price Score 9D' as signal_type, 'Score' as signal_category,
    ROUND(
      0.5 * SAFE_DIVIDE(avg_high_open_9d, Open) * 100 +
      0.3 * SAFE_DIVIDE(High - Low, Open) * 100 +
      0.2 * SAFE_DIVIDE(Close - Open, Open) * 100
    , 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE Open > 0
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'High Price Score 14D' as signal_type, 'Score' as signal_category,
    ROUND(
      0.5 * SAFE_DIVIDE(avg_high_open_14d, Open) * 100 +
      0.3 * SAFE_DIVIDE(High - Low, Open) * 100 +
      0.2 * SAFE_DIVIDE(Close - Open, Open) * 100
    , 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE Open > 0
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'High Price Score 20D' as signal_type, 'Score' as signal_category,
    ROUND(
      0.5 * SAFE_DIVIDE(avg_high_open_20d, Open) * 100 +
      0.3 * SAFE_DIVIDE(High - Low, Open) * 100 +
      0.2 * SAFE_DIVIDE(Close - Open, Open) * 100
    , 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE Open > 0
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'Low Price Score 3D' as signal_type, 'Score' as signal_category,
    ROUND(
      0.5 * SAFE_DIVIDE(avg_open_low_3d, Open) * 100 +
      0.3 * SAFE_DIVIDE(High - Low, Open) * 100 +
      0.2 * ABS(SAFE_DIVIDE(Close - Open, Open)) * 100
    , 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE Open > 0
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'Low Price Score 7D' as signal_type, 'Score' as signal_category,
    ROUND(
      0.5 * SAFE_DIVIDE(avg_open_low_7d, Open) * 100 +
      0.3 * SAFE_DIVIDE(High - Low, Open) * 100 +
      0.2 * ABS(SAFE_DIVIDE(Close - Open, Open)) * 100
    , 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE Open > 0
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'Low Price Score 9D' as signal_type, 'Score' as signal_category,
    ROUND(
      0.5 * SAFE_DIVIDE(avg_open_low_9d, Open) * 100 +
      0.3 * SAFE_DIVIDE(High - Low, Open) * 100 +
      0.2 * ABS(SAFE_DIVIDE(Close - Open, Open)) * 100
    , 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE Open > 0
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'Low Price Score 14D' as signal_type, 'Score' as signal_category,
    ROUND(
      0.5 * SAFE_DIVIDE(avg_open_low_14d, Open) * 100 +
      0.3 * SAFE_DIVIDE(High - Low, Open) * 100 +
      0.2 * ABS(SAFE_DIVIDE(Close - Open, Open)) * 100
    , 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE Open > 0
  
  UNION ALL
  
  SELECT signal_date, target_date as reference_date, stock_code, stock_name,
    'Low Price Score 20D' as signal_type, 'Score' as signal_category,
    ROUND(
      0.5 * SAFE_DIVIDE(avg_open_low_20d, Open) * 100 +
      0.3 * SAFE_DIVIDE(High - Low, Open) * 100 +
      0.2 * ABS(SAFE_DIVIDE(Close - Open, Open)) * 100
    , 4) as signal_value
  FROM latest_quotes_with_indicators
  WHERE Open > 0
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
-- 6. 処理結果の確認
-- ============================================================================
WITH process_summary AS (
  SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT stock_code) as stocks,
    COUNT(DISTINCT signal_type) as signal_types,
    reference_date,
    signal_date
  FROM 
    `kabu-376213.kabu2411.d01_signals_raw`
  WHERE
    reference_date = target_date
  GROUP BY
    reference_date, signal_date
)
SELECT 
  FORMAT('シグナル計算が完了しました（%s → %s）', 
         CAST(reference_date AS STRING), 
         CAST(signal_date AS STRING)) AS message,
  total_records,
  stocks,
  signal_types,
  ROUND(total_records / stocks / signal_types, 1) as avg_records_per_combination
FROM 
  process_summary;

-- カテゴリ別の統計
SELECT
  signal_category,
  COUNT(DISTINCT signal_type) as types,
  COUNT(*) as records,
  COUNT(DISTINCT stock_code) as stocks,
  ROUND(AVG(signal_value), 2) as avg_value,
  ROUND(STDDEV(signal_value), 2) as std_value
FROM
  `kabu-376213.kabu2411.d01_signals_raw`
WHERE
  reference_date = target_date
GROUP BY
  signal_category
ORDER BY
  signal_category;