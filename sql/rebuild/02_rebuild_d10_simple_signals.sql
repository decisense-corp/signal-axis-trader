-- ============================================================================
-- d10_simple_signals 完全再構築（重複削除済みdaily_quotesベース）
-- 説明: クリーンなdaily_quotesから未来視なしのシグナルデータを生成
-- 実行日: 2025年7月2日
-- 重複問題・未来視問題: 解決済み
-- ============================================================================

-- 1. バックアップ作成
CREATE TABLE IF NOT EXISTS `kabu-376213.kabu2411.d10_simple_signals_backup_rebuild` AS
SELECT * FROM `kabu-376213.kabu2411.d10_simple_signals`;

-- 2. 既存テーブル削除・新規作成
DROP TABLE IF EXISTS `kabu-376213.kabu2411.d10_simple_signals`;

CREATE TABLE `kabu-376213.kabu2411.d10_simple_signals` (
  signal_date DATE,
  reference_date DATE,
  stock_code STRING,
  stock_name STRING,
  signal_type STRING,
  signal_category STRING,
  signal_value FLOAT64,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY signal_date
CLUSTER BY stock_code, signal_type
OPTIONS(
  description="Daily signal calculation values history table (clean version). 27 types of signals.",
  partition_expiration_days=1095
);

-- 3. クリーンなシグナル計算・挿入
INSERT INTO `kabu-376213.kabu2411.d10_simple_signals`
(signal_date, reference_date, stock_code, stock_name, signal_type, signal_category, signal_value)

WITH quotes_data AS (
  SELECT 
    REGEXP_REPLACE(dq.Code, '0$', '') as stock_code,
    mts.company_name as stock_name,
    dq.Date as quote_date,
    -- 営業日カレンダーを使用した安全な翌営業日計算
    (
      SELECT MIN(tc.Date)
      FROM `kabu-376213.kabu2411.trading_calendar` tc
      WHERE tc.Date > dq.Date
        AND tc.HolidayDivision = '1'
    ) as signal_date,
    dq.Open,
    dq.High,
    dq.Low,
    dq.Close,
    dq.Volume,
    dq.TurnoverValue,
    
    -- 前日データ
    LAG(dq.Close, 1) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date) as prev_close,
    LAG(dq.Volume, 1) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date) as prev_volume,
    LAG(dq.TurnoverValue, 1) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date) as prev_value,
    
    -- 移動平均（Close）
    AVG(dq.Close) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as ma3_close,
    AVG(dq.Close) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as ma5_close,
    AVG(dq.Close) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as ma10_close,
    AVG(dq.Close) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as ma20_close,
    
    -- 移動平均（Volume）
    AVG(dq.Volume) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as ma3_volume,
    AVG(dq.Volume) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as ma5_volume,
    AVG(dq.Volume) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as ma10_volume,
    
    -- 移動平均（TurnoverValue）
    AVG(dq.TurnoverValue) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as ma3_value,
    AVG(dq.TurnoverValue) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as ma5_value,
    AVG(dq.TurnoverValue) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as ma10_value,
    
    -- 最高値・最安値
    MAX(dq.Close) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as max20_close,
    MIN(dq.Close) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as min20_close,
    
    -- 標準偏差
    STDDEV(dq.Close) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as stddev20_close
    
  FROM `kabu-376213.kabu2411.daily_quotes` dq
  INNER JOIN `kabu-376213.kabu2411.master_trading_stocks` mts
    ON REGEXP_REPLACE(dq.Code, '0$', '') = mts.stock_code
  WHERE dq.Date >= '2022-07-01'  -- 27種類システム開始日
)

-- Price signals (8 types)
SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Close to Prev Close Ratio' as signal_type, 'Price' as signal_category,
  ROUND(Close / prev_close * 100, 4) as signal_value
FROM quotes_data
WHERE prev_close > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date  -- 未来視防止

UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Close Change Rate' as signal_type, 'Price' as signal_category,
  ROUND((Close - prev_close) / prev_close * 100, 4) as signal_value
FROM quotes_data
WHERE prev_close > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Close MA3 Deviation' as signal_type, 'Price' as signal_category,
  ROUND(Close / ma3_close * 100, 4) as signal_value
FROM quotes_data
WHERE ma3_close > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Close MA5 Deviation' as signal_type, 'Price' as signal_category,
  ROUND(Close / ma5_close * 100, 4) as signal_value
FROM quotes_data
WHERE ma5_close > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Close MA10 Deviation' as signal_type, 'Price' as signal_category,
  ROUND(Close / ma10_close * 100, 4) as signal_value
FROM quotes_data
WHERE ma10_close > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Close to MAX20 Ratio' as signal_type, 'Price' as signal_category,
  ROUND(Close / max20_close * 100, 4) as signal_value
FROM quotes_data
WHERE max20_close > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Close to MIN20 Ratio' as signal_type, 'Price' as signal_category,
  ROUND(Close / min20_close * 100, 4) as signal_value
FROM quotes_data
WHERE min20_close > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Close Volatility' as signal_type, 'Price' as signal_category,
  ROUND(SAFE_DIVIDE(stddev20_close, ma20_close) * 100, 4) as signal_value
FROM quotes_data
WHERE ma20_close > 0 AND stddev20_close IS NOT NULL 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

-- PriceRange signals (5 types)
UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Close to Range Ratio' as signal_type, 'PriceRange' as signal_category,
  ROUND(SAFE_DIVIDE(Close - Low, High - Low) * 100, 4) as signal_value
FROM quotes_data
WHERE High > Low 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'High to Close Drop Rate' as signal_type, 'PriceRange' as signal_category,
  ROUND(SAFE_DIVIDE(High - Close, High - Low) * 100, 4) as signal_value
FROM quotes_data
WHERE High > Low 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Close to Low Rise Rate' as signal_type, 'PriceRange' as signal_category,
  ROUND(SAFE_DIVIDE(Close - Low, High - Low) * 100, 4) as signal_value
FROM quotes_data
WHERE High > Low 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'High to Close Ratio' as signal_type, 'PriceRange' as signal_category,
  ROUND(Close / High * 100, 4) as signal_value
FROM quotes_data
WHERE High > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Close to Low Ratio' as signal_type, 'PriceRange' as signal_category,
  ROUND(Close / Low * 100, 4) as signal_value
FROM quotes_data
WHERE Low > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

-- OpenClose signals (3 types)
UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Close to Open Ratio' as signal_type, 'OpenClose' as signal_category,
  ROUND(Close / Open * 100, 4) as signal_value
FROM quotes_data
WHERE Open > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Open to Close Change Rate' as signal_type, 'OpenClose' as signal_category,
  ROUND((Close - Open) / Open * 100, 4) as signal_value
FROM quotes_data
WHERE Open > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Open Close Range Efficiency' as signal_type, 'OpenClose' as signal_category,
  ROUND(SAFE_DIVIDE(Close - Open, High - Low) * 100, 4) as signal_value
FROM quotes_data
WHERE High > Low 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

-- Open signals (3 types)
UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Open to Range Ratio' as signal_type, 'Open' as signal_category,
  ROUND(SAFE_DIVIDE(Open - Low, High - Low) * 100, 4) as signal_value
FROM quotes_data
WHERE High > Low 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'High to Open Drop Rate' as signal_type, 'Open' as signal_category,
  ROUND(SAFE_DIVIDE(High - Open, High - Low) * 100, 4) as signal_value
FROM quotes_data
WHERE High > Low 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Open to Low Rise Rate' as signal_type, 'Open' as signal_category,
  ROUND(SAFE_DIVIDE(Open - Low, High - Low) * 100, 4) as signal_value
FROM quotes_data
WHERE High > Low 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

-- Volume signals (4 types)
UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Volume to Prev Ratio' as signal_type, 'Volume' as signal_category,
  ROUND(Volume / prev_volume * 100, 4) as signal_value
FROM quotes_data
WHERE prev_volume > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Volume MA3 Deviation' as signal_type, 'Volume' as signal_category,
  ROUND(Volume / ma3_volume * 100, 4) as signal_value
FROM quotes_data
WHERE ma3_volume > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Volume MA5 Deviation' as signal_type, 'Volume' as signal_category,
  ROUND(Volume / ma5_volume * 100, 4) as signal_value
FROM quotes_data
WHERE ma5_volume > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Volume MA10 Deviation' as signal_type, 'Volume' as signal_category,
  ROUND(Volume / ma10_volume * 100, 4) as signal_value
FROM quotes_data
WHERE ma10_volume > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

-- Value signals (4 types)
UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Value to Prev Ratio' as signal_type, 'Value' as signal_category,
  ROUND(TurnoverValue / prev_value * 100, 4) as signal_value
FROM quotes_data
WHERE prev_value > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Value MA3 Deviation' as signal_type, 'Value' as signal_category,
  ROUND(TurnoverValue / ma3_value * 100, 4) as signal_value
FROM quotes_data
WHERE ma3_value > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Value MA5 Deviation' as signal_type, 'Value' as signal_category,
  ROUND(TurnoverValue / ma5_value * 100, 4) as signal_value
FROM quotes_data
WHERE ma5_value > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Value MA10 Deviation' as signal_type, 'Value' as signal_category,
  ROUND(TurnoverValue / ma10_value * 100, 4) as signal_value
FROM quotes_data
WHERE ma10_value > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date;

-- ============================================================================
-- 4. 再構築結果の確認
-- ============================================================================

-- 基本統計
SELECT 
  'Rebuild completion check' as check_type,
  COUNT(*) as total_records,
  COUNT(DISTINCT signal_type) as signal_types_count,
  COUNT(DISTINCT stock_code) as stocks_count,
  MIN(signal_date) as min_signal_date,
  MAX(signal_date) as max_signal_date,
  MIN(reference_date) as min_reference_date,
  MAX(reference_date) as max_reference_date
FROM `kabu-376213.kabu2411.d10_simple_signals`;

-- 未来視チェック（critical check）
SELECT 
  'Future leak check' as check_type,
  COUNT(*) as future_leak_records
FROM `kabu-376213.kabu2411.d10_simple_signals`
WHERE signal_date <= reference_date;

-- シグナル種類確認
SELECT 
  'Signal types verification' as check_type,
  signal_type,
  signal_category,
  COUNT(*) as record_count,
  COUNT(DISTINCT stock_code) as stock_count
FROM `kabu-376213.kabu2411.d10_simple_signals`
GROUP BY signal_type, signal_category
ORDER BY signal_category, signal_type;