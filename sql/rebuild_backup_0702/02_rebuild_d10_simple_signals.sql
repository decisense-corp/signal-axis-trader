-- ============================================================================
-- Phase 2: d10_simple_signals 再構築（新指標中心版）
-- 作成日: 2025年7月3日
-- 目的: 新指標10種類 + 比較用既存指標7種類 = 17指標
-- 戦略: データ量削減のため古い指標を大幅削減、各カテゴリ1個ずつ残す
-- ============================================================================

-- 1. バックアップ作成（念のため）
CREATE TABLE IF NOT EXISTS `kabu-376213.kabu2411.d10_simple_signals_backup_17signals` AS
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
  description="Phase 2完成版: 17指標（新指標10 + 比較用7）。新指標による独自性確保重視。",
  partition_expiration_days=1095
);

-- 3. 新指標中心のシグナル計算・挿入（17指標版）
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
    
    -- 比較用既存指標の最小限計算
    LAG(dq.Close, 1) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date) as prev_close,
    LAG(dq.Volume, 1) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date) as prev_volume,
    LAG(dq.TurnoverValue, 1) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date) as prev_value,
    AVG(dq.Close) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as ma5_close,
    MAX(dq.Close) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as max20_close,
    
    -- 🚀 新指標計算用（High/Low Price Score用の基礎計算）
    -- 高値/始値の移動平均（各期間）
    AVG(CASE WHEN dq.Open > 0 THEN dq.High / dq.Open ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as avg_high_open_3d,
    AVG(CASE WHEN dq.Open > 0 THEN dq.High / dq.Open ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as avg_high_open_7d,
    AVG(CASE WHEN dq.Open > 0 THEN dq.High / dq.Open ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) as avg_high_open_9d,
    AVG(CASE WHEN dq.Open > 0 THEN dq.High / dq.Open ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as avg_high_open_14d,
    AVG(CASE WHEN dq.Open > 0 THEN dq.High / dq.Open ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as avg_high_open_20d,
    
    -- 始値/安値の移動平均（各期間）
    AVG(CASE WHEN dq.Low > 0 THEN dq.Open / dq.Low ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as avg_open_low_3d,
    AVG(CASE WHEN dq.Low > 0 THEN dq.Open / dq.Low ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as avg_open_low_7d,
    AVG(CASE WHEN dq.Low > 0 THEN dq.Open / dq.Low ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) as avg_open_low_9d,
    AVG(CASE WHEN dq.Low > 0 THEN dq.Open / dq.Low ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as avg_open_low_14d,
    AVG(CASE WHEN dq.Low > 0 THEN dq.Open / dq.Low ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as avg_open_low_20d
    
  FROM `kabu-376213.kabu2411.daily_quotes` dq
  INNER JOIN `kabu-376213.kabu2411.master_trading_stocks` mts
    ON REGEXP_REPLACE(dq.Code, '0$', '') = mts.stock_code
  WHERE dq.Date >= '2022-07-01'  -- 開始日
)

-- ========================================================================
-- 比較用既存指標（各カテゴリから1個ずつ、計7指標）
-- ========================================================================

-- Price signals (1 type) - Phase 7で最も劣化しやすかった指標
SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Close MA5 Deviation' as signal_type, 'Price' as signal_category,
  ROUND(Close / ma5_close * 100, 4) as signal_value
FROM quotes_data
WHERE ma5_close > 0 
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

-- PriceRange signals (1 type)
UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Close to Range Ratio' as signal_type, 'PriceRange' as signal_category,
  ROUND(SAFE_DIVIDE(Close - Low, High - Low) * 100, 4) as signal_value
FROM quotes_data
WHERE High > Low 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

-- OpenClose signals (1 type) - Phase 7で最も劣化しやすかった指標
UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Close to Open Ratio' as signal_type, 'OpenClose' as signal_category,
  ROUND(Close / Open * 100, 4) as signal_value
FROM quotes_data
WHERE Open > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

-- Open signals (1 type)
UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Open to Range Ratio' as signal_type, 'Open' as signal_category,
  ROUND(SAFE_DIVIDE(Open - Low, High - Low) * 100, 4) as signal_value
FROM quotes_data
WHERE High > Low 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

-- Volume signals (1 type) - Phase 7で最も劣化しにくかった指標
UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Volume to Prev Ratio' as signal_type, 'Volume' as signal_category,
  ROUND(Volume / prev_volume * 100, 4) as signal_value
FROM quotes_data
WHERE prev_volume > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

-- Value signals (1 type) - Phase 7で最も劣化しやすかった指標
UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Value to Prev Ratio' as signal_type, 'Value' as signal_category,
  ROUND(TurnoverValue / prev_value * 100, 4) as signal_value
FROM quotes_data
WHERE prev_value > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

-- ========================================================================
-- 🚀 新指標10種類（High Price Score 5種類 + Low Price Score 5種類）
-- ========================================================================

-- High Price Score 3D
UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'High Price Score 3D' as signal_type, 'Score' as signal_category,
  ROUND(
    CASE 
      WHEN Open > 0 AND avg_high_open_3d IS NOT NULL THEN
        COALESCE(avg_high_open_3d * 50, 0) +
        COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) +
        COALESCE(SAFE_DIVIDE(Close - Open, Open) * 20, 0)
      ELSE NULL 
    END, 4) as signal_value
FROM quotes_data
WHERE Open > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'High Price Score 7D' as signal_type, 'Score' as signal_category,
  ROUND(
    CASE 
      WHEN Open > 0 AND avg_high_open_7d IS NOT NULL THEN
        COALESCE(avg_high_open_7d * 50, 0) +
        COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) +
        COALESCE(SAFE_DIVIDE(Close - Open, Open) * 20, 0)
      ELSE NULL 
    END, 4) as signal_value
FROM quotes_data
WHERE Open > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'High Price Score 9D' as signal_type, 'Score' as signal_category,
  ROUND(
    CASE 
      WHEN Open > 0 AND avg_high_open_9d IS NOT NULL THEN
        COALESCE(avg_high_open_9d * 50, 0) +
        COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) +
        COALESCE(SAFE_DIVIDE(Close - Open, Open) * 20, 0)
      ELSE NULL 
    END, 4) as signal_value
FROM quotes_data
WHERE Open > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'High Price Score 14D' as signal_type, 'Score' as signal_category,
  ROUND(
    CASE 
      WHEN Open > 0 AND avg_high_open_14d IS NOT NULL THEN
        COALESCE(avg_high_open_14d * 50, 0) +
        COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) +
        COALESCE(SAFE_DIVIDE(Close - Open, Open) * 20, 0)
      ELSE NULL 
    END, 4) as signal_value
FROM quotes_data
WHERE Open > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'High Price Score 20D' as signal_type, 'Score' as signal_category,
  ROUND(
    CASE 
      WHEN Open > 0 AND avg_high_open_20d IS NOT NULL THEN
        COALESCE(avg_high_open_20d * 50, 0) +
        COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) +
        COALESCE(SAFE_DIVIDE(Close - Open, Open) * 20, 0)
      ELSE NULL 
    END, 4) as signal_value
FROM quotes_data
WHERE Open > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

-- Low Price Score 3D
UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Low Price Score 3D' as signal_type, 'Score' as signal_category,
  ROUND(
    CASE 
      WHEN Open > 0 AND avg_open_low_3d IS NOT NULL THEN
        COALESCE(avg_open_low_3d * 50, 0) +
        COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) +
        COALESCE(ABS(SAFE_DIVIDE(Close - Open, Open)) * 20, 0)
      ELSE NULL 
    END, 4) as signal_value
FROM quotes_data
WHERE Open > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Low Price Score 7D' as signal_type, 'Score' as signal_category,
  ROUND(
    CASE 
      WHEN Open > 0 AND avg_open_low_7d IS NOT NULL THEN
        COALESCE(avg_open_low_7d * 50, 0) +
        COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) +
        COALESCE(ABS(SAFE_DIVIDE(Close - Open, Open)) * 20, 0)
      ELSE NULL 
    END, 4) as signal_value
FROM quotes_data
WHERE Open > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Low Price Score 9D' as signal_type, 'Score' as signal_category,
  ROUND(
    CASE 
      WHEN Open > 0 AND avg_open_low_9d IS NOT NULL THEN
        COALESCE(avg_open_low_9d * 50, 0) +
        COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) +
        COALESCE(ABS(SAFE_DIVIDE(Close - Open, Open)) * 20, 0)
      ELSE NULL 
    END, 4) as signal_value
FROM quotes_data
WHERE Open > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Low Price Score 14D' as signal_type, 'Score' as signal_category,
  ROUND(
    CASE 
      WHEN Open > 0 AND avg_open_low_14d IS NOT NULL THEN
        COALESCE(avg_open_low_14d * 50, 0) +
        COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) +
        COALESCE(ABS(SAFE_DIVIDE(Close - Open, Open)) * 20, 0)
      ELSE NULL 
    END, 4) as signal_value
FROM quotes_data
WHERE Open > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date

UNION ALL

SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Low Price Score 20D' as signal_type, 'Score' as signal_category,
  ROUND(
    CASE 
      WHEN Open > 0 AND avg_open_low_20d IS NOT NULL THEN
        COALESCE(avg_open_low_20d * 50, 0) +
        COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) +
        COALESCE(ABS(SAFE_DIVIDE(Close - Open, Open)) * 20, 0)
      ELSE NULL 
    END, 4) as signal_value
FROM quotes_data
WHERE Open > 0 
  AND signal_date IS NOT NULL 
  AND signal_date > quote_date;

-- ============================================================================
-- 4. 再構築結果の確認（17指標版）
-- ============================================================================

-- 基本統計
SELECT 
  '🎉 Phase 2完了確認（17指標版）' as check_type,
  COUNT(*) as total_records,
  COUNT(DISTINCT signal_type) as signal_types_count_should_be_17,
  COUNT(DISTINCT stock_code) as stocks_count,
  MIN(signal_date) as min_signal_date,
  MAX(signal_date) as max_signal_date
FROM `kabu-376213.kabu2411.d10_simple_signals`;

-- 未来視チェック（critical check）
SELECT 
  '🚨 未来視チェック' as check_type,
  COUNT(*) as future_leak_records_should_be_0
FROM `kabu-376213.kabu2411.d10_simple_signals`
WHERE signal_date <= reference_date;

-- シグナル種類確認
SELECT 
  'シグナル種類確認' as check_type,
  signal_category,
  COUNT(DISTINCT signal_type) as signal_count,
  COUNT(*) as record_count,
  STRING_AGG(signal_type ORDER BY signal_type) as signal_list
FROM `kabu-376213.kabu2411.d10_simple_signals`
GROUP BY signal_category
ORDER BY signal_category;

-- 新指標 vs 既存指標の比較（データ量確認）
SELECT 
  '🚀 新指標 vs 既存指標比較' as check_type,
  CASE 
    WHEN signal_category = 'Score' THEN '新指標'
    ELSE '既存指標（比較用）'
  END as indicator_type,
  COUNT(DISTINCT signal_type) as signal_count,
  COUNT(*) as record_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
FROM `kabu-376213.kabu2411.d10_simple_signals`
GROUP BY 
  CASE 
    WHEN signal_category = 'Score' THEN '新指標'
    ELSE '既存指標（比較用）'
  END
ORDER BY indicator_type;

-- 処理完了メッセージ
SELECT 
  'Phase 2: d10_simple_signals (17指標版) 作成完了' as message,
  '📊 構成: 新指標10 + 比較用既存指標7 = 計17指標' as composition,
  '🎯 戦略: Phase 7で最も劣化した指標を比較対象に選定' as strategy,
  '🚀 新指標: High/Low Price Score による独自性確保' as new_features,
  '💾 データ量: 大幅削減により安定実行' as performance,
  '📈 期待: 新指標の劣化 < 既存指標の15-17%劣化' as hypothesis,
  '⚡ 次段階: Phase 3 (m30_signal_bins 17指標境界値計算) 実行可能' as next_step,
  CURRENT_DATETIME('Asia/Tokyo') as completion_time;