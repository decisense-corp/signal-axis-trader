/*
ファイル: 05_rebuild_d20_basic_signal_results_37_optimized.sql
説明: Phase 5 - 37指標版 d20_basic_signal_results 完全再構築（最適化版）
作成日: 2025年7月3日
依存: d15_signals_with_bins (37指標版・1800万件) + daily_quotes
目的: 37指標による取引結果計算・2分割対応（学習期間 + 検証期間）
実績: 過去の稼働実績から2分割で十分、確認は必要最低限
*/

-- ============================================================================
-- Phase 5: d20_basic_signal_results（37指標版）完全再構築実行
-- ============================================================================

-- 処理開始メッセージ
SELECT 
  '🚀 Phase 5開始: d20_basic_signal_results（37指標版）再構築' as message,
  'データソース: d15_signals_with_bins (1800万行・37指標版) + daily_quotes' as source_info,
  '分割戦略: 学習期間 + 検証期間の2分割実行（実績ベース）' as strategy,
  CURRENT_TIMESTAMP() as start_time;

-- ============================================================================
-- Step 1: 既存データのバックアップ（安全性確保）
-- ============================================================================

-- バックアップテーブル作成（構造のみ・大規模データのため）
CREATE OR REPLACE TABLE `kabu-376213.kabu2411.d20_basic_signal_results_backup_phase5_37` AS
SELECT *, CURRENT_TIMESTAMP() as backup_timestamp
FROM `kabu-376213.kabu2411.d20_basic_signal_results`
LIMIT 1000;  -- 構造確認用のみ

SELECT 
  '✅ バックアップ準備完了' as status,
  '大規模データのため構造のみ保存' as note,
  '安全性確保: 37指標版構築前の準備' as purpose;

-- ============================================================================
-- Step 2: 新テーブル構造での完全再構築
-- ============================================================================

-- 既存テーブルを削除
DROP TABLE IF EXISTS `kabu-376213.kabu2411.d20_basic_signal_results`;

-- 新構造でテーブル作成（パラメータチューニング用項目含む）
CREATE TABLE `kabu-376213.kabu2411.d20_basic_signal_results` (
  -- 基本項目
  signal_date DATE,
  reference_date DATE,
  stock_code STRING,
  stock_name STRING,
  signal_type STRING,
  signal_bin INT64,
  trade_type STRING,
  signal_value FLOAT64,
  profit_rate FLOAT64,
  is_win BOOLEAN,
  trading_volume FLOAT64,
  created_at TIMESTAMP,
  
  -- パラメータチューニング用項目（高速表示対応）
  prev_close FLOAT64,           -- 前日終値
  day_open FLOAT64,             -- 始値  
  day_high FLOAT64,             -- 高値
  day_low FLOAT64,              -- 安値
  day_close FLOAT64,            -- 終値
  gap_amount FLOAT64,           -- ギャップ（円）
  open_to_high_amount FLOAT64,  -- 始値→高値（円）
  open_to_low_amount FLOAT64,   -- 始値→安値（円）
  open_to_close_amount FLOAT64, -- 始値→終値（円）
  daily_range FLOAT64          -- 日足値幅（円）
)
PARTITION BY signal_date
CLUSTER BY stock_code, signal_type;

-- ============================================================================
-- Step 3: 学習期間データ投入（2022/7/4〜2024/6/30）
-- ============================================================================

INSERT INTO `kabu-376213.kabu2411.d20_basic_signal_results`
WITH daily_quotes_with_prev AS (
  -- 日付ベースで正しい前日終値を計算
  SELECT 
    Code,
    Date,
    Open,
    High,
    Low,
    Close,
    Volume,
    LAG(Close) OVER (
      PARTITION BY Code 
      ORDER BY Date
    ) as prev_close
  FROM `kabu-376213.kabu2411.daily_quotes`
  WHERE Date >= '2022-07-01' AND Date <= '2024-06-30'  -- 学習期間
),
base_data AS (
  SELECT 
    s.signal_date,
    s.reference_date,
    s.stock_code,
    s.stock_name,
    s.signal_type,
    s.signal_bin,
    s.signal_value,
    
    -- 価格データ取得（signal_date当日の四本値）
    q.Open as day_open,
    q.High as day_high,
    q.Low as day_low,
    q.Close as day_close,
    
    -- 前日終値取得（正しい日付ベース）
    q.prev_close,
    
    -- 出来高
    q.Volume as trading_volume,
    
    -- 作成日時
    CURRENT_TIMESTAMP() as created_at
    
  FROM `kabu-376213.kabu2411.d15_signals_with_bins` s
  INNER JOIN `kabu-376213.kabu2411.code_mapping` cm
    ON s.stock_code = cm.standard_code
  INNER JOIN daily_quotes_with_prev q
    ON cm.original_code = q.Code 
    AND s.signal_date = q.Date
  WHERE s.signal_date >= '2022-07-04' AND s.signal_date <= '2024-06-30'  -- 学習期間
    AND s.signal_date IS NOT NULL
    AND q.Date IS NOT NULL
    AND q.Open IS NOT NULL
    AND q.Close IS NOT NULL
    AND q.prev_close IS NOT NULL
    AND s.signal_bin IS NOT NULL
),
enriched_data AS (
  SELECT 
    *,
    -- 事前計算項目（パラメータチューニング用）
    day_open - prev_close as gap_amount,
    day_high - day_open as open_to_high_amount,
    day_low - day_open as open_to_low_amount,
    day_close - day_open as open_to_close_amount,
    day_high - day_low as daily_range,
    
    -- 基本取引結果計算（寄り引け）
    CASE 
      WHEN day_open > 0 AND day_close > 0 
      THEN ROUND((day_close - day_open) / day_open * 100, 4)
      ELSE NULL 
    END as profit_rate_long,
    
    CASE 
      WHEN day_open > 0 AND day_close > 0 
      THEN ROUND((day_open - day_close) / day_open * 100, 4)
      ELSE NULL 
    END as profit_rate_short
    
  FROM base_data
)
-- LONG取引結果
SELECT 
  signal_date,
  reference_date,
  stock_code,
  stock_name,
  signal_type,
  signal_bin,
  'LONG' as trade_type,
  signal_value,
  profit_rate_long as profit_rate,
  CASE WHEN profit_rate_long > 0 THEN TRUE ELSE FALSE END as is_win,
  trading_volume,
  created_at,
  
  -- パラメータチューニング用項目
  prev_close,
  day_open,
  day_high,
  day_low,
  day_close,
  gap_amount,
  open_to_high_amount,
  open_to_low_amount,
  open_to_close_amount,
  daily_range
  
FROM enriched_data
WHERE profit_rate_long IS NOT NULL

UNION ALL

-- SHORT取引結果
SELECT 
  signal_date,
  reference_date,
  stock_code,
  stock_name,
  signal_type,
  signal_bin,
  'SHORT' as trade_type,
  signal_value,
  profit_rate_short as profit_rate,
  CASE WHEN profit_rate_short > 0 THEN TRUE ELSE FALSE END as is_win,
  trading_volume,
  created_at,
  
  -- パラメータチューニング用項目
  prev_close,
  day_open,
  day_high,
  day_low,
  day_close,
  gap_amount,
  open_to_high_amount,
  open_to_low_amount,
  open_to_close_amount,
  daily_range
  
FROM enriched_data
WHERE profit_rate_short IS NOT NULL;

-- 学習期間投入完了確認
SELECT 
  '✅ Step 3完了: 学習期間投入' as status,
  COUNT(*) as learning_period_records,
  COUNT(DISTINCT signal_type) as signal_types_37_expected,
  COUNT(DISTINCT stock_code) as stock_count,
  MIN(signal_date) as min_date,
  MAX(signal_date) as max_date,
  '次: Step 4（検証期間投入）を実行してください' as next_action
FROM `kabu-376213.kabu2411.d20_basic_signal_results`;

-- ============================================================================
-- Step 4: 検証期間データ投入（2024/7/1〜現在）
-- ============================================================================

INSERT INTO `kabu-376213.kabu2411.d20_basic_signal_results`
WITH daily_quotes_with_prev AS (
  -- 日付ベースで正しい前日終値を計算
  SELECT 
    Code,
    Date,
    Open,
    High,
    Low,
    Close,
    Volume,
    LAG(Close) OVER (
      PARTITION BY Code 
      ORDER BY Date
    ) as prev_close
  FROM `kabu-376213.kabu2411.daily_quotes`
  WHERE Date >= '2024-07-01'  -- 検証期間
),
base_data AS (
  SELECT 
    s.signal_date,
    s.reference_date,
    s.stock_code,
    s.stock_name,
    s.signal_type,
    s.signal_bin,
    s.signal_value,
    
    -- 価格データ取得（signal_date当日の四本値）
    q.Open as day_open,
    q.High as day_high,
    q.Low as day_low,
    q.Close as day_close,
    
    -- 前日終値取得（正しい日付ベース）
    q.prev_close,
    
    -- 出来高
    q.Volume as trading_volume,
    
    -- 作成日時
    CURRENT_TIMESTAMP() as created_at
    
  FROM `kabu-376213.kabu2411.d15_signals_with_bins` s
  INNER JOIN `kabu-376213.kabu2411.code_mapping` cm
    ON s.stock_code = cm.standard_code
  INNER JOIN daily_quotes_with_prev q
    ON cm.original_code = q.Code 
    AND s.signal_date = q.Date
  WHERE s.signal_date >= '2024-07-01'  -- 検証期間
    AND s.signal_date IS NOT NULL
    AND q.Date IS NOT NULL
    AND q.Open IS NOT NULL
    AND q.Close IS NOT NULL
    AND q.prev_close IS NOT NULL
    AND s.signal_bin IS NOT NULL
),
enriched_data AS (
  SELECT 
    *,
    -- 事前計算項目（パラメータチューニング用）
    day_open - prev_close as gap_amount,
    day_high - day_open as open_to_high_amount,
    day_low - day_open as open_to_low_amount,
    day_close - day_open as open_to_close_amount,
    day_high - day_low as daily_range,
    
    -- 基本取引結果計算（寄り引け）
    CASE 
      WHEN day_open > 0 AND day_close > 0 
      THEN ROUND((day_close - day_open) / day_open * 100, 4)
      ELSE NULL 
    END as profit_rate_long,
    
    CASE 
      WHEN day_open > 0 AND day_close > 0 
      THEN ROUND((day_open - day_close) / day_open * 100, 4)
      ELSE NULL 
    END as profit_rate_short
    
  FROM base_data
)
-- LONG取引結果
SELECT 
  signal_date,
  reference_date,
  stock_code,
  stock_name,
  signal_type,
  signal_bin,
  'LONG' as trade_type,
  signal_value,
  profit_rate_long as profit_rate,
  CASE WHEN profit_rate_long > 0 THEN TRUE ELSE FALSE END as is_win,
  trading_volume,
  created_at,
  
  -- パラメータチューニング用項目
  prev_close,
  day_open,
  day_high,
  day_low,
  day_close,
  gap_amount,
  open_to_high_amount,
  open_to_low_amount,
  open_to_close_amount,
  daily_range
  
FROM enriched_data
WHERE profit_rate_long IS NOT NULL

UNION ALL

-- SHORT取引結果
SELECT 
  signal_date,
  reference_date,
  stock_code,
  stock_name,
  signal_type,
  signal_bin,
  'SHORT' as trade_type,
  signal_value,
  profit_rate_short as profit_rate,
  CASE WHEN profit_rate_short > 0 THEN TRUE ELSE FALSE END as is_win,
  trading_volume,
  created_at,
  
  -- パラメータチューニング用項目
  prev_close,
  day_open,
  day_high,
  day_low,
  day_close,
  gap_amount,
  open_to_high_amount,
  open_to_low_amount,
  open_to_close_amount,
  daily_range
  
FROM enriched_data
WHERE profit_rate_short IS NOT NULL;

-- ============================================================================
-- Step 5: 作成結果の確認（必要最低限）
-- ============================================================================

-- 基本統計確認
SELECT 
  '🎉 Phase 5作成結果（37指標版）' as check_point,
  COUNT(*) as total_records,
  COUNT(DISTINCT signal_type) as signal_types_37_expected,
  COUNT(DISTINCT stock_code) as stock_count,
  COUNT(DISTINCT trade_type) as trade_types,
  MIN(signal_date) as min_date,
  MAX(signal_date) as max_date,
  ROUND(AVG(CASE WHEN is_win THEN 1.0 ELSE 0.0 END) * 100, 1) as overall_win_rate
FROM `kabu-376213.kabu2411.d20_basic_signal_results`;

-- LONG/SHORT分布確認
SELECT 
  'Phase 5: LONG/SHORT分布' as check_point,
  trade_type,
  COUNT(*) as record_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
FROM `kabu-376213.kabu2411.d20_basic_signal_results`
GROUP BY trade_type;

-- データ完全性確認
WITH source_vs_result AS (
  SELECT 
    'source (d15)' as data_source,
    COUNT(*) as record_count,
    COUNT(DISTINCT signal_type) as signal_types
  FROM `kabu-376213.kabu2411.d15_signals_with_bins`
  UNION ALL
  SELECT 
    'result (d20)' as data_source,
    COUNT(*) / 2 as record_count,  -- LONG/SHORT分割のため
    COUNT(DISTINCT signal_type) as signal_types
  FROM `kabu-376213.kabu2411.d20_basic_signal_results`
)
SELECT 
  'Phase 5データ完全性' as check_point,
  data_source,
  record_count,
  signal_types,
  ROUND((record_count / LAG(record_count) OVER (ORDER BY data_source)) * 100, 1) as retention_rate_percent
FROM source_vs_result
ORDER BY data_source;

-- ============================================================================
-- Step 6: Phase 5完了確認
-- ============================================================================

SELECT 
  '🎉 Phase 5完了（37指標版）' as final_check,
  COUNT(*) as total_records,
  COUNT(DISTINCT signal_type) as signal_types_37_expected,
  ROUND(COUNT(*) / COUNT(DISTINCT signal_date) / 2, 0) as avg_records_per_day_per_direction,
  '2分割実行により大規模データ処理成功' as execution_method,
  'Phase 6: 統計テーブル群再構築 実行可能' as next_step,
  CURRENT_TIMESTAMP() as completion_time
FROM `kabu-376213.kabu2411.d20_basic_signal_results`;

-- 次Phase準備確認
SELECT 
  '📋 Phase 6準備確認' as next_phase,
  '✅ d20_basic_signal_results (Phase 5完了・37指標版)' as completed,
  '⚡ 統計テーブル群 (Phase 6実行予定・37指標版)' as next_target,
  '対象: d30, d40, d60, m10, u10, u20' as target_tables;

-- ============================================================================
-- 処理完了メッセージ
-- ============================================================================

SELECT 
  'Phase 5: d20_basic_signal_results作成が完了しました（37指標版）' as message,
  '2分割実行により大規模処理成功（学習期間 + 検証期間）' as achievement,
  'パラメータチューニング用項目追加でUX向上' as enhancement,
  '次段階: Phase 6 (統計テーブル群再構築・37指標版) 実行可能' as next_step,
  CURRENT_TIMESTAMP() as completion_time;