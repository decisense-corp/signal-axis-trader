/*
ファイル: 05_rebuild_d20_basic_results_17.sql
説明: Phase 5 - 17指標版 d20_basic_signal_results 完全再構築
作成日: 2025年7月3日 21:20 JST
依存: d15_signals_with_bins (17指標版) + daily_quotes
目的: 独自指標戦略の取引結果計算・劣化分析準備
*/

-- ============================================================================
-- Phase 5: d20_basic_signal_results（17指標版）完全再構築実行
-- ============================================================================

-- 処理開始メッセージ
SELECT 
  '🚀 Phase 5開始: d20_basic_signal_results（17指標版）再構築' as message,
  'データソース: d15_signals_with_bins (816万行・17指標版) + daily_quotes' as source_info,
  '戦略: 新指標10 + 比較用7指標による劣化分析準備' as strategy,
  '目標: Phase 7の15-17%劣化を新指標が改善するかの検証' as target,
  CURRENT_TIMESTAMP() as start_time;

-- ============================================================================
-- 1. 既存データのバックアップ（安全性確保）
-- ============================================================================

-- バックアップテーブル作成
CREATE OR REPLACE TABLE `kabu-376213.kabu2411.d20_basic_signal_results_backup_phase5_17` AS
SELECT *, CURRENT_TIMESTAMP() as backup_timestamp
FROM `kabu-376213.kabu2411.d20_basic_signal_results`;

SELECT 
  '✅ バックアップ完了' as status,
  COUNT(*) as backup_record_count,
  '安全性確保: 17指標版構築前の既存データ保存' as note
FROM `kabu-376213.kabu2411.d20_basic_signal_results_backup_phase5_17`;

-- ============================================================================
-- 2. 新テーブル構造での完全再構築（17指標版対応）
-- ============================================================================

-- 既存テーブルを削除
DROP TABLE IF EXISTS `kabu-376213.kabu2411.d20_basic_signal_results`;

-- 新構造でテーブル作成（既存項目 + パラメータチューニング用項目）
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
-- 3. データ投入（17指標版: d15_signals_with_bins + daily_quotes結合）
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
  WHERE Date >= '2022-07-01'  -- 17指標版の対象期間
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
  WHERE s.signal_date IS NOT NULL
    AND q.Date IS NOT NULL
    AND q.Open IS NOT NULL
    AND q.Close IS NOT NULL
    AND q.prev_close IS NOT NULL
    -- 17指標版の品質確保
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
-- 4. データ品質検証（17指標版）
-- ============================================================================

-- 基本統計確認
SELECT 
  '📊 Phase 5: データ投入完了（17指標版）' as status,
  COUNT(*) as total_records,
  COUNT(*) / 2 as unique_signal_records,  -- LONG/SHORT分割のため
  COUNT(DISTINCT stock_code) as unique_stocks,
  COUNT(DISTINCT signal_type) as unique_signal_types_should_be_17,
  MIN(signal_date) as min_date,
  MAX(signal_date) as max_date,
  ROUND(COUNT(*) / COUNT(DISTINCT signal_date), 0) as avg_records_per_day
FROM `kabu-376213.kabu2411.d20_basic_signal_results`;

-- 新項目のデータ完全性確認
SELECT 
  '✅ パラメータチューニング用項目完全性確認' as check_type,
  COUNT(*) as total_records,
  COUNT(prev_close) as prev_close_count,
  COUNT(day_open) as day_open_count,
  COUNT(day_high) as day_high_count,
  COUNT(day_low) as day_low_count,
  COUNT(day_close) as day_close_count,
  COUNT(gap_amount) as gap_amount_count,
  COUNT(open_to_high_amount) as open_to_high_amount_count,
  COUNT(open_to_low_amount) as open_to_low_amount_count,
  COUNT(open_to_close_amount) as open_to_close_amount_count,
  COUNT(daily_range) as daily_range_count,
  ROUND(COUNT(prev_close) * 100.0 / COUNT(*), 2) as data_completeness_percent
FROM `kabu-376213.kabu2411.d20_basic_signal_results`;

-- 17指標構成確認
SELECT 
  '🚀 17指標構成確認' as check_type,
  signal_type,
  COUNT(*) / 2 as unique_records,  -- LONG/SHORT分割調整
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage,
  CASE 
    WHEN signal_type LIKE '%High_Price_Score%' OR signal_type LIKE '%Low_Price_Score%' 
    THEN '新指標'
    ELSE '比較用'
  END as indicator_category
FROM `kabu-376213.kabu2411.d20_basic_signal_results`
GROUP BY signal_type
ORDER BY indicator_category, signal_type;

-- 新指標 vs 比較用指標の統計
SELECT 
  '📈 新指標 vs 比較用指標統計' as analysis_type,
  CASE 
    WHEN signal_type LIKE '%High_Price_Score%' OR signal_type LIKE '%Low_Price_Score%' 
    THEN '新指標（High/Low Price Score）'
    ELSE '比較用（Phase 7劣化上位）'
  END as indicator_type,
  COUNT(DISTINCT signal_type) as signal_count,
  COUNT(*) / 2 as total_records,  -- LONG/SHORT分割調整
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage,
  'Phase 6で劣化分析実行' as next_step
FROM `kabu-376213.kabu2411.d20_basic_signal_results`
GROUP BY 
  CASE 
    WHEN signal_type LIKE '%High_Price_Score%' OR signal_type LIKE '%Low_Price_Score%' 
    THEN '新指標（High/Low Price Score）'
    ELSE '比較用（Phase 7劣化上位）'
  END
ORDER BY indicator_type;

-- 計算精度確認（サンプル検証）
SELECT 
  '🔍 計算精度確認（最新10件）' as check_type,
  signal_date,
  stock_code,
  signal_type,
  trade_type,
  prev_close,
  day_open,
  day_close,
  profit_rate,
  CASE 
    WHEN trade_type = 'LONG' 
    THEN ROUND((day_close - day_open) / day_open * 100, 4)
    ELSE ROUND((day_open - day_close) / day_open * 100, 4)
  END as calculated_profit_rate,
  CASE WHEN is_win THEN '勝' ELSE '負' END as win_status,
  gap_amount,
  ROUND(day_open - prev_close, 2) as gap_verify
FROM `kabu-376213.kabu2411.d20_basic_signal_results`
WHERE signal_date >= '2025-07-01'
ORDER BY signal_date DESC, stock_code, signal_type
LIMIT 10;

-- d15との整合性確認
WITH comparison AS (
  SELECT 
    'd15_signals_with_bins（17指標版）' as source_table,
    COUNT(*) as record_count,
    COUNT(DISTINCT CONCAT(signal_date, stock_code, signal_type)) as unique_combinations,
    'Phase 4完了データ' as note
  FROM `kabu-376213.kabu2411.d15_signals_with_bins`
  
  UNION ALL
  
  SELECT 
    'd20_basic_signal_results（17指標版）' as source_table,
    COUNT(*) / 2 as record_count,  -- LONG/SHORT分割のため
    COUNT(DISTINCT CONCAT(signal_date, stock_code, signal_type)) / 2 as unique_combinations,
    'Phase 5完了データ' as note
  FROM `kabu-376213.kabu2411.d20_basic_signal_results`
)
SELECT 
  '📋 データ整合性確認' as check_type,
  source_table,
  record_count,
  unique_combinations,
  note,
  CASE 
    WHEN source_table LIKE 'd15_%' THEN NULL
    ELSE ROUND(record_count * 100.0 / LAG(record_count) OVER (ORDER BY source_table), 2)
  END as retention_rate_percent
FROM comparison
ORDER BY source_table;

-- ============================================================================
-- 5. パフォーマンス最適化確認
-- ============================================================================

-- クラスタリング効果確認
SELECT 
  '⚡ パフォーマンス最適化確認' as check_type,
  COUNT(*) as total_records,
  COUNT(DISTINCT stock_code) as clustered_stocks,
  COUNT(DISTINCT signal_type) as clustered_signal_types,
  'パーティション: signal_date' as partition_info,
  'クラスタ: stock_code, signal_type' as cluster_info,
  'JOIN処理完全排除によるUX劇的向上' as performance_gain
FROM `kabu-376213.kabu2411.d20_basic_signal_results`;

-- ============================================================================
-- 6. Phase 5完了報告
-- ============================================================================

SELECT 
  '🎉 Phase 5完了報告（17指標版）' as status,
  '✅ d20_basic_signal_results（17指標版）構築完了' as achievement,
  '📊 新指標10 + 比較用7指標の取引結果計算完了' as composition,
  '🎯 Phase 7劣化分析用データ準備完了' as analysis_ready,
  '⚡ パラメータチューニング用項目追加完了' as enhancement,
  '📈 Phase 6で新指標の真価を検証開始' as next_phase,
  CURRENT_TIMESTAMP() as completion_time;

-- 成功判定基準の再確認
SELECT 
  '🎯 成功判定基準（再確認）' as criteria_type,
  '最低目標: 新指標劣化 < 15.25%（既存最優秀を上回る）' as minimum_target,
  '理想目標: 新指標劣化 < 10%（明確な優位性確立）' as ideal_target,
  '継続率: 優秀パターン継続率 > 40%（既存30-36%を上回る）' as continuity_target,
  'Phase 6で数値検証により仮説を検証' as verification_method;

-- ============================================================================
-- 使用方法例：パラメータチューニング画面での高速表示
-- ============================================================================

/*
-- 使用例1: 新指標の詳細分析（JOIN不要の高速表示）
SELECT 
  signal_date as 日付,
  prev_close as 前日終値,
  day_open as 始値,
  day_high as 高値,
  day_low as 安値,
  day_close as 終値,
  gap_amount as ギャップ,
  profit_rate as 利益率,
  CASE WHEN is_win THEN '勝' ELSE '負' END as 勝敗,
  signal_bin as bin値
FROM `kabu-376213.kabu2411.d20_basic_signal_results`
WHERE signal_type = 'High_Price_Score_7D'  -- 新指標
  AND trade_type = 'LONG'
  AND signal_bin = 1  -- 最強シグナル
ORDER BY signal_date DESC
LIMIT 100;

-- 使用例2: 新指標vs比較用指標の勝率比較
SELECT 
  CASE 
    WHEN signal_type LIKE '%High_Price_Score%' OR signal_type LIKE '%Low_Price_Score%' 
    THEN '新指標'
    ELSE '比較用'
  END as indicator_type,
  COUNT(*) as total_trades,
  SUM(CASE WHEN is_win THEN 1 ELSE 0 END) as win_trades,
  ROUND(AVG(CASE WHEN is_win THEN 1.0 ELSE 0.0 END) * 100, 2) as win_rate_percent,
  ROUND(AVG(profit_rate), 4) as avg_profit_rate
FROM `kabu-376213.kabu2411.d20_basic_signal_results`
WHERE signal_bin = 1  -- 最強シグナルのみ
GROUP BY 
  CASE 
    WHEN signal_type LIKE '%High_Price_Score%' OR signal_type LIKE '%Low_Price_Score%' 
    THEN '新指標'
    ELSE '比較用'
  END
ORDER BY win_rate_percent DESC;
*/