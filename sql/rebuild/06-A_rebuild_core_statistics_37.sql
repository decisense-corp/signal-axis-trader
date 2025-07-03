/*
ファイル: 06-A_rebuild_core_statistics_37.sql (Phase 7対応版)
説明: Phase 6-A - 37指標版 基盤統計テーブル再構築 + d30に優秀パターン判定追加
作成日: 2025年7月3日
修正日: 2025年7月3日 (Phase 7対応)
依存: d20_basic_signal_results (37指標版・3600万件)
対象: d30_learning_period_snapshot + d40_axis_performance_stats
目的: API基盤の核心となる統計テーブル完成 + d30に優秀パターン判定機能追加
処理時間: 約15-20分
Phase 7対応: d30テーブルにis_excellent_pattern, pattern_category追加
*/

-- ============================================================================
-- Phase 6-A: 基盤統計テーブル再構築（37指標版・Phase 7対応）
-- ============================================================================

-- 処理開始メッセージ
SELECT 
  '🚀 Phase 6-A開始: 基盤統計テーブル再構築（37指標版・Phase 7対応）' as message,
  'データソース: d20_basic_signal_results (3600万件・37指標版)' as source_info,
  'Target 1: d30_learning_period_snapshot (学習期間統計・優秀パターン判定追加)' as target1,
  'Target 2: d40_axis_performance_stats (全期間統計・優秀パターン判定)' as target2,
  '新機能: d30に is_excellent_pattern + pattern_category 追加' as phase7_feature,
  '予想処理時間: 約15-20分' as estimated_time,
  CURRENT_TIMESTAMP() as start_time;

-- ============================================================================
-- Step 1: d30_learning_period_snapshot再構築（37指標版・優秀パターン判定付き）
-- ============================================================================

-- バックアップ作成（構造のみ・大規模データのため）
CREATE OR REPLACE TABLE `kabu-376213.kabu2411.d30_learning_period_snapshot_backup_phase6a_37` AS
SELECT *, CURRENT_TIMESTAMP() as backup_timestamp
FROM `kabu-376213.kabu2411.d30_learning_period_snapshot`
LIMIT 1000;  -- 構造確認用のみ

SELECT 
  '✅ d30バックアップ完了' as status,
  '大規模データのため構造のみ保存' as note,
  'バックアップテーブル: d30_learning_period_snapshot_backup_phase6a_37' as backup_table;

-- 既存テーブル削除・再作成（優秀パターン判定付き）
DROP TABLE IF EXISTS `kabu-376213.kabu2411.d30_learning_period_snapshot`;

CREATE TABLE `kabu-376213.kabu2411.d30_learning_period_snapshot`
PARTITION BY signal_date
CLUSTER BY stock_code, signal_type
AS
WITH learning_period_data AS (
  SELECT 
    signal_date,
    reference_date,
    stock_code,
    stock_name,
    signal_type,
    signal_bin,
    trade_type,
    signal_value,
    profit_rate,
    is_win,
    trading_volume,
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
    daily_range,
    created_at,
    CURRENT_TIMESTAMP() as snapshot_created_at
  FROM `kabu-376213.kabu2411.d20_basic_signal_results`
  WHERE signal_date <= '2024-06-30'  -- 学習期間のみ
),
learning_stats AS (
  -- 学習期間の4軸別統計計算
  SELECT
    signal_type,
    signal_bin,
    trade_type,
    stock_code,
    ANY_VALUE(stock_name) as stock_name,
    COUNT(*) as total_signals,
    SUM(CASE WHEN is_win = true THEN 1 ELSE 0 END) as win_signals,
    ROUND(AVG(CASE WHEN is_win = true THEN 1.0 ELSE 0.0 END) * 100, 1) as win_rate,
    ROUND(AVG(profit_rate), 4) as avg_profit_rate,
    ROUND(STDDEV(profit_rate), 4) as std_deviation,
    ROUND(SAFE_DIVIDE(AVG(profit_rate), NULLIF(STDDEV(profit_rate), 0)), 3) as sharpe_ratio
  FROM learning_period_data
  GROUP BY signal_type, signal_bin, trade_type, stock_code
)
SELECT 
  lpd.*,
  -- 優秀パターン判定（Phase 7対応）
  CASE 
    WHEN ls.win_rate >= 55.0 
    AND ls.avg_profit_rate >= 0.5 
    AND ls.total_signals >= 20 
    AND ls.sharpe_ratio > 0.1
    THEN true 
    ELSE false 
  END as is_excellent_pattern,
  
  -- パターンカテゴリ分類（Phase 7対応）
  CASE 
    WHEN ls.win_rate >= 65.0 AND ls.avg_profit_rate >= 1.0 THEN 'PREMIUM'
    WHEN ls.win_rate >= 60.0 AND ls.avg_profit_rate >= 0.8 THEN 'EXCELLENT'
    WHEN ls.win_rate >= 55.0 AND ls.avg_profit_rate >= 0.5 THEN 'GOOD'
    WHEN ls.win_rate >= 50.0 AND ls.avg_profit_rate >= 0.2 THEN 'NORMAL'
    ELSE 'CAUTION'
  END as pattern_category,
  
  -- 統計情報参照用（APIで使用可能）
  ls.total_signals as learning_total_signals,
  ls.win_signals as learning_win_signals,
  ls.win_rate as learning_win_rate,
  ls.avg_profit_rate as learning_avg_profit,
  ls.std_deviation as learning_std_deviation,
  ls.sharpe_ratio as learning_sharpe_ratio
  
FROM learning_period_data lpd
LEFT JOIN learning_stats ls
  ON lpd.signal_type = ls.signal_type
  AND lpd.signal_bin = ls.signal_bin
  AND lpd.trade_type = ls.trade_type
  AND lpd.stock_code = ls.stock_code;

-- d30作成完了確認
SELECT 
  '✅ Step 1完了: d30_learning_period_snapshot（37指標版・優秀パターン判定付き）' as status,
  COUNT(*) as learning_period_records,
  COUNT(DISTINCT signal_type) as signal_types_37_expected,
  COUNT(DISTINCT stock_code) as stocks_count,
  COUNT(DISTINCT trade_type) as trade_types,
  SUM(CASE WHEN is_excellent_pattern = true THEN 1 ELSE 0 END) as excellent_records,
  MIN(signal_date) as learning_start,
  MAX(signal_date) as learning_end,
  '🎯 優秀パターン判定機能追加完了' as phase7_feature
FROM `kabu-376213.kabu2411.d30_learning_period_snapshot`;

-- 優秀パターン分布確認（d30ベース）
SELECT 
  '📊 d30優秀パターン分布確認' as check_type,
  pattern_category,
  COUNT(DISTINCT CONCAT(signal_type, '-', signal_bin, '-', trade_type, '-', stock_code)) as unique_patterns,
  COUNT(*) as total_records,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
FROM `kabu-376213.kabu2411.d30_learning_period_snapshot`
GROUP BY pattern_category
ORDER BY 
  CASE pattern_category
    WHEN 'PREMIUM' THEN 1
    WHEN 'EXCELLENT' THEN 2
    WHEN 'GOOD' THEN 3
    WHEN 'NORMAL' THEN 4
    WHEN 'CAUTION' THEN 5
  END;

-- ============================================================================
-- Step 2: d40_axis_performance_stats再構築（37指標版）
-- ============================================================================

-- バックアップ作成（構造のみ・大規模データのため）
CREATE OR REPLACE TABLE `kabu-376213.kabu2411.d40_axis_performance_stats_backup_phase6a_37` AS
SELECT *, CURRENT_TIMESTAMP() as backup_timestamp
FROM `kabu-376213.kabu2411.d40_axis_performance_stats`
LIMIT 1000;  -- 構造確認用のみ

SELECT 
  '✅ d40バックアップ完了' as status,
  '大規模データのため構造のみ保存' as note,
  'バックアップテーブル: d40_axis_performance_stats_backup_phase6a_37' as backup_table;

-- 既存テーブル削除・再作成
DROP TABLE IF EXISTS `kabu-376213.kabu2411.d40_axis_performance_stats`;

CREATE TABLE `kabu-376213.kabu2411.d40_axis_performance_stats`
CLUSTER BY is_excellent_pattern, signal_type, signal_bin
AS 
WITH learning_stats AS (
  -- 学習期間統計（〜2024/6/30）
  SELECT
    signal_type,
    signal_bin,
    trade_type,
    stock_code,
    ANY_VALUE(stock_name) as stock_name,
    COUNT(*) as learning_total_signals,
    SUM(CASE WHEN is_win = true THEN 1 ELSE 0 END) as learning_win_signals,
    ROUND(AVG(CASE WHEN is_win = true THEN 1.0 ELSE 0.0 END) * 100, 1) as learning_win_rate,
    ROUND(AVG(profit_rate), 2) as learning_avg_profit,
    ROUND(STDDEV(profit_rate), 3) as learning_std_deviation,
    ROUND(SAFE_DIVIDE(AVG(profit_rate), NULLIF(STDDEV(profit_rate), 0)), 3) as learning_sharpe_ratio,
    MIN(signal_date) as learning_first_signal,
    MAX(signal_date) as learning_last_signal
  FROM `kabu-376213.kabu2411.d20_basic_signal_results`
  WHERE signal_date <= '2024-06-30'
  GROUP BY signal_type, signal_bin, trade_type, stock_code
),
verification_stats AS (
  -- 検証期間統計（2024/7/1〜）
  SELECT
    signal_type,
    signal_bin,
    trade_type,
    stock_code,
    ANY_VALUE(stock_name) as stock_name,
    COUNT(*) as verification_total_signals,
    SUM(CASE WHEN is_win = true THEN 1 ELSE 0 END) as verification_win_signals,
    ROUND(AVG(CASE WHEN is_win = true THEN 1.0 ELSE 0.0 END) * 100, 1) as verification_win_rate,
    ROUND(AVG(profit_rate), 2) as verification_avg_profit,
    ROUND(STDDEV(profit_rate), 3) as verification_std_deviation,
    ROUND(SAFE_DIVIDE(AVG(profit_rate), NULLIF(STDDEV(profit_rate), 0)), 3) as verification_sharpe_ratio,
    MIN(signal_date) as verification_first_signal,
    MAX(signal_date) as verification_last_signal
  FROM `kabu-376213.kabu2411.d20_basic_signal_results`
  WHERE signal_date > '2024-06-30'
  GROUP BY signal_type, signal_bin, trade_type, stock_code
),
combined_stats AS (
  SELECT 
    COALESCE(l.signal_type, v.signal_type) as signal_type,
    COALESCE(l.signal_bin, v.signal_bin) as signal_bin,
    COALESCE(l.trade_type, v.trade_type) as trade_type,
    COALESCE(l.stock_code, v.stock_code) as stock_code,
    COALESCE(l.stock_name, v.stock_name) as stock_name,
    
    -- 学習期間統計
    COALESCE(l.learning_total_signals, 0) as learning_total_signals,
    COALESCE(l.learning_win_signals, 0) as learning_win_signals,
    COALESCE(l.learning_win_rate, 0) as learning_win_rate,
    COALESCE(l.learning_avg_profit, 0) as learning_avg_profit,
    COALESCE(l.learning_std_deviation, 0) as learning_std_deviation,
    COALESCE(l.learning_sharpe_ratio, 0) as learning_sharpe_ratio,
    l.learning_first_signal,
    l.learning_last_signal,
    
    -- 検証期間統計
    COALESCE(v.verification_total_signals, 0) as recent_total_signals,
    COALESCE(v.verification_win_signals, 0) as recent_win_signals,
    COALESCE(v.verification_win_rate, 0) as recent_win_rate,
    COALESCE(v.verification_avg_profit, 0) as recent_avg_profit,
    COALESCE(v.verification_std_deviation, 0) as recent_std_deviation,
    COALESCE(v.verification_sharpe_ratio, 0) as recent_sharpe_ratio,
    v.verification_first_signal as recent_first_signal,
    v.verification_last_signal as recent_last_signal
    
  FROM learning_stats l
  FULL OUTER JOIN verification_stats v
    ON l.signal_type = v.signal_type
    AND l.signal_bin = v.signal_bin
    AND l.trade_type = v.trade_type
    AND l.stock_code = v.stock_code
)
SELECT 
  *,
  
  -- 劣化分析指標
  CASE 
    WHEN learning_win_rate > 0 AND recent_total_signals >= 5 
    THEN ROUND(learning_win_rate - recent_win_rate, 2)
    ELSE NULL
  END as win_rate_degradation,
  
  CASE 
    WHEN learning_avg_profit <> 0 AND recent_total_signals >= 5 
    THEN ROUND(learning_avg_profit - recent_avg_profit, 4)
    ELSE NULL
  END as profit_degradation,
  
  -- 継続性指標
  CASE 
    WHEN learning_win_rate >= 55 AND learning_avg_profit >= 0.15 AND recent_total_signals >= 5
    THEN CASE 
      WHEN recent_win_rate >= 50 AND recent_avg_profit >= 0.1 THEN TRUE
      ELSE FALSE
    END
    ELSE NULL
  END as pattern_continuity,
  
  -- 優秀パターン判定（学習期間ベース・d30と同じ基準）
  CASE 
    WHEN learning_total_signals >= 20 
         AND learning_win_rate >= 55 
         AND learning_avg_profit >= 0.5 
         AND learning_sharpe_ratio > 0.1
    THEN TRUE 
    ELSE FALSE 
  END as is_excellent_pattern,
  
  -- パターンカテゴリ（d30と同じ基準）
  CASE 
    WHEN learning_win_rate >= 65.0 AND learning_avg_profit >= 1.0 THEN 'PREMIUM'
    WHEN learning_win_rate >= 60.0 AND learning_avg_profit >= 0.8 THEN 'EXCELLENT'
    WHEN learning_win_rate >= 55.0 AND learning_avg_profit >= 0.5 THEN 'GOOD'
    WHEN learning_win_rate >= 50.0 AND learning_avg_profit >= 0.2 THEN 'NORMAL'
    ELSE 'CAUTION'
  END as pattern_category,
  
  CURRENT_TIMESTAMP() as last_updated
  
FROM combined_stats;

-- d40作成完了確認
SELECT 
  '✅ Step 2完了: d40_axis_performance_stats（37指標版）' as status,
  COUNT(*) as total_patterns,
  COUNT(DISTINCT signal_type) as signal_types_37_expected,
  COUNT(DISTINCT stock_code) as stocks_count,
  COUNT(DISTINCT trade_type) as trade_types,
  SUM(CASE WHEN is_excellent_pattern = true THEN 1 ELSE 0 END) as excellent_patterns,
  SUM(CASE WHEN recent_total_signals >= 5 THEN 1 ELSE 0 END) as patterns_with_verification_data,
  '学習期間+検証期間統計統合完了' as integration_status
FROM `kabu-376213.kabu2411.d40_axis_performance_stats`;

-- ============================================================================
-- Step 3: 基本品質確認（必要最低限）
-- ============================================================================

-- 37指標構成確認
SELECT 
  '📊 37指標構成確認' as check_type,
  'target: d30_learning_period_snapshot' as target_table,
  signal_type,
  COUNT(DISTINCT CONCAT(signal_bin, '-', trade_type, '-', stock_code)) as unique_patterns,
  SUM(CASE WHEN is_excellent_pattern = true THEN 1 ELSE 0 END) as excellent_records,
  ROUND(SUM(CASE WHEN is_excellent_pattern = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as excellent_percentage
FROM `kabu-376213.kabu2411.d30_learning_period_snapshot`
GROUP BY signal_type
ORDER BY signal_type
LIMIT 15;  -- 一部のみ表示

-- 優秀パターン分布確認（d40ベース）
SELECT 
  '🎯 優秀パターン分布確認' as check_type,
  'target: d40_axis_performance_stats' as target_table,
  pattern_category,
  COUNT(*) as pattern_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage,
  ROUND(AVG(learning_win_rate), 1) as avg_win_rate,
  ROUND(AVG(learning_avg_profit), 2) as avg_profit_rate
FROM `kabu-376213.kabu2411.d40_axis_performance_stats`
GROUP BY pattern_category
ORDER BY 
  CASE pattern_category
    WHEN 'PREMIUM' THEN 1
    WHEN 'EXCELLENT' THEN 2
    WHEN 'GOOD' THEN 3
    WHEN 'NORMAL' THEN 4
    WHEN 'CAUTION' THEN 5
  END;

-- データ整合性確認
WITH integrity_check AS (
  SELECT 
    'd30 (learning_period)' as table_name,
    COUNT(*) as record_count,
    COUNT(DISTINCT signal_type) as signal_types,
    COUNT(DISTINCT stock_code) as stock_count,
    SUM(CASE WHEN is_excellent_pattern = true THEN 1 ELSE 0 END) as excellent_records
  FROM `kabu-376213.kabu2411.d30_learning_period_snapshot`
  
  UNION ALL
  
  SELECT 
    'd40 (performance_stats)' as table_name,
    COUNT(*) as record_count,
    COUNT(DISTINCT signal_type) as signal_types,
    COUNT(DISTINCT stock_code) as stock_count,
    SUM(CASE WHEN is_excellent_pattern = true THEN 1 ELSE 0 END) as excellent_patterns
  FROM `kabu-376213.kabu2411.d40_axis_performance_stats`
  
  UNION ALL
  
  SELECT 
    'd20 (source_data)' as table_name,
    COUNT(*) as record_count,
    COUNT(DISTINCT signal_type) as signal_types,
    COUNT(DISTINCT stock_code) as stock_count,
    NULL as excellent_count
  FROM `kabu-376213.kabu2411.d20_basic_signal_results`
)
SELECT 
  '🔍 データ整合性確認' as check_type,
  table_name,
  record_count,
  signal_types,
  stock_count,
  excellent_records
FROM integrity_check
ORDER BY table_name;

-- ============================================================================
-- Step 4: Phase 6-A完了確認（Phase 7対応）
-- ============================================================================

SELECT 
  '🎉 Phase 6-A完了（37指標版・Phase 7対応）' as final_status,
  '✅ d30_learning_period_snapshot 再構築完了' as achievement1,
  '✅ d40_axis_performance_stats 再構築完了' as achievement2,
  '🎯 d30に優秀パターン判定機能追加完了' as phase7_achievement,
  '📊 37指標による基盤統計テーブル完成' as technical_achievement,
  '⚡ API高速化基盤準備完了' as performance_achievement,
  '🔗 d30単体でAPI完結可能' as integration_achievement,
  CURRENT_TIMESTAMP() as completion_time;

-- 次段階準備確認
SELECT 
  '📋 Phase 7 API実装準備確認' as next_phase,
  '✅ Phase 6-A (基盤統計+優秀パターン判定) 完了' as current_status,
  '⚡ /api/signals/tomorrow でd30単体利用可能' as api_capability,
  '🎯 d30 × d15 × u10 JOIN構造実装可能' as join_strategy,
  '予想API応答時間: 数秒以内' as performance_expectation;

-- ============================================================================
-- 処理完了メッセージ（Phase 7対応）
-- ============================================================================

SELECT 
  'Phase 6-A: 基盤統計テーブル再構築が完了しました（37指標版・Phase 7対応）' as message,
  'd30 + d40による統計基盤完成' as achievement,
  'd30テーブルに優秀パターン判定機能追加完了' as phase7_feature,
  'API高速化・優秀パターン判定機能準備完了' as capability,
  '次段階: Phase 7 API実装（d30メインテーブル利用）実行可能' as next_step,
  CURRENT_TIMESTAMP() as completion_time;