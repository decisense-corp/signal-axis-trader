/*
ファイル: 06_rebuild_statistics_17.sql
説明: Phase 6 - 17指標版 統計テーブル群再構築・劣化分析実行
作成日: 2025年7月3日 21:25 JST
依存: d20_basic_signal_results (17指標版・1631万件)
目的: 新指標の劣化分析・既存指標との比較検証
重要: この分析で新指標の優位性を数値的に証明する
*/

-- ============================================================================
-- Phase 6: 17指標版 統計テーブル群再構築・劣化分析実行
-- ============================================================================

-- 処理開始メッセージ
SELECT 
  '🎯 Phase 6開始: 新指標劣化分析・真価検証' as message,
  'データソース: d20_basic_signal_results (1631万件・17指標版)' as source_info,
  '検証仮説: 新指標劣化 < 15.25%（既存最優秀を上回る）' as hypothesis,
  '期待効果: 独自性による持続的競争優位の確立' as expectation,
  CURRENT_TIMESTAMP() as start_time;

-- ============================================================================
-- Step 1: d30_learning_period_snapshot再構築（17指標版）
-- ============================================================================

-- バックアップ作成
CREATE OR REPLACE TABLE `kabu-376213.kabu2411.d30_learning_period_snapshot_backup_phase6_17` AS
SELECT *, CURRENT_TIMESTAMP() as backup_timestamp
FROM `kabu-376213.kabu2411.d30_learning_period_snapshot`;

-- 既存テーブル削除・再作成
DROP TABLE IF EXISTS `kabu-376213.kabu2411.d30_learning_period_snapshot`;

CREATE TABLE `kabu-376213.kabu2411.d30_learning_period_snapshot`
PARTITION BY signal_date
CLUSTER BY stock_code, signal_type
AS
SELECT 
  signal_date,
  stock_code,
  stock_name,
  signal_type,
  signal_bin,
  trade_type,
  signal_value,
  profit_rate,
  is_win,
  trading_volume,
  -- パラメータチューニング用項目も含める
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
WHERE signal_date <= '2024-06-30';  -- 学習期間のみ

-- d30作成完了確認
SELECT 
  '✅ d30_learning_period_snapshot（17指標版）作成完了' as status,
  COUNT(*) as learning_period_records,
  COUNT(DISTINCT signal_type) as signal_types_17_expected,
  COUNT(DISTINCT stock_code) as stocks_count,
  MIN(signal_date) as learning_start,
  MAX(signal_date) as learning_end,
  '学習期間: 2022/7/4〜2024/6/30' as period_note
FROM `kabu-376213.kabu2411.d30_learning_period_snapshot`;

-- ============================================================================
-- Step 2: d40_axis_performance_stats再構築（17指標版・劣化分析対応）
-- ============================================================================

-- バックアップ作成
CREATE OR REPLACE TABLE `kabu-376213.kabu2411.d40_axis_performance_stats_backup_phase6_17` AS
SELECT *, CURRENT_TIMESTAMP() as backup_timestamp
FROM `kabu-376213.kabu2411.d40_axis_performance_stats`;

-- 既存テーブル削除・再作成
DROP TABLE IF EXISTS `kabu-376213.kabu2411.d40_axis_performance_stats`;

CREATE TABLE `kabu-376213.kabu2411.d40_axis_performance_stats`
CLUSTER BY is_excellent_pattern, signal_type, signal_bin
AS 
WITH learning_stats AS (
  -- 学習期間統計（2022/7/4〜2024/6/30）
  SELECT
    signal_type,
    signal_bin,
    trade_type,
    stock_code,
    stock_name,
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
  GROUP BY signal_type, signal_bin, trade_type, stock_code, stock_name
),
verification_stats AS (
  -- 検証期間統計（2024/7/1〜2025/7/1）
  SELECT
    signal_type,
    signal_bin,
    trade_type,
    stock_code,
    stock_name,
    COUNT(*) as verification_total_signals,
    SUM(CASE WHEN is_win = true THEN 1 ELSE 0 END) as verification_win_signals,
    ROUND(AVG(CASE WHEN is_win = true THEN 1.0 ELSE 0.0 END) * 100, 1) as verification_win_rate,
    ROUND(AVG(profit_rate), 2) as verification_avg_profit,
    ROUND(STDDEV(profit_rate), 3) as verification_std_deviation,
    ROUND(SAFE_DIVIDE(AVG(profit_rate), NULLIF(STDDEV(profit_rate), 0)), 3) as verification_sharpe_ratio,
    MIN(signal_date) as verification_first_signal,
    MAX(signal_date) as verification_last_signal
  FROM `kabu-376213.kabu2411.d20_basic_signal_results`
  WHERE signal_date > '2024-06-30' AND signal_date <= '2025-07-01'
  GROUP BY signal_type, signal_bin, trade_type, stock_code, stock_name
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
    
    -- 検証期間統計（劣化分析用）
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
  
  -- 劣化分析指標（重要！）
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
  
  -- 優秀パターン判定（学習期間ベース）
  CASE 
    WHEN learning_total_signals >= 10 
         AND learning_win_rate >= 55 
         AND learning_avg_profit >= 0.15 
    THEN TRUE 
    ELSE FALSE 
  END as is_excellent_pattern,
  
  -- パターンカテゴリ
  CASE 
    WHEN learning_total_signals >= 100 AND learning_win_rate >= 60 AND learning_avg_profit >= 0.3 THEN 'PREMIUM'
    WHEN learning_total_signals >= 50 AND learning_win_rate >= 55 AND learning_avg_profit >= 0.15 THEN 'EXCELLENT'
    WHEN learning_total_signals >= 20 AND learning_win_rate >= 52 AND learning_avg_profit >= 0.1 THEN 'GOOD'
    WHEN learning_total_signals >= 10 AND learning_win_rate >= 50 THEN 'NORMAL'
    ELSE 'CAUTION'
  END as pattern_category,
  
  -- 新指標分類（劣化分析用）
  CASE 
    WHEN signal_type LIKE '%High_Price_Score%' OR signal_type LIKE '%Low_Price_Score%' 
    THEN 'NEW_INDICATOR'
    ELSE 'COMPARISON_INDICATOR'
  END as indicator_group,
  
  CURRENT_TIMESTAMP() as last_updated
  
FROM combined_stats;

-- d40作成完了確認
SELECT 
  '✅ d40_axis_performance_stats（17指標版）作成完了' as status,
  COUNT(*) as total_patterns,
  COUNT(DISTINCT signal_type) as signal_types_17_expected,
  COUNT(DISTINCT stock_code) as stocks_count,
  SUM(CASE WHEN is_excellent_pattern = true THEN 1 ELSE 0 END) as excellent_patterns,
  SUM(CASE WHEN recent_total_signals >= 5 THEN 1 ELSE 0 END) as patterns_with_verification_data,
  '学習期間+検証期間統計統合完了' as integration_status
FROM `kabu-376213.kabu2411.d40_axis_performance_stats`;

-- ============================================================================
-- Step 3: 🎯 劣化分析実行（新指標の真価検証）
-- ============================================================================

-- 最重要分析: 新指標 vs 比較用指標の劣化比較
SELECT 
  '🎯 【最重要】新指標 vs 比較用指標 劣化比較分析' as analysis_title,
  indicator_group,
  CASE 
    WHEN indicator_group = 'NEW_INDICATOR' THEN '新指標（High/Low Price Score）'
    ELSE '比較用（Phase 7劣化上位）'
  END as indicator_description,
  
  COUNT(*) as total_patterns,
  SUM(CASE WHEN is_excellent_pattern = true THEN 1 ELSE 0 END) as excellent_patterns,
  SUM(CASE WHEN recent_total_signals >= 5 THEN 1 ELSE 0 END) as patterns_with_verification,
  
  -- 劣化分析（勝率）
  ROUND(AVG(CASE WHEN recent_total_signals >= 5 THEN win_rate_degradation END), 2) as avg_win_rate_degradation,
  ROUND(AVG(CASE WHEN recent_total_signals >= 5 THEN profit_degradation END), 4) as avg_profit_degradation,
  
  -- 継続性分析
  SUM(CASE WHEN pattern_continuity = TRUE THEN 1 ELSE 0 END) as continuing_excellent_patterns,
  ROUND(
    SUM(CASE WHEN pattern_continuity = TRUE THEN 1 ELSE 0 END) * 100.0 / 
    NULLIF(SUM(CASE WHEN pattern_continuity IS NOT NULL THEN 1 ELSE 0 END), 0), 
    1
  ) as continuity_rate_percent,
  
  -- 基準値との比較
  CASE 
    WHEN indicator_group = 'NEW_INDICATOR' THEN
      CASE 
        WHEN AVG(CASE WHEN recent_total_signals >= 5 THEN win_rate_degradation END) < 15.25 
        THEN '✅ 成功（既存最優秀を上回る）'
        WHEN AVG(CASE WHEN recent_total_signals >= 5 THEN win_rate_degradation END) < 10 
        THEN '🎉 大成功（明確な優位性）'
        ELSE '❌ 改善が必要'
      END
    ELSE '比較基準'
  END as performance_evaluation

FROM `kabu-376213.kabu2411.d40_axis_performance_stats`
WHERE recent_total_signals >= 5  -- 統計的に意味のあるデータのみ
GROUP BY indicator_group
ORDER BY indicator_group;

-- 詳細分析: 新指標の個別パフォーマンス
SELECT 
  '📊 新指標個別パフォーマンス詳細' as analysis_title,
  signal_type,
  COUNT(*) as total_patterns,
  SUM(CASE WHEN is_excellent_pattern = true THEN 1 ELSE 0 END) as excellent_patterns,
  SUM(CASE WHEN recent_total_signals >= 5 THEN 1 ELSE 0 END) as patterns_with_verification,
  
  -- 劣化指標
  ROUND(AVG(CASE WHEN recent_total_signals >= 5 THEN win_rate_degradation END), 2) as avg_win_rate_degradation,
  ROUND(AVG(CASE WHEN recent_total_signals >= 5 THEN profit_degradation END), 4) as avg_profit_degradation,
  
  -- 継続性
  ROUND(
    SUM(CASE WHEN pattern_continuity = TRUE THEN 1 ELSE 0 END) * 100.0 / 
    NULLIF(SUM(CASE WHEN pattern_continuity IS NOT NULL THEN 1 ELSE 0 END), 0), 
    1
  ) as continuity_rate_percent,
  
  -- 期間別パフォーマンス
  ROUND(AVG(CASE WHEN is_excellent_pattern = true THEN learning_win_rate END), 1) as avg_learning_win_rate,
  ROUND(AVG(CASE WHEN recent_total_signals >= 5 THEN recent_win_rate END), 1) as avg_verification_win_rate

FROM `kabu-376213.kabu2411.d40_axis_performance_stats`
WHERE indicator_group = 'NEW_INDICATOR' 
  AND recent_total_signals >= 3  -- 少ないデータも含めて詳細確認
GROUP BY signal_type
ORDER BY avg_win_rate_degradation ASC;

-- 比較用指標のベンチマーク確認
SELECT 
  '📋 比較用指標ベンチマーク確認' as analysis_title,
  signal_type,
  COUNT(*) as total_patterns,
  SUM(CASE WHEN is_excellent_pattern = true THEN 1 ELSE 0 END) as excellent_patterns,
  SUM(CASE WHEN recent_total_signals >= 5 THEN 1 ELSE 0 END) as patterns_with_verification,
  
  -- 劣化指標（Phase 7との比較基準）
  ROUND(AVG(CASE WHEN recent_total_signals >= 5 THEN win_rate_degradation END), 2) as avg_win_rate_degradation,
  ROUND(AVG(CASE WHEN recent_total_signals >= 5 THEN profit_degradation END), 4) as avg_profit_degradation,
  
  -- 継続性
  ROUND(
    SUM(CASE WHEN pattern_continuity = TRUE THEN 1 ELSE 0 END) * 100.0 / 
    NULLIF(SUM(CASE WHEN pattern_continuity IS NOT NULL THEN 1 ELSE 0 END), 0), 
    1
  ) as continuity_rate_percent,
  
  'Phase 7で15-17%劣化確認済み' as phase7_reference

FROM `kabu-376213.kabu2411.d40_axis_performance_stats`
WHERE indicator_group = 'COMPARISON_INDICATOR'
  AND recent_total_signals >= 3
GROUP BY signal_type
ORDER BY avg_win_rate_degradation ASC;

-- ============================================================================
-- Step 4: 仮説検証結果の総合判定
-- ============================================================================

WITH hypothesis_test AS (
  SELECT 
    indicator_group,
    AVG(CASE WHEN recent_total_signals >= 5 THEN win_rate_degradation END) as avg_degradation,
    COUNT(CASE WHEN pattern_continuity = TRUE THEN 1 END) as continuing_patterns,
    COUNT(CASE WHEN pattern_continuity IS NOT NULL THEN 1 END) as evaluable_patterns
  FROM `kabu-376213.kabu2411.d40_axis_performance_stats`
  WHERE recent_total_signals >= 5
  GROUP BY indicator_group
)
SELECT 
  '🏆 【最終判定】仮説検証結果' as final_judgment,
  
  -- 新指標の成績
  (SELECT avg_degradation FROM hypothesis_test WHERE indicator_group = 'NEW_INDICATOR') as new_indicator_degradation,
  (SELECT avg_degradation FROM hypothesis_test WHERE indicator_group = 'COMPARISON_INDICATOR') as comparison_degradation,
  
  -- 継続率
  ROUND(
    (SELECT continuing_patterns * 100.0 / evaluable_patterns FROM hypothesis_test WHERE indicator_group = 'NEW_INDICATOR'), 
    1
  ) as new_indicator_continuity_rate,
  
  -- 仮説A検証（最低目標）
  CASE 
    WHEN (SELECT avg_degradation FROM hypothesis_test WHERE indicator_group = 'NEW_INDICATOR') < 15.25 
    THEN '✅ 仮説A成功: 新指標劣化 < 15.25%'
    ELSE '❌ 仮説A失敗: 新指標劣化 >= 15.25%'
  END as hypothesis_a_minimum_target,
  
  -- 仮説B検証（理想目標）
  CASE 
    WHEN (SELECT avg_degradation FROM hypothesis_test WHERE indicator_group = 'NEW_INDICATOR') < 10 
    THEN '🎉 仮説B成功: 新指標劣化 < 10%（明確な優位性）'
    ELSE '⚠️ 仮説B未達: 新指標劣化 >= 10%'
  END as hypothesis_b_ideal_target,
  
  -- 継続性目標
  CASE 
    WHEN (SELECT continuing_patterns * 100.0 / evaluable_patterns FROM hypothesis_test WHERE indicator_group = 'NEW_INDICATOR') > 40 
    THEN '✅ 継続性成功: 継続率 > 40%'
    ELSE '⚠️ 継続性改善余地: 継続率 <= 40%'
  END as continuity_evaluation,
  
  -- 戦略的結論
  CASE 
    WHEN (SELECT avg_degradation FROM hypothesis_test WHERE indicator_group = 'NEW_INDICATOR') < 10 
    THEN '🚀 戦略的結論: 独自指標拡充・実用化を推進'
    WHEN (SELECT avg_degradation FROM hypothesis_test WHERE indicator_group = 'NEW_INDICATOR') < 15.25 
    THEN '📈 戦略的結論: 新指標有効、さらなる改良検討'
    ELSE '🔄 戦略的結論: リアルタイム学習・アルゴリズム転換検討'
  END as strategic_conclusion;

-- ============================================================================
-- Step 5: Phase 6完了報告
-- ============================================================================

SELECT 
  '🎉 Phase 6完了報告（17指標版）' as status,
  '✅ 統計テーブル再構築完了（d30, d40）' as technical_achievement,
  '🎯 新指標劣化分析実行完了' as analysis_achievement,
  '📊 独自性戦略の数値検証完了' as strategic_achievement,
  '⚡ Phase 7比較基準との厳密比較実現' as comparison_achievement,
  '🏆 データ駆動の意思決定基盤構築完了' as decision_foundation,
  CURRENT_TIMESTAMP() as completion_time;

-- 次段階への申し送り
SELECT 
  '📋 Phase 6完了・次段階申し送り' as handover_note,
  '基盤: d30, d40統計テーブル（17指標版）完成' as foundation_status,
  '検証: 新指標の劣化分析・継続性分析完了' as verification_status,
  '判定: 仮説A（最低目標）・仮説B（理想目標）の検証完了' as evaluation_status,
  '方向性: 数値結果に基づく戦略的意思決定実現' as strategic_direction,
  '完了: Phase 3-6の17指標版基盤構築全完了' as completion_scope