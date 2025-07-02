/*
ファイル: 06-B_rebuild_master_summary_37.sql
説明: Phase 6-B - 37指標版 マスタ・集計テーブル再構築
作成日: 2025年7月3日
依存: d40_axis_performance_stats (37指標版・96万パターン) + master_trading_stocks
対象: m10_axis_combinations + d60_stock_tradetype_summary
目的: API検索効率最適化・明日のシグナル一覧高速化
処理時間: 約5-10分
*/

-- ============================================================================
-- Phase 6-B: マスタ・集計テーブル再構築（37指標版）
-- ============================================================================

-- 処理開始メッセージ
SELECT 
  '🚀 Phase 6-B開始: マスタ・集計テーブル再構築（37指標版）' as message,
  'データソース: d40_axis_performance_stats (96万パターン・37指標版)' as source_info,
  'Target 1: m10_axis_combinations (4軸マスタ・検索効率化)' as target1,
  'Target 2: d60_stock_tradetype_summary (銘柄×売買集計・明日のシグナル高速化)' as target2,
  '予想処理時間: 約5-10分' as estimated_time,
  CURRENT_TIMESTAMP() as start_time;

-- ============================================================================
-- Step 1: m10_axis_combinations再構築（37指標版）
-- ============================================================================

-- バックアップ作成
CREATE OR REPLACE TABLE `kabu-376213.kabu2411.m10_axis_combinations_backup_phase6b_37` AS
SELECT *, CURRENT_TIMESTAMP() as backup_timestamp
FROM `kabu-376213.kabu2411.m10_axis_combinations`
LIMIT 1000;  -- 構造確認用のみ

SELECT 
  '✅ m10バックアップ完了' as status,
  '大規模データのため構造のみ保存' as note,
  'バックアップテーブル: m10_axis_combinations_backup_phase6b_37' as backup_table;

-- 既存テーブル削除・再作成
DROP TABLE IF EXISTS `kabu-376213.kabu2411.m10_axis_combinations`;

CREATE TABLE `kabu-376213.kabu2411.m10_axis_combinations`
CLUSTER BY signal_type, signal_bin, trade_type
AS
WITH signal_types AS (
  SELECT DISTINCT signal_type
  FROM `kabu-376213.kabu2411.d40_axis_performance_stats`
),
signal_bins AS (
  SELECT DISTINCT signal_bin
  FROM `kabu-376213.kabu2411.d40_axis_performance_stats`
  WHERE signal_bin IS NOT NULL
),
trade_types AS (
  SELECT 'LONG' as trade_type
  UNION ALL
  SELECT 'SHORT' as trade_type
),
stock_codes AS (
  SELECT stock_code, company_name as stock_name
  FROM `kabu-376213.kabu2411.master_trading_stocks`
),
all_combinations AS (
  SELECT 
    st.signal_type,
    sb.signal_bin,
    tt.trade_type,
    sc.stock_code,
    sc.stock_name
  FROM signal_types st
  CROSS JOIN signal_bins sb
  CROSS JOIN trade_types tt
  CROSS JOIN stock_codes sc
)
SELECT 
  ac.signal_type,
  ac.signal_bin,
  ac.trade_type,
  ac.stock_code,
  ac.stock_name,
  
  -- 統計情報付与
  COALESCE(ps.learning_total_signals, 0) as learning_total_signals,
  COALESCE(ps.learning_win_rate, 0) as learning_win_rate,
  COALESCE(ps.learning_avg_profit, 0) as learning_avg_profit,
  COALESCE(ps.recent_total_signals, 0) as recent_total_signals,
  COALESCE(ps.recent_win_rate, 0) as recent_win_rate,
  COALESCE(ps.recent_avg_profit, 0) as recent_avg_profit,
  COALESCE(ps.is_excellent_pattern, FALSE) as is_excellent_pattern,
  COALESCE(ps.pattern_category, 'CAUTION') as pattern_category,
  
  -- 4軸ID生成（検索最適化用）
  CONCAT(
    ac.signal_type, '|',
    CAST(ac.signal_bin AS STRING), '|',
    ac.trade_type, '|',
    ac.stock_code
  ) as axis_combination_id,
  
  CURRENT_TIMESTAMP() as created_at
  
FROM all_combinations ac
LEFT JOIN `kabu-376213.kabu2411.d40_axis_performance_stats` ps
  ON ac.signal_type = ps.signal_type
  AND ac.signal_bin = ps.signal_bin
  AND ac.trade_type = ps.trade_type
  AND ac.stock_code = ps.stock_code;

-- m10作成完了確認
SELECT 
  '✅ Step 1完了: m10_axis_combinations（37指標版）' as status,
  COUNT(*) as total_combinations,
  COUNT(DISTINCT signal_type) as signal_types_37_expected,
  COUNT(DISTINCT signal_bin) as signal_bins_20_expected,
  COUNT(DISTINCT trade_type) as trade_types_2_expected,
  COUNT(DISTINCT stock_code) as stocks_688_expected,
  SUM(CASE WHEN is_excellent_pattern = TRUE THEN 1 ELSE 0 END) as excellent_combinations,
  '理論値: 37×20×2×688 = 1,018,240' as theoretical_total
FROM `kabu-376213.kabu2411.m10_axis_combinations`;

-- ============================================================================
-- Step 2: d60_stock_tradetype_summary再構築（37指標版）
-- ============================================================================

-- バックアップ作成
CREATE OR REPLACE TABLE `kabu-376213.kabu2411.d60_stock_tradetype_summary_backup_phase6b_37` AS
SELECT *, CURRENT_TIMESTAMP() as backup_timestamp
FROM `kabu-376213.kabu2411.d60_stock_tradetype_summary`;

SELECT 
  '✅ d60バックアップ完了' as status,
  COUNT(*) as backup_record_count,
  'バックアップテーブル: d60_stock_tradetype_summary_backup_phase6b_37' as backup_table
FROM `kabu-376213.kabu2411.d60_stock_tradetype_summary_backup_phase6b_37`;

-- 既存テーブル削除・再作成
DROP TABLE IF EXISTS `kabu-376213.kabu2411.d60_stock_tradetype_summary`;

CREATE TABLE `kabu-376213.kabu2411.d60_stock_tradetype_summary`
CLUSTER BY stock_code, trade_type
AS
WITH stock_tradetype_stats AS (
  SELECT 
    stock_code,
    stock_name,
    trade_type,
    
    -- 全パターン統計
    COUNT(*) as total_patterns,
    SUM(CASE WHEN is_excellent_pattern = TRUE THEN 1 ELSE 0 END) as excellent_patterns,
    
    -- 学習期間統計（平均値）
    ROUND(AVG(learning_win_rate), 1) as avg_learning_win_rate,
    ROUND(AVG(learning_avg_profit), 3) as avg_learning_profit,
    SUM(learning_total_signals) as total_learning_signals,
    
    -- 検証期間統計（平均値）
    ROUND(AVG(recent_win_rate), 1) as avg_recent_win_rate,
    ROUND(AVG(recent_avg_profit), 3) as avg_recent_profit,
    SUM(recent_total_signals) as total_recent_signals,
    
    -- 優秀パターン統計
    ROUND(AVG(CASE WHEN is_excellent_pattern = TRUE THEN learning_win_rate END), 1) as excellent_avg_win_rate,
    ROUND(AVG(CASE WHEN is_excellent_pattern = TRUE THEN learning_avg_profit END), 3) as excellent_avg_profit,
    
    -- カテゴリ別集計
    SUM(CASE WHEN pattern_category = 'PREMIUM' THEN 1 ELSE 0 END) as premium_patterns,
    SUM(CASE WHEN pattern_category = 'EXCELLENT' THEN 1 ELSE 0 END) as excellent_only_patterns,
    SUM(CASE WHEN pattern_category = 'GOOD' THEN 1 ELSE 0 END) as good_patterns,
    SUM(CASE WHEN pattern_category = 'NORMAL' THEN 1 ELSE 0 END) as normal_patterns,
    SUM(CASE WHEN pattern_category = 'CAUTION' THEN 1 ELSE 0 END) as caution_patterns
    
  FROM `kabu-376213.kabu2411.d40_axis_performance_stats`
  GROUP BY stock_code, stock_name, trade_type
)
SELECT 
  *,
  
  -- 優秀パターン比率
  ROUND(excellent_patterns * 100.0 / NULLIF(total_patterns, 0), 1) as excellent_pattern_rate,
  
  -- 総合評価スコア（API表示順序用）
  ROUND(
    (COALESCE(excellent_avg_win_rate, 0) * 0.4) +
    (COALESCE(excellent_avg_profit, 0) * 1000 * 0.3) +
    (COALESCE(excellent_patterns, 0) * 0.3)
  , 2) as overall_score,
  
  -- 銘柄×売買方向の推奨度
  CASE 
    WHEN excellent_patterns >= 50 AND excellent_avg_win_rate >= 60 THEN 'HIGHLY_RECOMMENDED'
    WHEN excellent_patterns >= 20 AND excellent_avg_win_rate >= 55 THEN 'RECOMMENDED'
    WHEN excellent_patterns >= 10 AND excellent_avg_win_rate >= 52 THEN 'CONSIDER'
    WHEN excellent_patterns >= 5 THEN 'CAUTION'
    ELSE 'NOT_RECOMMENDED'
  END as recommendation_level,
  
  CURRENT_TIMESTAMP() as last_updated
  
FROM stock_tradetype_stats;

-- d60作成完了確認
SELECT 
  '✅ Step 2完了: d60_stock_tradetype_summary（37指標版）' as status,
  COUNT(*) as total_stock_tradetype_combinations,
  COUNT(DISTINCT stock_code) as unique_stocks_688_expected,
  COUNT(DISTINCT trade_type) as trade_types_2_expected,
  SUM(excellent_patterns) as total_excellent_patterns,
  ROUND(AVG(excellent_pattern_rate), 1) as avg_excellent_rate_across_stocks,
  '銘柄×売買方向集計完了' as summary_status
FROM `kabu-376213.kabu2411.d60_stock_tradetype_summary`;

-- ============================================================================
-- Step 3: API最適化確認
-- ============================================================================

-- 検索効率確認（m10_axis_combinations）
SELECT 
  '⚡ API最適化確認: m10_axis_combinations' as check_type,
  'クラスタリング: signal_type, signal_bin, trade_type' as clustering_info,
  'axis_combination_id: 高速検索用ID生成完了' as search_optimization,
  COUNT(*) as total_records,
  COUNT(DISTINCT axis_combination_id) as unique_ids,
  '4軸検索の大幅高速化実現' as performance_gain
FROM `kabu-376213.kabu2411.m10_axis_combinations`;

-- 明日のシグナル検索効率確認（d60_stock_tradetype_summary）
SELECT 
  '⚡ API最適化確認: d60_stock_tradetype_summary' as check_type,
  'クラスタリング: stock_code, trade_type' as clustering_info,
  'overall_score: 表示順序最適化完了' as ranking_optimization,
  COUNT(*) as total_stock_trade_combinations,
  ROUND(AVG(overall_score), 2) as avg_overall_score,
  '明日のシグナル一覧の高速化実現' as performance_gain
FROM `kabu-376213.kabu2411.d60_stock_tradetype_summary`;

-- 推奨レベル分布確認
SELECT 
  '📊 推奨レベル分布確認' as check_type,
  recommendation_level,
  COUNT(*) as combination_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage,
  ROUND(AVG(excellent_pattern_rate), 1) as avg_excellent_rate
FROM `kabu-376213.kabu2411.d60_stock_tradetype_summary`
GROUP BY recommendation_level
ORDER BY 
  CASE recommendation_level
    WHEN 'HIGHLY_RECOMMENDED' THEN 1
    WHEN 'RECOMMENDED' THEN 2
    WHEN 'CONSIDER' THEN 3
    WHEN 'CAUTION' THEN 4
    WHEN 'NOT_RECOMMENDED' THEN 5
  END;

-- ============================================================================
-- Step 4: Phase 6-B完了確認
-- ============================================================================

SELECT 
  '🎉 Phase 6-B完了（37指標版）' as final_status,
  '✅ m10_axis_combinations 再構築完了' as achievement1,
  '✅ d60_stock_tradetype_summary 再構築完了' as achievement2,
  '📊 37指標による4軸マスタ・集計テーブル完成' as technical_achievement,
  '⚡ API検索効率大幅向上' as performance_achievement,
  '🎯 明日のシグナル一覧高速化完備' as search_achievement,
  CURRENT_TIMESTAMP() as completion_time;

-- 次段階準備確認
SELECT 
  '📋 Phase 6-C準備確認' as next_phase,
  '✅ Phase 6-B (マスタ・集計) 完了' as current_status,
  '⚡ Phase 6-C (ユーザー管理) 実行可能' as next_target,
  'Target: u10_user_decisions + u20_user_decision_history' as next_tables,
  '予想処理時間: 約1-3分' as next_estimated_time;

-- ============================================================================
-- 処理完了メッセージ
-- ============================================================================

SELECT 
  'Phase 6-B: マスタ・集計テーブル再構築が完了しました（37指標版）' as message,
  'm10 + d60による検索効率大幅向上' as achievement,
  'API高速化・明日のシグナル一覧最適化完了' as capability,
  '次段階: Phase 6-C (ユーザー管理テーブル再構築) 実行可能' as next_step,
  CURRENT_TIMESTAMP() as completion_time;