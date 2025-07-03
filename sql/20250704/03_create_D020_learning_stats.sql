/*
ファイル: 03_create_D020_learning_stats.sql
説明: D020_learning_stats テーブル作成（学習期間統計 + ユーザー設定統合）
作成日: 2025年7月4日
依存: D010_basic_results（3,700万レコード・37指標版完成）
目的: 学習期間統計＋ユーザー条件設定を統合した基盤テーブル
処理時間: 約10-15分（学習期間データ集計含む）
*/

-- ============================================================================
-- Phase 2: D020_learning_stats作成（設計書準拠・統合版）
-- ============================================================================

-- 処理開始メッセージ
SELECT 
  '🚀 D020_learning_stats作成開始' as message,
  'データソース: D010_basic_results 学習期間（〜2024-06-30）' as source_info,
  '機能: 学習期間統計 + ユーザー設定統合' as purpose,
  'TARGET: 4軸一覧画面・チューニング画面の基盤テーブル' as target_usage,
  '予想処理時間: 約10-15分' as estimated_time,
  CURRENT_TIMESTAMP() as start_time;

-- ============================================================================
-- Step 1: テーブル構造作成
-- ============================================================================

-- 既存テーブル削除（存在する場合）
DROP TABLE IF EXISTS `kabu-376213.kabu2411.D020_learning_stats`;

-- D020_learning_stats テーブル作成（設計書準拠）
CREATE TABLE `kabu-376213.kabu2411.D020_learning_stats` (
  -- 4軸情報
  signal_type STRING NOT NULL,           -- 4軸① (37指標対応)
  signal_bin INT64 NOT NULL,             -- 4軸② (0-9)
  trade_type STRING NOT NULL,            -- 4軸③ 'BUY'/'SELL'
  stock_code STRING NOT NULL,            -- 4軸④ (688銘柄)
  stock_name STRING,                     -- 表示用
  
  -- 学習期間統計（〜2024-06-30）
  total_samples INT64,                   -- サンプル数
  win_samples INT64,                     -- 勝利サンプル数
  win_rate FLOAT64,                      -- 勝率（%）
  avg_profit_rate FLOAT64,              -- 平均利益率（%）
  std_deviation FLOAT64,                -- 標準偏差
  sharpe_ratio FLOAT64,                 -- シャープレシオ
  max_profit_rate FLOAT64,              -- 最大利益率
  min_profit_rate FLOAT64,              -- 最小利益率
  
  -- 期間情報
  first_signal_date DATE,               -- 最初のシグナル日
  last_signal_date DATE,                -- 最後のシグナル日
  signal_frequency FLOAT64,             -- シグナル頻度（日/回）
  
  -- 優秀パターン判定
  is_excellent_pattern BOOLEAN,         -- 優秀パターンフラグ
  pattern_category STRING,              -- カテゴリ('PREMIUM','EXCELLENT','GOOD','NORMAL','CAUTION')
  quality_score FLOAT64,                -- 品質スコア
  
  -- ユーザー設定状況（統合設計）
  decision_status STRING DEFAULT 'pending',  -- 'pending', 'configured', 'rejected'
  decision_note STRING,                      -- 設定時のメモ
  last_decision_date DATE,                  -- 最終設定日
  
  -- 表示制御
  priority_score FLOAT64,               -- ソート用スコア（優秀度×頻度）
  display_order INT64,                  -- 表示順序
  
  -- メタデータ
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY stock_code, signal_type, trade_type
OPTIONS(
  description="学習期間統計+ユーザー設定統合テーブル（4軸一覧・チューニング画面基盤）"
);

SELECT 
  '✅ Step 1完了: D020テーブル構造作成' as status,
  'CLUSTER BY: stock_code, signal_type, trade_type' as clustering_info,
  '機能統合: 学習期間統計 + ユーザー設定' as integration_feature;

-- ============================================================================
-- Step 2: 学習期間統計データ投入（D010から集計）
-- ============================================================================

-- 学習期間データ集計・投入
INSERT INTO `kabu-376213.kabu2411.D020_learning_stats` (
  signal_type,
  signal_bin,
  trade_type,
  stock_code,
  stock_name,
  total_samples,
  win_samples,
  win_rate,
  avg_profit_rate,
  std_deviation,
  sharpe_ratio,
  max_profit_rate,
  min_profit_rate,
  first_signal_date,
  last_signal_date,
  signal_frequency,
  is_excellent_pattern,
  pattern_category,
  quality_score,
  priority_score
)
WITH learning_period_stats AS (
  SELECT 
    signal_type,
    signal_bin,
    trade_type,
    stock_code,
    stock_name,
    
    -- 基本統計
    COUNT(*) as total_samples,
    SUM(CASE WHEN is_win = true THEN 1 ELSE 0 END) as win_samples,
    ROUND(AVG(CASE WHEN is_win = true THEN 1.0 ELSE 0.0 END) * 100, 2) as win_rate,
    ROUND(AVG(baseline_profit_rate), 4) as avg_profit_rate,
    ROUND(STDDEV(baseline_profit_rate), 4) as std_deviation,
    ROUND(SAFE_DIVIDE(AVG(baseline_profit_rate), NULLIF(STDDEV(baseline_profit_rate), 0)), 4) as sharpe_ratio,
    ROUND(MAX(baseline_profit_rate), 4) as max_profit_rate,
    ROUND(MIN(baseline_profit_rate), 4) as min_profit_rate,
    
    -- 期間情報
    MIN(signal_date) as first_signal_date,
    MAX(signal_date) as last_signal_date,
    ROUND(DATE_DIFF(MAX(signal_date), MIN(signal_date), DAY) / NULLIF(COUNT(*), 0), 2) as signal_frequency
    
  FROM `kabu-376213.kabu2411.D010_basic_results`
  WHERE signal_date <= '2024-06-30'  -- 学習期間のみ
  GROUP BY signal_type, signal_bin, trade_type, stock_code, stock_name
  HAVING COUNT(*) >= 5  -- 最小サンプル数フィルタ
),
pattern_evaluation AS (
  SELECT 
    *,
    
    -- 優秀パターン判定（設計書基準）
    CASE 
      WHEN total_samples >= 20 
           AND win_rate >= 55 
           AND avg_profit_rate >= 0.005  -- 0.5%
           AND sharpe_ratio > 0.1
      THEN TRUE 
      ELSE FALSE 
    END as is_excellent_pattern,
    
    -- パターンカテゴリ（設計書基準）
    CASE 
      WHEN win_rate >= 65.0 AND avg_profit_rate >= 0.010 THEN 'PREMIUM'
      WHEN win_rate >= 60.0 AND avg_profit_rate >= 0.008 THEN 'EXCELLENT'
      WHEN win_rate >= 55.0 AND avg_profit_rate >= 0.005 THEN 'GOOD'
      WHEN win_rate >= 50.0 AND avg_profit_rate >= 0.002 THEN 'NORMAL'
      ELSE 'CAUTION'
    END as pattern_category,
    
    -- 品質スコア（勝率×期待値×シャープレシオ）
    ROUND(win_rate * avg_profit_rate * GREATEST(sharpe_ratio, 0.1) * 1000, 2) as quality_score
    
  FROM learning_period_stats
)
SELECT 
  signal_type,
  signal_bin,
  trade_type,
  stock_code,
  stock_name,
  total_samples,
  win_samples,
  win_rate,
  avg_profit_rate,
  std_deviation,
  sharpe_ratio,
  max_profit_rate,
  min_profit_rate,
  first_signal_date,
  last_signal_date,
  signal_frequency,
  is_excellent_pattern,
  pattern_category,
  quality_score,
  
  -- 優先度スコア（品質×頻度）
  ROUND(quality_score * GREATEST(1000.0 / NULLIF(signal_frequency, 0), 1), 2) as priority_score
  
FROM pattern_evaluation
ORDER BY priority_score DESC;

-- 投入結果確認
SELECT 
  '✅ Step 2完了: 学習期間統計データ投入' as status,
  COUNT(*) as total_patterns,
  COUNT(DISTINCT signal_type) as signal_types_count,
  COUNT(DISTINCT stock_code) as stocks_count,
  SUM(CASE WHEN is_excellent_pattern = true THEN 1 ELSE 0 END) as excellent_patterns,
  ROUND(SUM(CASE WHEN is_excellent_pattern = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as excellent_percentage
FROM `kabu-376213.kabu2411.D020_learning_stats`;

-- ============================================================================
-- Step 3: 表示順序設定（MERGE文を使用）
-- ============================================================================

-- 表示順序設定（優先度スコア基準）
MERGE `kabu-376213.kabu2411.D020_learning_stats` AS target
USING (
  SELECT 
    signal_type,
    signal_bin,
    trade_type,
    stock_code,
    ROW_NUMBER() OVER (ORDER BY priority_score DESC, total_samples DESC) as row_num
  FROM `kabu-376213.kabu2411.D020_learning_stats`
) AS ranking
ON target.signal_type = ranking.signal_type
   AND target.signal_bin = ranking.signal_bin
   AND target.trade_type = ranking.trade_type
   AND target.stock_code = ranking.stock_code
WHEN MATCHED THEN
  UPDATE SET display_order = ranking.row_num;

SELECT 
  '✅ Step 3完了: 表示順序設定' as status,
  '基準: priority_score DESC → total_samples DESC' as sorting_criteria;

-- ============================================================================
-- Step 4: データ品質確認
-- ============================================================================

-- 37指標構成確認
SELECT 
  '📊 37指標構成確認' as check_type,
  signal_type,
  COUNT(*) as pattern_count,
  COUNT(DISTINCT stock_code) as stocks_count,
  SUM(CASE WHEN is_excellent_pattern = true THEN 1 ELSE 0 END) as excellent_count,
  ROUND(AVG(win_rate), 1) as avg_win_rate,
  ROUND(AVG(avg_profit_rate * 100), 2) as avg_profit_percent
FROM `kabu-376213.kabu2411.D020_learning_stats`
GROUP BY signal_type
ORDER BY signal_type
LIMIT 10;  -- 一部表示

-- パターンカテゴリ分布
SELECT 
  '🎯 パターンカテゴリ分布' as check_type,
  pattern_category,
  COUNT(*) as pattern_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage,
  ROUND(AVG(win_rate), 1) as avg_win_rate,
  ROUND(AVG(avg_profit_rate * 100), 2) as avg_profit_percent,
  ROUND(AVG(total_samples), 0) as avg_samples
FROM `kabu-376213.kabu2411.D020_learning_stats`
GROUP BY pattern_category
ORDER BY 
  CASE pattern_category
    WHEN 'PREMIUM' THEN 1
    WHEN 'EXCELLENT' THEN 2
    WHEN 'GOOD' THEN 3
    WHEN 'NORMAL' THEN 4
    WHEN 'CAUTION' THEN 5
  END;

-- 優秀パターン詳細確認
SELECT 
  '⭐ 優秀パターン TOP5' as check_type,
  signal_type,
  signal_bin,
  trade_type,
  stock_name,
  total_samples,
  win_rate,
  ROUND(avg_profit_rate * 100, 2) as profit_percent,
  pattern_category,
  ROUND(priority_score, 1) as priority
FROM `kabu-376213.kabu2411.D020_learning_stats`
WHERE is_excellent_pattern = true
ORDER BY priority_score DESC
LIMIT 5;

-- ============================================================================
-- 🎉 D020_learning_stats完成確認
-- ============================================================================

SELECT 
  '🏆 D020_learning_stats完成！' as achievement,
  '✅ 学習期間統計集計完成' as statistics_completion,
  '✅ 優秀パターン判定実装' as pattern_judgment,
  '✅ ユーザー設定欄準備完成' as user_settings_ready,
  '✅ 4軸一覧・チューニング画面基盤完成' as ui_foundation,
  COUNT(*) as total_patterns,
  COUNT(DISTINCT signal_type) as signal_types_37_expected,
  SUM(CASE WHEN is_excellent_pattern = true THEN 1 ELSE 0 END) as excellent_patterns,
  '次Phase: D030_tomorrow_signals等の作成可能' as next_development,
  CURRENT_TIMESTAMP() as completion_time
FROM `kabu-376213.kabu2411.D020_learning_stats`;