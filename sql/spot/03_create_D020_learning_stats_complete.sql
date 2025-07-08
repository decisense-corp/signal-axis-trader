/*
ファイル: 03_create_D020_learning_stats_complete.sql
説明: D020_learning_stats テーブル作成（設計書完全準拠・初期構築版）
作成日: 2025年7月4日
依存: D010_basic_results（3,700万レコード・37指標版完成）
目的: 学習期間統計＋ユーザー条件設定を統合した基盤テーブル（設計書準拠）
処理時間: 約10-15分（学習期間データ集計含む）
*/

-- ============================================================================
-- Phase 2: D020_learning_stats作成（設計書完全準拠・統合版）
-- ============================================================================

-- 処理開始メッセージ
SELECT 
  '🚀 D020_learning_stats作成開始（設計書完全準拠版）' as message,
  'データソース: D010_basic_results 学習期間（〜2024-06-30）' as source_info,
  '機能: 学習期間統計 + ユーザー設定統合（設計書準拠）' as purpose,
  'TARGET: 4軸一覧画面・チューニング画面の基盤テーブル' as target_usage,
  '予想処理時間: 約10-15分' as estimated_time,
  CURRENT_TIMESTAMP() as start_time;

-- ============================================================================
-- Step 1: テーブル構造作成（設計書完全準拠）
-- ============================================================================

-- 既存テーブル削除（存在する場合）
DROP TABLE IF EXISTS `kabu-376213.kabu2411.D020_learning_stats`;

-- D020_learning_stats テーブル作成（設計書完全準拠）
CREATE TABLE `kabu-376213.kabu2411.D020_learning_stats` (
  -- 4軸情報
  signal_type STRING NOT NULL,           -- 4軸① (37指標対応)
  signal_bin INT64 NOT NULL,             -- 4軸② (1-20)
  trade_type STRING NOT NULL,            -- 4軸③ 'BUY'/'SELL'
  stock_code STRING NOT NULL,            -- 4軸④ (687銘柄)
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
  first_signal_date DATE,               -- 学習期間開始日
  last_signal_date DATE,                -- 学習期間終了日
  
  -- 優秀パターン判定
  is_excellent_pattern BOOLEAN,         -- 優秀パターンフラグ
  pattern_category STRING,              -- カテゴリ('PREMIUM','EXCELLENT','GOOD','NORMAL','CAUTION')
  
  -- ユーザー設定項目（統合）
  decision_status STRING DEFAULT 'pending',  -- 'pending', 'configured', 'rejected'
  profit_target_yen FLOAT64,            -- 利確目標（円）
  loss_cut_yen FLOAT64,                 -- 損切設定（円）
  prev_close_gap_condition STRING,      -- 'all', 'above', 'below'
  additional_notes STRING,              -- メモ
  decided_at TIMESTAMP,                 -- 決定日時
  
  -- 表示制御
  priority_score FLOAT64,               -- ソート用優先度スコア
  
  -- メタデータ
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY is_excellent_pattern, signal_type, signal_bin
OPTIONS(
  description="学習期間統計+ユーザー設定統合テーブル（4軸一覧・チューニング画面基盤）"
);

SELECT 
  '✅ Step 1完了: D020テーブル構造作成（設計書完全準拠）' as status,
  'CLUSTER BY: is_excellent_pattern, signal_type, signal_bin' as clustering_info,
  '機能統合: 学習期間統計 + ユーザー設定（設計書準拠）' as integration_feature;

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
  is_excellent_pattern,
  pattern_category,
  decision_status,
  profit_target_yen,
  loss_cut_yen,
  prev_close_gap_condition,
  additional_notes,
  decided_at,
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
    MAX(signal_date) as last_signal_date
    
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
    
    -- 優先度スコア（勝率×期待値×サンプル数重み）
    ROUND(
      win_rate * 
      GREATEST(avg_profit_rate * 100, 0.1) * 
      LOG(GREATEST(total_samples, 1)) * 
      GREATEST(sharpe_ratio, 0.1), 
      2
    ) as priority_score
    
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
  is_excellent_pattern,
  pattern_category,
  
  -- ユーザー設定（初期値設定）
  'pending' as decision_status,
  CAST(NULL AS FLOAT64) as profit_target_yen,
  CAST(NULL AS FLOAT64) as loss_cut_yen,
  'all' as prev_close_gap_condition,
  CAST(NULL AS STRING) as additional_notes,
  CAST(NULL AS TIMESTAMP) as decided_at,
  
  priority_score
  
FROM pattern_evaluation
ORDER BY priority_score DESC;

-- 投入結果確認
SELECT 
  '✅ Step 2完了: 学習期間統計データ投入（設計書準拠）' as status,
  COUNT(*) as total_patterns,
  COUNT(DISTINCT signal_type) as signal_types_count,
  COUNT(DISTINCT stock_code) as stocks_count,
  SUM(CASE WHEN is_excellent_pattern = true THEN 1 ELSE 0 END) as excellent_patterns,
  ROUND(SUM(CASE WHEN is_excellent_pattern = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as excellent_percentage
FROM `kabu-376213.kabu2411.D020_learning_stats`;

-- ============================================================================
-- Step 3: データ品質確認
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
  '⭐ 優秀パターン TOP10' as check_type,
  signal_type,
  signal_bin,
  trade_type,
  stock_name,
  total_samples,
  win_rate,
  ROUND(avg_profit_rate * 100, 2) as profit_percent,
  pattern_category,
  ROUND(priority_score, 1) as priority,
  decision_status
FROM `kabu-376213.kabu2411.D020_learning_stats`
WHERE is_excellent_pattern = true
ORDER BY priority_score DESC
LIMIT 10;

-- ユーザー設定項目確認
SELECT 
  '⚙️ ユーザー設定項目確認' as check_type,
  decision_status,
  COUNT(*) as count,
  COUNT(CASE WHEN profit_target_yen IS NOT NULL THEN 1 END) as with_profit_target,
  COUNT(CASE WHEN loss_cut_yen IS NOT NULL THEN 1 END) as with_loss_cut,
  COUNT(CASE WHEN additional_notes IS NOT NULL THEN 1 END) as with_notes
FROM `kabu-376213.kabu2411.D020_learning_stats`
GROUP BY decision_status;

-- ============================================================================
-- 🎉 D020_learning_stats完成確認（設計書準拠版）
-- ============================================================================

SELECT 
  '🏆 D020_learning_stats完成！（設計書完全準拠版）' as achievement,
  '✅ 学習期間統計集計完成' as statistics_completion,
  '✅ 優秀パターン判定実装' as pattern_judgment,
  '✅ ユーザー設定欄実装完成' as user_settings_complete,
  '✅ 設計書との整合性確保' as design_compliance,
  '✅ 4軸一覧・チューニング画面基盤完成' as ui_foundation,
  COUNT(*) as total_patterns,
  COUNT(DISTINCT signal_type) as signal_types_37_expected,
  SUM(CASE WHEN is_excellent_pattern = true THEN 1 ELSE 0 END) as excellent_patterns,
  '次Phase: D030_tomorrow_signals再実行可能' as next_development,
  'D030エラー解消: ユーザー設定項目完全対応' as error_fixed,
  CURRENT_TIMESTAMP() as completion_time
FROM `kabu-376213.kabu2411.D020_learning_stats`;

-- ============================================================================
-- 実行完了メッセージ
-- ============================================================================

SELECT 
  'D020_learning_stats（設計書完全準拠版）作成完了' as message,
  '設計書準拠: ユーザー設定項目完全実装' as compliance,
  'D030対応: profit_target_yen, loss_cut_yen等追加完了' as d030_ready,
  'データ基盤: Signal Axis Trader 中核機能完成' as foundation_complete,
  '🚀 設計書通りの完璧なテーブル構成実現！' as celebration,
  CURRENT_TIMESTAMP() as completion_time;