/*
ファイル: 04_create_D030_tomorrow_signals.sql
説明: D030_tomorrow_signals テーブル作成（明日シグナル予定・完全版）
作成日: 2025年7月4日
依存: D020_learning_stats（978,532パターン・統計完成）+ 日次シグナル計算
目的: 明日発生予定のシグナル + 学習期間統計の統合テーブル（JOIN完全不要）
処理時間: 約1-2分（テーブル作成のみ）
データ量: 約5万レコード/日（1日分のみ保持）
更新: 日次で全件削除→再作成
*/

-- ============================================================================
-- Phase 3: D030_tomorrow_signals作成（設計書準拠・4軸一覧画面基盤）
-- ============================================================================

-- 処理開始メッセージ
SELECT 
  '🚀 D030_tomorrow_signals作成開始' as message,
  'データソース: D020_learning_stats統計 + 明日シグナル予定' as source_info,
  '機能: 4軸一覧画面のデータソース（JOIN完全不要）' as purpose,
  'TARGET: 超高速4軸一覧表示（1秒以内）' as target_usage,
  '予想処理時間: 約1-2分（構造作成のみ）' as estimated_time,
  CURRENT_TIMESTAMP() as start_time;

-- ============================================================================
-- Step 1: 事前確認
-- ============================================================================

-- 依存テーブル確認
SELECT 
  'Step 1: 事前確認' as check_step,
  (SELECT COUNT(*) FROM `kabu-376213.kabu2411.D020_learning_stats`) as D020_patterns_expected_978532,
  (SELECT COUNT(DISTINCT signal_type) FROM `kabu-376213.kabu2411.D020_learning_stats`) as signal_types_expected_37,
  (SELECT COUNT(DISTINCT stock_code) FROM `kabu-376213.kabu2411.D020_learning_stats`) as stock_codes_expected_687,
  'D020統計データの利用準備確認' as check_purpose;

-- ============================================================================
-- Step 2: 既存テーブル削除と新規作成
-- ============================================================================

-- 既存テーブル確認
SELECT 
  'Step 2: 既存テーブル確認' as check_step,
  (
    SELECT COUNT(*) 
    FROM `kabu-376213.kabu2411.INFORMATION_SCHEMA.TABLES` 
    WHERE table_name = 'D030_tomorrow_signals'
  ) as table_exists,
  CASE 
    WHEN (
      SELECT COUNT(*) 
      FROM `kabu-376213.kabu2411.INFORMATION_SCHEMA.TABLES` 
      WHERE table_name = 'D030_tomorrow_signals'
    ) > 0 THEN 'テーブル存在 - 削除後再作成'
    ELSE 'テーブル未存在 - 新規作成'
  END as action_required;

-- 既存テーブル削除（設計変更対応）
DROP TABLE IF EXISTS `kabu-376213.kabu2411.D030_tomorrow_signals`;

SELECT 
  '✅ Step 2完了: 既存テーブル削除完了' as status,
  '次: Step 3（新テーブル作成）' as next_action;

-- ============================================================================
-- Step 3: D030_tomorrow_signals テーブル新規作成（設計書準拠）
-- ============================================================================

-- 新設計書準拠でテーブル作成
CREATE TABLE `kabu-376213.kabu2411.D030_tomorrow_signals` (
  target_date DATE NOT NULL,             -- 明日の日付（パーティションキー）
  
  -- 4軸情報
  signal_type STRING NOT NULL,           -- 4軸① 'High_Price_Score_7D'等
  signal_bin INT64 NOT NULL,             -- 4軸② 1-20
  trade_type STRING NOT NULL,            -- 4軸③ 'BUY'/'SELL'
  stock_code STRING NOT NULL,            -- 4軸④ '1301'等
  stock_name STRING,                     -- 表示用（冗長データ）
  signal_value FLOAT64,                  -- 予測シグナル値
  
  -- 学習期間統計（D020から複写・JOIN回避）
  total_samples INT64,                   -- サンプル数
  win_samples INT64,                     -- 勝利サンプル数
  win_rate FLOAT64,                      -- 勝率（%）
  avg_profit_rate FLOAT64,              -- 期待値（%）※既に%単位
  std_deviation FLOAT64,                 -- 標準偏差
  sharpe_ratio FLOAT64,                  -- シャープレシオ
  max_profit_rate FLOAT64,              -- 最大利益率
  min_profit_rate FLOAT64,              -- 最小利益率
  
  -- パターン評価（D020から複写）
  is_excellent_pattern BOOLEAN,          -- 優秀パターンフラグ
  pattern_category STRING,               -- 'PREMIUM', 'EXCELLENT', 'GOOD', 'NORMAL', 'CAUTION'
  priority_score FLOAT64,                -- ソート用スコア
  
  -- ユーザー設定状況（D020から複写）
  decision_status STRING DEFAULT 'pending',  -- 'pending', 'configured', 'rejected'
  profit_target_yen FLOAT64,             -- 利確目標（円）
  loss_cut_yen FLOAT64,                  -- 損切設定（円）
  prev_close_gap_condition STRING,       -- 'all', 'above', 'below'
  additional_notes STRING,               -- メモ
  decided_at TIMESTAMP,                  -- 決定日時
  
  -- 期間情報（D020から複写）
  first_signal_date DATE,                -- 学習期間開始日
  last_signal_date DATE,                 -- 学習期間終了日
  
  -- システム項目
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY target_date
CLUSTER BY stock_code, trade_type;

SELECT 
  '✅ Step 3完了: D030_tomorrow_signals テーブル作成完了' as status,
  '構造: 4軸情報 + 学習期間統計（複写） + ユーザー設定' as table_structure,
  'パーティション: target_date（1日分のみ保持）' as partition_info,
  'クラスタ: stock_code, trade_type' as cluster_info,
  '次: データ投入バッチ実装' as next_action;

-- ============================================================================
-- Step 4: テーブル情報確認
-- ============================================================================

-- テーブル作成確認
SELECT 
  '📊 テーブル情報確認' as info_type,
  table_name,
  table_type,
  creation_time,
  'D030テーブル正常作成確認' as status
FROM `kabu-376213.kabu2411.INFORMATION_SCHEMA.TABLES`
WHERE table_name = 'D030_tomorrow_signals';

-- カラム構成確認
SELECT 
  'カラム構成確認' as check_type,
  column_name,
  data_type,
  is_nullable,
  CASE WHEN is_partitioning_column = 'YES' THEN '🔑パーティション' ELSE '' END as partition_flag,
  CASE WHEN clustering_ordinal_position IS NOT NULL THEN CONCAT('🗂️クラスタ(', clustering_ordinal_position, ')') ELSE '' END as cluster_flag
FROM `kabu-376213.kabu2411.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'D030_tomorrow_signals'
ORDER BY ordinal_position;

-- ============================================================================
-- Step 5: 日次更新バッチ用サンプルSQL作成（コメント形式）
-- ============================================================================

SELECT 
  '📝 日次更新バッチ用サンプルSQL' as info_type,
  'D030は日次で全件削除→再作成' as update_strategy,
  '1. DELETE FROM D030 WHERE target_date = CURRENT_DATE()' as step_1,
  '2. INSERT INTO D030 (D020統計 + 明日シグナル予定)' as step_2,
  '3. 約5万レコード/日の想定' as data_volume,
  '別途日次バッチSQLで実装予定' as implementation_note;

/*
日次更新バッチサンプル（実装時に別ファイル作成）:

-- 1. 既存データ削除（明日分のみ）
DELETE FROM `kabu-376213.kabu2411.D030_tomorrow_signals` 
WHERE target_date = DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY);

-- 2. 新データ投入（D020統計 + 明日シグナル予定結合）
INSERT INTO `kabu-376213.kabu2411.D030_tomorrow_signals`
SELECT 
  DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY) as target_date,
  
  -- 4軸情報（明日シグナル予定から）
  tomorrow_signals.signal_type,
  tomorrow_signals.signal_bin,
  tomorrow_signals.trade_type,
  tomorrow_signals.stock_code,
  tomorrow_signals.stock_name,
  tomorrow_signals.signal_value,
  
  -- 学習期間統計（D020から複写）
  stats.total_samples,
  stats.win_samples,
  stats.win_rate,
  stats.avg_profit_rate,
  stats.std_deviation,
  stats.sharpe_ratio,
  stats.max_profit_rate,
  stats.min_profit_rate,
  
  -- パターン評価（D020から複写）
  stats.is_excellent_pattern,
  stats.pattern_category,
  stats.priority_score,
  
  -- ユーザー設定（D020から複写）
  stats.decision_status,
  stats.profit_target_yen,
  stats.loss_cut_yen,
  stats.prev_close_gap_condition,
  stats.additional_notes,
  stats.decided_at,
  
  -- 期間情報（D020から複写）
  stats.first_signal_date,
  stats.last_signal_date,
  
  -- システム項目
  CURRENT_TIMESTAMP() as created_at,
  CURRENT_TIMESTAMP() as updated_at
  
FROM (明日シグナル予定計算) tomorrow_signals
LEFT JOIN `kabu-376213.kabu2411.D020_learning_stats` stats
  ON tomorrow_signals.signal_type = stats.signal_type
  AND tomorrow_signals.signal_bin = stats.signal_bin
  AND tomorrow_signals.trade_type = stats.trade_type
  AND tomorrow_signals.stock_code = stats.stock_code;
*/

-- ============================================================================
-- Step 6: D030完成確認
-- ============================================================================

SELECT 
  '🎉 D030_tomorrow_signals作成完了！' as achievement,
  '✅ テーブル構造作成完成' as table_creation,
  '✅ 4軸一覧画面基盤準備完成' as ui_foundation,
  '✅ JOIN完全不要設計実現' as performance_optimization,
  '✅ 日次更新バッチ準備完成' as batch_ready,
  '次Phase: P010_batch_status作成 or 日次バッチ実装' as next_development,
  'API実装: 4軸一覧画面（D030単一テーブル）実装可能' as api_ready,
  CURRENT_TIMESTAMP() as completion_time;

-- ============================================================================
-- 実行完了メッセージ
-- ============================================================================

SELECT 
  'D030_tomorrow_signals テーブル作成が完了しました' as message,
  '設計書準拠: 4軸情報 + 学習期間統計（複写）' as structure,
  '目的達成: JOIN完全不要の4軸一覧画面基盤' as achievement,
  '準備完了: 日次バッチ実装 or P010作成へ' as next_action,
  '🚀 Signal Axis Trader 超高速4軸一覧基盤完成！' as celebration,
  CURRENT_TIMESTAMP() as completion_time;