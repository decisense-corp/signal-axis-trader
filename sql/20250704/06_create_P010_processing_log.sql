/*
ファイル: 06_create_P010_processing_log.sql
説明: P010_processing_log テーブル作成（処理ログ管理）
作成日: 2025年7月4日
依存: なし（独立テーブル）
目的: バッチ処理の実行ログ管理・エラー追跡・運用監視
処理時間: 約10秒（テーブル作成のみ）
データ量: 約1,000-2,000レコード/月
更新: 各バッチ処理実行時に追記
*/

-- ============================================================================
-- Phase 4: P010_processing_log作成（運用管理基盤）
-- ============================================================================

-- 処理開始メッセージ
SELECT 
  '🚀 P010_processing_log作成開始' as message,
  '機能: バッチ処理実行ログ・エラー監視・運用管理' as purpose,
  'データ保持: 1年間（パーティション期限設定）' as retention_policy,
  '用途: 日次バッチ監視・障害対応・パフォーマンス分析' as usage,
  '予想処理時間: 約10秒（構造作成のみ）' as estimated_time,
  CURRENT_TIMESTAMP() as start_time;

-- ============================================================================
-- Step 1: 事前確認
-- ============================================================================

-- 既存テーブル確認
SELECT 
  'Step 1: 既存テーブル確認' as check_step,
  (
    SELECT COUNT(*) 
    FROM `kabu-376213.kabu2411.INFORMATION_SCHEMA.TABLES` 
    WHERE table_name = 'P010_processing_log'
  ) as table_exists,
  CASE 
    WHEN (
      SELECT COUNT(*) 
      FROM `kabu-376213.kabu2411.INFORMATION_SCHEMA.TABLES` 
      WHERE table_name = 'P010_processing_log'
    ) > 0 THEN 'テーブル存在 - 削除後再作成'
    ELSE 'テーブル未存在 - 新規作成'
  END as action_required;

-- ============================================================================
-- Step 2: 既存テーブル削除と新規作成
-- ============================================================================

-- 既存テーブルがある場合は削除
DROP TABLE IF EXISTS `kabu-376213.kabu2411.P010_processing_log`;

SELECT 
  '✅ Step 2開始: 既存テーブル削除完了' as status,
  '次: 新規テーブル作成' as next_action;

-- P010_processing_log テーブル作成
CREATE TABLE `kabu-376213.kabu2411.P010_processing_log` (
  -- プロセス識別情報
  process_id STRING NOT NULL,            -- UUID or タイムスタンプベースID
  process_type STRING NOT NULL,          -- 処理種別（下記参照）
  process_name STRING,                   -- 処理名（人間が読める形式）
  
  -- 実行時間情報
  process_date DATE NOT NULL,            -- 処理日（パーティション用）
  start_time TIMESTAMP NOT NULL,         -- 開始時刻
  end_time TIMESTAMP,                    -- 終了時刻
  duration_seconds INT64,                -- 処理時間（秒）
  
  -- 実行結果
  status STRING NOT NULL,                -- 'RUNNING', 'SUCCESS', 'FAILED', 'WARNING'
  records_processed INT64,               -- 処理レコード数
  records_inserted INT64,                -- 挿入レコード数
  records_updated INT64,                 -- 更新レコード数
  records_deleted INT64,                 -- 削除レコード数
  records_error INT64,                   -- エラーレコード数
  
  -- エラー情報
  error_code STRING,                     -- エラーコード
  error_message STRING,                  -- エラーメッセージ
  error_details STRING,                  -- 詳細なエラー情報（スタックトレース等）
  
  -- 追加情報
  target_table STRING,                   -- 対象テーブル名
  target_date DATE,                      -- 対象日付（該当する場合）
  execution_mode STRING,                 -- 'FULL', 'INCREMENTAL', 'RETRY'
  retry_count INT64 DEFAULT 0,           -- リトライ回数
  
  -- 環境情報
  executed_by STRING DEFAULT 'SYSTEM',   -- 実行者（将来の拡張用）
  execution_environment STRING,          -- 'PRODUCTION', 'TEST', 'DEVELOPMENT'
  
  -- メタデータ
  additional_info JSON,                  -- その他の情報（JSON形式）
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY process_date
CLUSTER BY process_type, status
OPTIONS(
  description="バッチ処理の実行ログ管理。エラー追跡・パフォーマンス分析・運用監視用。",
  partition_expiration_days=365  -- 1年間保持
);

SELECT 
  '✅ Step 2完了: P010_processing_log テーブル作成完了' as status,
  'パーティション: process_date（1年保持）' as partition_info,
  'クラスタ: process_type, status' as cluster_info,
  '次: Step 3（process_type定義確認）' as next_action;

-- ============================================================================
-- Step 3: process_type定義と使用例
-- ============================================================================

-- process_type定義一覧
WITH process_types AS (
  SELECT 'DAILY_SIGNAL_CALC' as process_type, 'D030日次シグナル計算' as description, 'D030_tomorrow_signals' as target_table
  UNION ALL
  SELECT 'DAILY_RESULT_UPDATE', 'D010日次結果更新', 'D010_basic_results'
  UNION ALL
  SELECT 'STATS_RECALC', 'D020統計再計算', 'D020_learning_stats'
  UNION ALL
  SELECT 'USER_DECISION_UPDATE', 'ユーザー設定更新', 'D020_learning_stats'
  UNION ALL
  SELECT 'MASTER_UPDATE', 'マスタデータ更新', 'master_trading_stocks'
  UNION ALL
  SELECT 'DATA_VALIDATION', 'データ検証処理', '複数テーブル'
  UNION ALL
  SELECT 'BACKUP_PROCESS', 'バックアップ処理', '全テーブル'
  UNION ALL
  SELECT 'CLEANUP_PROCESS', 'クリーンアップ処理', '複数テーブル'
)
SELECT 
  '📋 process_type定義一覧' as info_type,
  process_type,
  description,
  target_table
FROM process_types
ORDER BY process_type;

-- 使用例：成功ログの挿入サンプル
SELECT 
  '💡 使用例1: 成功ログ挿入' as example_type,
  '下記のINSERT文を実行してください' as instruction,
  'process_typeはDAILY_SIGNAL_CALCを使用' as note;

/*
使用例1: 成功ログ
INSERT INTO `kabu-376213.kabu2411.P010_processing_log` (
  process_id, process_type, process_name, process_date,
  start_time, end_time, duration_seconds, status,
  records_processed, records_inserted, target_table, target_date,
  execution_mode, execution_environment
) VALUES (
  GENERATE_UUID(), 'DAILY_SIGNAL_CALC', 'D030日次シグナル計算',
  CURRENT_DATE(), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 300 SECOND),
  CURRENT_TIMESTAMP(), 300, 'SUCCESS',
  49464, 49464, 'D030_tomorrow_signals', DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY),
  'FULL', 'PRODUCTION'
);
*/

-- 使用例：エラーログの挿入サンプル
SELECT 
  '💡 使用例2: エラーログ挿入' as example_type,
  '下記のINSERT文を実行してください' as instruction,
  'error_codeとerror_messageを必ず記録' as note;

/*
使用例2: エラーログ
INSERT INTO `kabu-376213.kabu2411.P010_processing_log` (
  process_id, process_type, process_name, process_date,
  start_time, status, error_code, error_message,
  target_table, execution_mode, execution_environment
) VALUES (
  GENERATE_UUID(), 'DAILY_RESULT_UPDATE', 'D010日次結果更新',
  CURRENT_DATE(), CURRENT_TIMESTAMP(), 'FAILED',
  'ERR_NO_DATA', 'daily_quotesに最新データが存在しません',
  'D010_basic_results', 'INCREMENTAL', 'PRODUCTION'
);
*/

-- ============================================================================
-- Step 4: 監視クエリサンプル
-- ============================================================================

-- 本日の処理状況サマリー（サンプル）
SELECT 
  '📊 監視クエリ1: 本日の処理状況' as query_type,
  '下記のクエリで本日の処理状況を確認' as description;

/*
監視クエリ1: 本日の処理状況
SELECT 
  process_type,
  COUNT(*) as execution_count,
  SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success_count,
  SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_count,
  AVG(duration_seconds) as avg_duration_seconds,
  MAX(end_time) as last_execution
FROM `kabu-376213.kabu2411.P010_processing_log`
WHERE process_date = CURRENT_DATE()
GROUP BY process_type
ORDER BY process_type;
*/

-- 最近のエラー確認（サンプル）
SELECT 
  '📊 監視クエリ2: 最近のエラー' as query_type,
  '過去7日間のエラーを確認' as description;

/*
監視クエリ2: 最近のエラー
SELECT 
  process_type,
  process_name,
  start_time,
  error_code,
  error_message,
  target_table
FROM `kabu-376213.kabu2411.P010_processing_log`
WHERE status = 'FAILED'
  AND process_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
ORDER BY start_time DESC
LIMIT 10;
*/

-- 処理時間トレンド（サンプル）
SELECT 
  '📊 監視クエリ3: 処理時間トレンド' as query_type,
  '過去30日間の処理時間推移を分析' as description;

/*
監視クエリ3: 処理時間トレンド
SELECT 
  process_type,
  process_date,
  AVG(duration_seconds) as avg_duration,
  MAX(duration_seconds) as max_duration,
  COUNT(*) as execution_count
FROM `kabu-376213.kabu2411.P010_processing_log`
WHERE status = 'SUCCESS'
  AND process_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY process_type, process_date
ORDER BY process_type, process_date DESC;
*/

-- ============================================================================
-- Step 5: 運用ガイドライン
-- ============================================================================

SELECT 
  '📚 P010運用ガイドライン' as guideline_type,
  '1. 全バッチ処理の開始・終了時にログ記録' as guideline_1,
  '2. エラー発生時は必ず詳細情報を記録' as guideline_2,
  '3. 日次で前日分の処理結果を確認' as guideline_3,
  '4. 週次でエラー傾向分析を実施' as guideline_4,
  '5. 月次でパフォーマンス劣化をチェック' as guideline_5,
  '保持期間: 1年（自動削除）' as retention_policy;

-- ============================================================================
-- Step 6: テーブル情報確認
-- ============================================================================

-- テーブル作成確認
SELECT 
  '📊 テーブル情報確認' as info_type,
  table_name,
  table_type,
  creation_time,
  'P010テーブル正常作成確認' as status
FROM `kabu-376213.kabu2411.INFORMATION_SCHEMA.TABLES`
WHERE table_name = 'P010_processing_log';

-- カラム構成確認
SELECT 
  'カラム構成確認' as check_type,
  column_name,
  data_type,
  is_nullable,
  CASE WHEN is_partitioning_column = 'YES' THEN '🔑パーティション' ELSE '' END as partition_flag,
  CASE WHEN clustering_ordinal_position IS NOT NULL THEN CONCAT('🗂️クラスタ(', clustering_ordinal_position, ')') ELSE '' END as cluster_flag
FROM `kabu-376213.kabu2411.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'P010_processing_log'
ORDER BY ordinal_position;

-- ============================================================================
-- 🎉 P010_processing_log作成完了確認
-- ============================================================================

SELECT 
  '🏆 P010_processing_log作成完了！' as achievement,
  '✅ バッチ処理ログ管理基盤完成' as log_management,
  '✅ エラー追跡機能実装' as error_tracking,
  '✅ パフォーマンス分析対応' as performance_analysis,
  '✅ 1年間自動保持設定完了' as retention_setting,
  '📊 運用監視体制構築可能' as monitoring_ready,
  '次Phase: 日次バッチへのログ組み込み' as next_development,
  CURRENT_TIMESTAMP() as completion_time;

-- ============================================================================
-- DB構築完了確認
-- ============================================================================

SELECT 
  '🎊 Signal Axis Trader DB構築完了！' as final_achievement,
  '✅ M010_signal_bins（マスタ）' as table_1,
  '✅ D010_basic_results（基本結果）' as table_2,
  '✅ D020_learning_stats（統計+設定）' as table_3,
  '✅ D030_tomorrow_signals（明日予定）' as table_4,
  '✅ P010_processing_log（処理ログ）' as table_5,
  '🚀 5テーブル構成完成！' as db_structure,
  'API開発・運用開始可能' as system_readiness,
  CURRENT_TIMESTAMP() as db_completion_time;

-- ============================================================================
-- 実行完了メッセージ
-- ============================================================================

SELECT 
  'P010_processing_logが作成されました' as message,
  'バッチ処理ログ管理・エラー監視・運用分析基盤完成' as functionality,
  'DB初期構築: 100%完了' as db_status,
  '🎉 Signal Axis Trader 完全稼働準備完了！' as celebration,
  CURRENT_TIMESTAMP() as completion_time;