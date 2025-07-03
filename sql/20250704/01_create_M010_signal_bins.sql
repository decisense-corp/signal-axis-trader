/*
ファイル: 01_create_M010_signal_bins.sql
説明: Signal Axis Trader 新設計書 - M010_signal_bins テーブル作成と既存データ移植
作成日: 2025年7月4日
目的: シグナル境界値マスタの作成（既存m30_signal_binsから移植）
実行時間: 約30秒
データ量: 740レコード（37指標 × 20分位）
*/

-- ============================================================================
-- Phase 1: M010_signal_bins テーブル作成と移植
-- ============================================================================

-- 処理開始メッセージ
SELECT 
  'Phase 1: M010_signal_bins 作成・移植を開始します' as message,
  '移植元: m30_signal_bins (37指標版)' as source_info,
  '新設計書準拠: M010_signal_bins' as target_info,
  CURRENT_TIMESTAMP() as start_time;

-- ============================================================================
-- 1. 既存データ確認（移植前チェック）
-- ============================================================================

-- 移植元テーブルの状況確認
SELECT 
  '移植前確認: m30_signal_bins' as check_point,
  COUNT(*) as total_records,
  COUNT(DISTINCT signal_type) as signal_types,
  MIN(signal_bin) as min_bin,
  MAX(signal_bin) as max_bin,
  MIN(sample_count) as min_sample_count,
  AVG(sample_count) as avg_sample_count
FROM `kabu-376213.kabu2411.m30_signal_bins`;

-- ============================================================================
-- 2. M010_signal_bins テーブル作成
-- ============================================================================

-- 既存テーブルがある場合は削除
DROP TABLE IF EXISTS `kabu-376213.kabu2411.M010_signal_bins`;

-- 新設計書準拠でテーブル作成
CREATE TABLE `kabu-376213.kabu2411.M010_signal_bins` (
  signal_type STRING NOT NULL,           -- 'High_Price_Score_7D'等
  signal_bin INT64 NOT NULL,             -- 1-20
  lower_bound FLOAT64,                   -- 下限値
  upper_bound FLOAT64,                   -- 上限値
  percentile_rank FLOAT64,               -- パーセンタイルランク
  sample_count INT64,                    -- サンプル数
  mean_value FLOAT64,                    -- 平均値
  std_value FLOAT64,                     -- 標準偏差
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY signal_type, signal_bin;

-- ============================================================================
-- 3. データ移植（既存テーブルから新テーブルへ）
-- ============================================================================

-- 既存データの移植実行
INSERT INTO `kabu-376213.kabu2411.M010_signal_bins`
SELECT 
  signal_type,
  signal_bin,
  lower_bound,
  upper_bound,
  percentile_rank,
  sample_count,
  mean_value,
  std_value,
  CURRENT_TIMESTAMP() as created_at  -- 移植時刻で更新
FROM `kabu-376213.kabu2411.m30_signal_bins`
ORDER BY signal_type, signal_bin;

-- ============================================================================
-- 4. 移植結果確認
-- ============================================================================

-- 基本移植確認
SELECT 
  '移植後確認: M010_signal_bins' as check_point,
  COUNT(*) as total_records,
  COUNT(DISTINCT signal_type) as signal_types,
  MIN(signal_bin) as min_bin,
  MAX(signal_bin) as max_bin,
  '740レコード期待（37指標×20分位）' as expected
FROM `kabu-376213.kabu2411.M010_signal_bins`;

-- データ整合性確認
SELECT 
  'データ整合性確認' as check_point,
  COUNT(CASE WHEN lower_bound IS NULL THEN 1 END) as null_lower_bound,
  COUNT(CASE WHEN upper_bound IS NULL THEN 1 END) as null_upper_bound,
  COUNT(CASE WHEN lower_bound >= upper_bound THEN 1 END) as invalid_bounds,
  COUNT(CASE WHEN sample_count <= 0 THEN 1 END) as invalid_sample_count
FROM `kabu-376213.kabu2411.M010_signal_bins`;

-- 指標別確認（サンプル）
SELECT 
  'サンプル確認: 指標別データ' as check_point,
  signal_type,
  COUNT(*) as bins_count,
  MIN(lower_bound) as min_lower,
  MAX(upper_bound) as max_upper,
  AVG(sample_count) as avg_samples
FROM `kabu-376213.kabu2411.M010_signal_bins`
GROUP BY signal_type
ORDER BY signal_type
LIMIT 10;

-- ============================================================================
-- 5. Phase 1完了確認
-- ============================================================================

SELECT 
  '🎉 Phase 1完了: M010_signal_bins作成・移植完了' as status,
  COUNT(*) as final_record_count,
  COUNT(DISTINCT signal_type) as signal_types_37,
  'シグナル境界値マスタ準備完了' as achievement,
  '次Phase: D010_basic_results作成準備' as next_step,
  CURRENT_TIMESTAMP() as completion_time
FROM `kabu-376213.kabu2411.M010_signal_bins`;

-- テーブル情報確認
SELECT 
  '📊 テーブル情報確認' as info_type,
  table_name,
  table_type,
  creation_time
FROM `kabu-376213.kabu2411.INFORMATION_SCHEMA.TABLES`
WHERE table_name = 'M010_signal_bins';

-- ============================================================================
-- 実行完了メッセージ
-- ============================================================================

SELECT 
  'M010_signal_bins テーブル作成・移植が完了しました' as message,
  '移植完了: 37指標×20分位=740レコード' as result,
  '設計書準拠: クラスタリング設定済み' as technical_setup,
  '準備完了: 次はD010_basic_results作成へ' as next_action,
  '🚀 Signal Axis Trader 新システム基盤構築開始！' as celebration,
  CURRENT_TIMESTAMP() as completion_time;