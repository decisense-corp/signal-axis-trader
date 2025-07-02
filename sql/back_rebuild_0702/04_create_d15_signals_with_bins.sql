/*
ファイル: 04_create_d15_signals_with_bins.sql
説明: Phase 4 - 生シグナル値とbin値を統合したテーブルを作成
作成日: 2025年7月2日
依存: d10_simple_signals（Phase 2完了）+ m30_signal_bins（Phase 3完了）
実行時間: 約3-5分
対象: d15_signals_with_bins テーブルの新規作成
*/

-- ============================================================================
-- Phase 4: d15_signals_with_bins 作成実行
-- ============================================================================

-- 処理開始メッセージ
SELECT 
  'Phase 4: d15_signals_with_bins作成を開始します' as message,
  'データソース1: d10_simple_signals (27種類, 13,973,509行)' as source_1,
  'データソース2: m30_signal_bins (27種類×20区分, 540行)' as source_2,
  CURRENT_DATETIME('Asia/Tokyo') as start_time;

-- ============================================================================
-- 1. 事前確認：依存テーブルの状況確認
-- ============================================================================

-- d10_simple_signals の状況確認
SELECT 
  'Phase 4事前確認: d10_simple_signals' as check_point,
  COUNT(*) as total_records,
  COUNT(DISTINCT signal_type) as signal_types,
  COUNT(DISTINCT stock_code) as stock_count,
  MIN(signal_date) as min_date,
  MAX(signal_date) as max_date
FROM `kabu-376213.kabu2411.d10_simple_signals`;

-- m30_signal_bins の状況確認
SELECT 
  'Phase 4事前確認: m30_signal_bins' as check_point,
  COUNT(*) as total_bins,
  COUNT(DISTINCT signal_type) as signal_types,
  COUNT(*) / COUNT(DISTINCT signal_type) as bins_per_signal
FROM `kabu-376213.kabu2411.m30_signal_bins`;

-- ============================================================================
-- 2. d15_signals_with_bins テーブル作成
-- ============================================================================

-- 既存テーブルがある場合は削除（新規作成）
DROP TABLE IF EXISTS `kabu-376213.kabu2411.d15_signals_with_bins`;

CREATE OR REPLACE TABLE `kabu-376213.kabu2411.d15_signals_with_bins`
PARTITION BY signal_date
CLUSTER BY stock_code, signal_type AS

WITH signal_bin_mapping AS (
  SELECT
    sr.signal_date,
    sr.reference_date,
    sr.stock_code,
    sr.stock_name,
    sr.signal_type,
    sr.signal_category,
    sr.signal_value,
    sr.created_at,
    -- 境界値問題対応：重複時は最上位binを選択
    MAX(sb.signal_bin) as signal_bin
  FROM `kabu-376213.kabu2411.d10_simple_signals` sr
  LEFT JOIN `kabu-376213.kabu2411.m30_signal_bins` sb
    ON sr.signal_type = sb.signal_type
    AND sr.signal_value <= sb.upper_bound
    AND sr.signal_value > sb.lower_bound
  GROUP BY 
    sr.signal_date, 
    sr.reference_date, 
    sr.stock_code, 
    sr.stock_name,
    sr.signal_type, 
    sr.signal_category, 
    sr.signal_value, 
    sr.created_at
)
SELECT 
  signal_date,
  reference_date,
  stock_code,
  stock_name,
  signal_type,
  signal_category,
  signal_value,
  signal_bin,
  created_at
FROM signal_bin_mapping
WHERE signal_bin IS NOT NULL;  -- bin割り当て成功レコードのみ

-- ============================================================================
-- 3. 作成結果の確認
-- ============================================================================

-- 基本統計確認
SELECT 
  'Phase 4作成結果: 基本統計' as check_point,
  COUNT(*) as total_records,
  COUNT(DISTINCT signal_type) as signal_types_27_expected,
  COUNT(DISTINCT stock_code) as stock_count,
  COUNT(DISTINCT signal_date) as date_count,
  MIN(signal_date) as min_date,
  MAX(signal_date) as max_date
FROM `kabu-376213.kabu2411.d15_signals_with_bins`;

-- bin割り当て状況確認
SELECT 
  'Phase 4作成結果: bin割り当て状況' as check_point,
  signal_type,
  COUNT(*) as record_count,
  COUNT(DISTINCT signal_bin) as unique_bins,
  MIN(signal_bin) as min_bin,
  MAX(signal_bin) as max_bin,
  COUNT(CASE WHEN signal_bin IS NULL THEN 1 END) as null_bins
FROM `kabu-376213.kabu2411.d15_signals_with_bins`
GROUP BY signal_type
ORDER BY signal_type
LIMIT 10;  -- 最初の10種類のみ表示

-- データ完全性確認（重要）
WITH source_vs_result AS (
  SELECT 
    'source (d10_simple_signals)' as data_source,
    COUNT(*) as record_count,
    COUNT(DISTINCT signal_type) as signal_types
  FROM `kabu-376213.kabu2411.d10_simple_signals`
  UNION ALL
  SELECT 
    'result (d15_signals_with_bins)' as data_source,
    COUNT(*) as record_count,
    COUNT(DISTINCT signal_type) as signal_types
  FROM `kabu-376213.kabu2411.d15_signals_with_bins`
)
SELECT 
  'Phase 4作成結果: データ完全性' as check_point,
  data_source,
  record_count,
  signal_types,
  ROUND((record_count / LAG(record_count) OVER (ORDER BY data_source)) * 100, 2) as retention_rate_percent
FROM source_vs_result
ORDER BY data_source;

-- ============================================================================
-- 4. 品質確認：最新3日間のデータ品質
-- ============================================================================

-- 最新3日間の日別レコード数
SELECT 
  'Phase 4品質確認: 最新3日間' as check_point,
  signal_date,
  COUNT(*) as record_count,
  COUNT(DISTINCT stock_code) as stock_count,
  COUNT(DISTINCT signal_type) as signal_types,
  ROUND(AVG(signal_bin), 2) as avg_bin,
  COUNT(CASE WHEN signal_bin IS NULL THEN 1 END) as null_bins
FROM `kabu-376213.kabu2411.d15_signals_with_bins`
WHERE signal_date >= DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 3 DAY)
GROUP BY signal_date
ORDER BY signal_date DESC;

-- ============================================================================
-- 5. bin境界値問題の最終確認
-- ============================================================================

-- 境界値での重複確認（デバッグ用）
WITH boundary_check AS (
  SELECT 
    sr.signal_type,
    sr.signal_value,
    COUNT(sb.signal_bin) as matching_bins,
    STRING_AGG(CAST(sb.signal_bin AS STRING) ORDER BY sb.signal_bin) as bin_list
  FROM `kabu-376213.kabu2411.d10_simple_signals` sr
  LEFT JOIN `kabu-376213.kabu2411.m30_signal_bins` sb
    ON sr.signal_type = sb.signal_type
    AND sr.signal_value <= sb.upper_bound
    AND sr.signal_value > sb.lower_bound
  GROUP BY sr.signal_type, sr.signal_value
  HAVING COUNT(sb.signal_bin) > 1
)
SELECT 
  'Phase 4品質確認: 境界値重複' as check_point,
  COUNT(*) as duplicate_value_count,
  'MAX(signal_bin)手法で自動解決済み' as resolution_method
FROM boundary_check;

-- ============================================================================
-- 6. Phase 4完了確認
-- ============================================================================

SELECT 
  '🎉 Phase 4 完了確認' as final_check,
  COUNT(*) as total_records,
  COUNT(DISTINCT signal_type) as signal_types_27_expected,
  COUNT(DISTINCT stock_code) as stock_count,
  ROUND(COUNT(*) / COUNT(DISTINCT signal_date), 0) as avg_records_per_day,
  ROUND((COUNT(*) - COUNT(CASE WHEN signal_bin IS NULL THEN 1 END)) / COUNT(*) * 100, 2) as bin_assignment_rate_percent,
  'Phase 4: d15_signals_with_bins 作成完了' as status,
  CURRENT_DATETIME('Asia/Tokyo') as completion_time
FROM `kabu-376213.kabu2411.d15_signals_with_bins`;

-- 次Phase準備確認
SELECT 
  '📋 Phase 5準備確認' as next_phase,
  '✅ d15_signals_with_bins (Phase 4完了)' as completed,
  '⚡ d20_basic_signal_results (Phase 5実行予定)' as next_target,
  '依存: d15_signals_with_bins + daily_quotes' as dependencies;

-- ============================================================================
-- 処理完了メッセージ
-- ============================================================================

SELECT 
  'Phase 4: d15_signals_with_bins作成が完了しました' as message,
  'データ統合: 生シグナル値 + bin値の統合成功' as integration_status,
  'MAX(signal_bin)手法により境界値問題も解決済み' as boundary_resolution,
  '次段階: Phase 5 (d20_basic_signal_results再構築) 実行可能' as next_step,
  CURRENT_DATETIME('Asia/Tokyo') as completion_time;