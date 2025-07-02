/*
ファイル: 04_create_d15_signals_with_bins_37.sql
説明: Phase 4 - 生シグナル値とbin値を統合したテーブルを作成（37指標版）
作成日: 2025年7月3日
依存: d10_simple_signals（Phase 2完了・37指標版）+ m30_signal_bins（Phase 3完了・37指標版）
実行時間: 約3-5分
対象: d15_signals_with_bins テーブルの新規作成（37指標版）
背景: 指標入れ替え検証の繰り返し実行に対応、確認を必要最低限に調整
*/

-- ============================================================================
-- Phase 4: d15_signals_with_bins 作成実行（37指標版）
-- ============================================================================

-- 処理開始メッセージ
SELECT 
  'Phase 4: d15_signals_with_bins作成を開始します（37指標版）' as message,
  'データソース1: d10_simple_signals (37指標)' as source_1,
  'データソース2: m30_signal_bins (37指標×20区分, 740件)' as source_2,
  CURRENT_DATETIME() as start_time;

-- ============================================================================
-- 1. 事前確認：依存テーブルの状況確認（必要最低限）
-- ============================================================================

-- 依存テーブル基本確認
SELECT 
  'Phase 4事前確認' as check_point,
  (SELECT COUNT(DISTINCT signal_type) FROM `kabu-376213.kabu2411.d10_simple_signals`) as d10_signal_types,
  (SELECT COUNT(DISTINCT signal_type) FROM `kabu-376213.kabu2411.m30_signal_bins`) as m30_signal_types,
  (SELECT COUNT(*) FROM `kabu-376213.kabu2411.m30_signal_bins`) as m30_total_bins,
  '37指標 × 20区分 = 740bins期待' as expected;

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
-- 3. 作成結果の確認（必要最低限）
-- ============================================================================

-- 基本統計確認
SELECT 
  'Phase 4作成結果' as check_point,
  COUNT(*) as total_records,
  COUNT(DISTINCT signal_type) as signal_types_37_expected,
  COUNT(DISTINCT stock_code) as stock_count,
  MIN(signal_date) as min_date,
  MAX(signal_date) as max_date,
  ROUND((COUNT(*) - COUNT(CASE WHEN signal_bin IS NULL THEN 1 END)) / COUNT(*) * 100, 1) as bin_assignment_rate_percent
FROM `kabu-376213.kabu2411.d15_signals_with_bins`;

-- データ完全性確認（重要）
WITH source_vs_result AS (
  SELECT 
    'source' as data_source,
    COUNT(*) as record_count,
    COUNT(DISTINCT signal_type) as signal_types
  FROM `kabu-376213.kabu2411.d10_simple_signals`
  UNION ALL
  SELECT 
    'result' as data_source,
    COUNT(*) as record_count,
    COUNT(DISTINCT signal_type) as signal_types
  FROM `kabu-376213.kabu2411.d15_signals_with_bins`
)
SELECT 
  'Phase 4データ完全性' as check_point,
  data_source,
  record_count,
  signal_types,
  ROUND((record_count / LAG(record_count) OVER (ORDER BY data_source)) * 100, 1) as retention_rate_percent
FROM source_vs_result
ORDER BY data_source;

-- ============================================================================
-- 4. Phase 4完了確認（37指標版）
-- ============================================================================

SELECT 
  '🎉 Phase 4完了（37指標版）' as final_check,
  COUNT(*) as total_records,
  COUNT(DISTINCT signal_type) as signal_types_37_expected,
  ROUND(COUNT(*) / COUNT(DISTINCT signal_date), 0) as avg_records_per_day,
  ROUND((COUNT(*) - COUNT(CASE WHEN signal_bin IS NULL THEN 1 END)) / COUNT(*) * 100, 1) as bin_assignment_rate_percent,
  'Phase 4: d15_signals_with_bins 作成完了（37指標版）' as status,
  CURRENT_DATETIME() as completion_time
FROM `kabu-376213.kabu2411.d15_signals_with_bins`;

-- 次Phase準備確認
SELECT 
  '📋 Phase 5準備確認' as next_phase,
  '✅ d15_signals_with_bins (Phase 4完了・37指標版)' as completed,
  '⚡ d20_basic_signal_results (Phase 5実行予定・37指標版)' as next_target,
  '予想サイズ: 1,500MB → 6,000MB（大規模・期間分割必須）' as next_scale;

-- ============================================================================
-- 処理完了メッセージ
-- ============================================================================

SELECT 
  'Phase 4: d15_signals_with_bins作成が完了しました（37指標版）' as message,
  'MAX(signal_bin)手法により境界値問題も解決済み' as boundary_resolution,
  '次段階: Phase 5 (d20_basic_signal_results再構築・37指標版) 実行可能' as next_step,
  '指標入れ替え検証: エンドレス対応で確認項目を必要最低限に調整' as optimization,
  CURRENT_DATETIME() as completion_time;