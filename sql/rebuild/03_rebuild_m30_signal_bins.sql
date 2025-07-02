/*
ファイル: 03_rebuild_m30_signal_bins_17.sql
説明: Phase 3 - 17指標（新指標10 + 比較用7）から20分位境界値を再計算
作成日: 2025年7月3日
依存: d10_simple_signals（Phase 2完了 - 17指標版）
実行時間: 約1-2分
対象: m30_signal_bins テーブルの完全再構築（17指標版）
背景: Phase 7で確認された技術分析の限界を突破するための独自指標検証
*/

-- ============================================================================
-- Phase 3: m30_signal_bins 再計算実行（17指標版）
-- ============================================================================

-- 処理開始メッセージ
SELECT 
  'Phase 3: m30_signal_bins再計算を開始します（17指標版）' as message,
  'データソース: d10_simple_signals (17指標, 858万件)' as source_info,
  '目的: 新指標による独自性確保・市場効率化回避' as purpose,
  CURRENT_TIMESTAMP() as start_time;

-- ============================================================================
-- 1. 既存データのバックアップ（安全性確保）
-- ============================================================================

-- バックアップテーブル作成（日付付き）
CREATE OR REPLACE TABLE `kabu-376213.kabu2411.m30_signal_bins_backup_20250703` AS
SELECT *, CURRENT_TIMESTAMP() as backup_timestamp
FROM `kabu-376213.kabu2411.m30_signal_bins`;

SELECT 
  'バックアップ完了' as status,
  COUNT(*) as backup_record_count,
  COUNT(DISTINCT signal_type) as old_signal_types
FROM `kabu-376213.kabu2411.m30_signal_bins_backup_20250703`;

-- ============================================================================
-- 2. 既存データをクリア
-- ============================================================================

TRUNCATE TABLE `kabu-376213.kabu2411.m30_signal_bins`;

-- ============================================================================
-- 3. 17指標から20分位境界値を再計算
-- ============================================================================

INSERT INTO `kabu-376213.kabu2411.m30_signal_bins`
WITH signal_percentiles AS (
  -- 各シグナルタイプの20分位点を計算
  SELECT
    signal_type,
    APPROX_QUANTILES(signal_value, 20) AS percentiles,
    COUNT(*) as sample_count,
    AVG(signal_value) as mean_value,
    APPROX_QUANTILES(signal_value, 2)[OFFSET(1)] as median_value,
    STDDEV(signal_value) as std_value,
    MIN(signal_value) as min_value,
    MAX(signal_value) as max_value
  FROM
    `kabu-376213.kabu2411.d10_simple_signals`
  WHERE
    signal_value IS NOT NULL
    AND ABS(signal_value) < 10000  -- 基本的な異常値除外
  GROUP BY
    signal_type
  HAVING
    COUNT(*) >= 1000  -- 最低サンプル数確保
),
expanded_bins AS (
  SELECT
    signal_type,
    bin_number,
    percentiles,
    sample_count,
    mean_value,
    median_value,
    std_value,
    min_value,
    max_value
  FROM
    signal_percentiles,
    UNNEST(GENERATE_ARRAY(1, 20)) AS bin_number
)
SELECT
  signal_type,
  bin_number as signal_bin,
  -- 下限値の設定
  CASE 
    WHEN bin_number = 1 THEN min_value
    ELSE percentiles[SAFE_ORDINAL(bin_number - 1)]
  END as lower_bound,
  -- 上限値の設定
  percentiles[SAFE_ORDINAL(bin_number)] as upper_bound,
  -- パーセンタイルランク
  bin_number * 5.0 as percentile_rank,
  sample_count,
  ROUND(mean_value, 6) as mean_value,
  ROUND(median_value, 6) as median_value,
  ROUND(std_value, 6) as std_value,
  CURRENT_DATE() as calculation_date,
  CURRENT_TIMESTAMP() as created_at
FROM
  expanded_bins
WHERE
  percentiles[SAFE_ORDINAL(bin_number)] IS NOT NULL
ORDER BY
  signal_type, signal_bin;

-- ============================================================================
-- 4. データ品質確認・検証
-- ============================================================================

-- 基本統計確認
SELECT 
  '4-1 基本統計確認' as check_point,
  COUNT(DISTINCT signal_type) as signal_type_count,
  COUNT(*) as total_bins,
  COUNT(*) / COUNT(DISTINCT signal_type) as avg_bins_per_signal,
  MIN(sample_count) as min_sample_count,
  MAX(sample_count) as max_sample_count,
  AVG(sample_count) as avg_sample_count
FROM `kabu-376213.kabu2411.m30_signal_bins`;

-- 新指標vs既存指標の境界値構成確認
SELECT 
  '4-2 新指標vs既存指標構成' as check_point,
  CASE 
    WHEN signal_type LIKE '%High_Price_Score%' OR signal_type LIKE '%Low_Price_Score%' THEN '新指標'
    ELSE '比較用既存指標'
  END as indicator_group,
  COUNT(DISTINCT signal_type) as signal_count,
  COUNT(*) as total_bins,
  ROUND(AVG(sample_count), 0) as avg_sample_count
FROM `kabu-376213.kabu2411.m30_signal_bins`
GROUP BY 
  CASE 
    WHEN signal_type LIKE '%High_Price_Score%' OR signal_type LIKE '%Low_Price_Score%' THEN '新指標'
    ELSE '比較用既存指標'
  END
ORDER BY signal_count DESC;

-- シグナルタイプ別確認
SELECT 
  '4-3 シグナルタイプ別確認' as check_point,
  signal_type,
  COUNT(*) as bin_count,
  MIN(signal_bin) as min_bin,
  MAX(signal_bin) as max_bin,
  ROUND(sample_count, 0) as sample_count,
  ROUND(mean_value, 4) as mean_val,
  ROUND(std_value, 4) as std_val
FROM `kabu-376213.kabu2411.m30_signal_bins`
GROUP BY signal_type, sample_count, mean_value, std_value
ORDER BY signal_type;

-- 境界値の論理チェック
SELECT 
  '4-4 境界値論理チェック' as check_point,
  COUNT(*) as error_count,
  CASE 
    WHEN COUNT(*) = 0 THEN '✅ 境界値エラーなし'
    ELSE '❌ 境界値エラーあり'
  END as result
FROM `kabu-376213.kabu2411.m30_signal_bins`
WHERE 
  lower_bound >= upper_bound 
  OR lower_bound IS NULL 
  OR upper_bound IS NULL;

-- サンプル境界値表示（新指標重点確認）
SELECT 
  '4-5 新指標サンプル境界値' as check_point,
  signal_type,
  signal_bin,
  ROUND(lower_bound, 4) as lower_bound,
  ROUND(upper_bound, 4) as upper_bound,
  percentile_rank,
  sample_count
FROM `kabu-376213.kabu2411.m30_signal_bins`
WHERE signal_type LIKE '%High_Price_Score%' OR signal_type LIKE '%Low_Price_Score%'
  AND signal_bin IN (5, 10, 15, 20)  -- 代表的な分位点のみ表示
ORDER BY signal_type, signal_bin
LIMIT 20;

-- ============================================================================
-- 5. 旧データとの比較（27種類→17指標への変化）
-- ============================================================================

-- 種類数の比較
SELECT 
  '5-1 シグナル種類数比較' as comparison_point,
  'バックアップ(旧)' as data_source,
  COUNT(DISTINCT signal_type) as signal_types
FROM `kabu-376213.kabu2411.m30_signal_bins_backup_20250703`
UNION ALL
SELECT 
  '5-1 シグナル種類数比較' as comparison_point,
  '新規作成(17指標版)' as data_source,
  COUNT(DISTINCT signal_type) as signal_types
FROM `kabu-376213.kabu2411.m30_signal_bins`
ORDER BY data_source;

-- 共通シグナルの境界値変化確認（残存した比較用指標のみ）
SELECT 
  '5-2 共通シグナル境界値変化' as comparison_point,
  curr.signal_type,
  curr.signal_bin,
  ROUND(prev.upper_bound, 4) as old_upper_bound,
  ROUND(curr.upper_bound, 4) as new_upper_bound,
  ROUND(curr.upper_bound - prev.upper_bound, 4) as diff,
  ROUND(prev.sample_count, 0) as old_sample_count,
  ROUND(curr.sample_count, 0) as new_sample_count
FROM `kabu-376213.kabu2411.m30_signal_bins` curr
LEFT JOIN `kabu-376213.kabu2411.m30_signal_bins_backup_20250703` prev
  ON curr.signal_type = prev.signal_type 
  AND curr.signal_bin = prev.signal_bin
WHERE curr.signal_bin IN (5, 10, 15, 20)  -- 代表的な分位点のみ表示
  AND prev.signal_type IS NOT NULL  -- 共通指標のみ
ORDER BY curr.signal_type, curr.signal_bin
LIMIT 20;

-- ============================================================================
-- 6. Phase 3完了確認（17指標版）
-- ============================================================================

SELECT 
  '🎉 Phase 3 完了確認（17指標版）' as final_check,
  COUNT(DISTINCT signal_type) as signal_types_17_expected,
  COUNT(*) as total_bins_340_expected,
  MIN(sample_count) as min_sample_count,
  AVG(sample_count) as avg_sample_count,
  'Phase 3: m30_signal_bins 再計算完了（17指標版）' as status,
  CURRENT_TIMESTAMP() as completion_time
FROM `kabu-376213.kabu2411.m30_signal_bins`;

-- 独自指標戦略の準備完了確認
SELECT 
  '🚀 独自指標戦略準備完了' as strategy_check,
  COUNT(CASE WHEN signal_type LIKE '%High_Price_Score%' OR signal_type LIKE '%Low_Price_Score%' THEN 1 END) as new_indicators_count,
  COUNT(CASE WHEN NOT (signal_type LIKE '%High_Price_Score%' OR signal_type LIKE '%Low_Price_Score%') THEN 1 END) as existing_indicators_count,
  '新指標による市場効率化回避戦略' as purpose,
  'Phase 7劣化15-17%の改善を期待' as target_improvement
FROM `kabu-376213.kabu2411.m30_signal_bins`;

-- 次Phase準備確認
SELECT 
  '📋 Phase 4準備確認' as next_phase,
  '✅ m30_signal_bins (Phase 3完了・17指標版)' as completed,
  '⚡ d15_signals_with_bins (Phase 4実行予定・17指標版)' as next_target,
  '依存: d10_simple_signals + m30_signal_bins (共に17指標版)' as dependencies;