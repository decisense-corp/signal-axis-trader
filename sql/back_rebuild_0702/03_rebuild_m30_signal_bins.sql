/*
ファイル: 03_rebuild_m30_signal_bins.sql
説明: Phase 3 - 27種類シグナルから20分位境界値を再計算
作成日: 2025年7月2日
依存: d10_simple_signals（Phase 2完了）
実行時間: 約1-2分
対象: m30_signal_bins テーブルの完全再構築
*/

-- ============================================================================
-- Phase 3: m30_signal_bins 再計算実行
-- ============================================================================

-- 処理開始メッセージ
SELECT 
  'Phase 3: m30_signal_bins再計算を開始します' as message,
  'データソース: d10_simple_signals (27種類, 13,973,509行)' as source_info,
  CURRENT_TIMESTAMP('Asia/Tokyo') as start_time;

-- ============================================================================
-- 1. 既存データのバックアップ（安全性確保）
-- ============================================================================

-- バックアップテーブル作成
CREATE OR REPLACE TABLE `kabu-376213.kabu2411.m30_signal_bins_backup_20250702` AS
SELECT *, CURRENT_TIMESTAMP() as backup_timestamp
FROM `kabu-376213.kabu2411.m30_signal_bins`;

SELECT 
  'バックアップ完了' as status,
  COUNT(*) as backup_record_count
FROM `kabu-376213.kabu2411.m30_signal_bins_backup_20250702`;

-- ============================================================================
-- 2. 既存データをクリア
-- ============================================================================

TRUNCATE TABLE `kabu-376213.kabu2411.m30_signal_bins`;

-- ============================================================================
-- 3. 27種類シグナルから20分位境界値を再計算
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
  CURRENT_DATE('Asia/Tokyo') as calculation_date,
  CURRENT_TIMESTAMP('Asia/Tokyo') as created_at
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
  '4-1. 基本統計確認' as check_point,
  COUNT(DISTINCT signal_type) as signal_type_count,
  COUNT(*) as total_bins,
  COUNT(*) / COUNT(DISTINCT signal_type) as avg_bins_per_signal,
  MIN(sample_count) as min_sample_count,
  MAX(sample_count) as max_sample_count,
  AVG(sample_count) as avg_sample_count
FROM `kabu-376213.kabu2411.m30_signal_bins`;

-- シグナルタイプ別確認
SELECT 
  '4-2. シグナルタイプ別確認' as check_point,
  signal_type,
  COUNT(*) as bin_count,
  MIN(signal_bin) as min_bin,
  MAX(signal_bin) as max_bin,
  sample_count,
  ROUND(mean_value, 4) as mean_val,
  ROUND(std_value, 4) as std_val
FROM `kabu-376213.kabu2411.m30_signal_bins`
GROUP BY signal_type, sample_count, mean_value, std_value
ORDER BY signal_type;

-- 境界値の論理チェック
SELECT 
  '4-3. 境界値論理チェック' as check_point,
  signal_type,
  signal_bin,
  lower_bound,
  upper_bound,
  upper_bound - lower_bound as range_width,
  CASE 
    WHEN lower_bound >= upper_bound THEN 'ERROR: 下限 >= 上限'
    WHEN lower_bound IS NULL OR upper_bound IS NULL THEN 'ERROR: NULL値'
    ELSE 'OK'
  END as validation_status
FROM `kabu-376213.kabu2411.m30_signal_bins`
WHERE 
  lower_bound >= upper_bound 
  OR lower_bound IS NULL 
  OR upper_bound IS NULL
ORDER BY signal_type, signal_bin;

-- エラーがない場合の確認
SELECT 
  '4-4. エラー件数確認' as check_point,
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

-- サンプル境界値表示（確認用）
SELECT 
  '4-5. サンプル境界値（最初の5シグナル）' as check_point,
  signal_type,
  signal_bin,
  ROUND(lower_bound, 4) as lower_bound,
  ROUND(upper_bound, 4) as upper_bound,
  percentile_rank,
  sample_count
FROM `kabu-376213.kabu2411.m30_signal_bins`
WHERE signal_type IN (
  SELECT signal_type 
  FROM `kabu-376213.kabu2411.m30_signal_bins` 
  GROUP BY signal_type 
  ORDER BY signal_type 
  LIMIT 5
)
ORDER BY signal_type, signal_bin;

-- ============================================================================
-- 5. 旧データとの比較（37種類→27種類）
-- ============================================================================

-- 種類数の比較
SELECT 
  '5-1. シグナル種類数比較' as comparison_point,
  'バックアップ(旧)' as data_source,
  COUNT(DISTINCT signal_type) as signal_types
FROM `kabu-376213.kabu2411.m30_signal_bins_backup_20250702`
UNION ALL
SELECT 
  '5-1. シグナル種類数比較' as comparison_point,
  '新規作成(新)' as data_source,
  COUNT(DISTINCT signal_type) as signal_types
FROM `kabu-376213.kabu2411.m30_signal_bins`
ORDER BY data_source;

-- 共通シグナルの境界値変化確認
SELECT 
  '5-2. 共通シグナル境界値変化' as comparison_point,
  new.signal_type,
  new.signal_bin,
  ROUND(old.upper_bound, 4) as old_upper_bound,
  ROUND(new.upper_bound, 4) as new_upper_bound,
  ROUND(new.upper_bound - old.upper_bound, 4) as diff,
  ROUND(old.sample_count, 0) as old_sample_count,
  ROUND(new.sample_count, 0) as new_sample_count
FROM `kabu-376213.kabu2411.m30_signal_bins` new
LEFT JOIN `kabu-376213.kabu2411.m30_signal_bins_backup_20250702` old
  ON new.signal_type = old.signal_type 
  AND new.signal_bin = old.signal_bin
WHERE new.signal_bin IN (5, 10, 15, 20)  -- 代表的な分位点のみ表示
  AND new.signal_type IN (
    SELECT signal_type 
    FROM `kabu-376213.kabu2411.m30_signal_bins` 
    GROUP BY signal_type 
    ORDER BY signal_type 
    LIMIT 3  -- 最初の3シグナルのみ
  )
ORDER BY new.signal_type, new.signal_bin;

-- ============================================================================
-- 6. Phase 3完了確認
-- ============================================================================

SELECT 
  '🎉 Phase 3 完了確認' as final_check,
  COUNT(DISTINCT signal_type) as signal_types_27_expected,
  COUNT(*) as total_bins_540_expected,
  MIN(sample_count) as min_sample_count,
  AVG(sample_count) as avg_sample_count,
  'Phase 3: m30_signal_bins 再計算完了' as status,
  CURRENT_TIMESTAMP('Asia/Tokyo') as completion_time
FROM `kabu-376213.kabu2411.m30_signal_bins`;

-- 次Phase準備確認
SELECT 
  '📋 Phase 4準備確認' as next_phase,
  '✅ m30_signal_bins (Phase 3完了)' as completed,
  '⚡ d15_signals_with_bins (Phase 4実行予定)' as next_target,
  '依存: d10_simple_signals + m30_signal_bins' as dependencies;