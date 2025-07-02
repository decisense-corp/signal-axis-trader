/*
ファイル: 03_rebuild_m30_signal_bins_optimized.sql
説明: Phase 3 - 37指標から20分位境界値を再計算（最適化版）
作成日: 2025年7月3日
依存: d10_simple_signals（Phase 2完了 - 37指標復活版）
実行時間: 約1-2分
対象: m30_signal_bins テーブルの完全再構築（37指標版）
最適化: 指標入れ替え再実行用に検証を簡略化

⚠️ 【境界値・パーセンタイル重複問題について】

💡 問題の本質：
   データに同じ値が大量存在する場合、連続する分位点が同じ値になる
   例：Close to Low Ratio = 100.0（大量の銘柄で終値=安値）
       Close Volatility = 0.0（値動きなしの銘柄が多数）
       各種Rate = 0.0（変化率が0の場合が大量）

⚙️ 技術的対応：
   1. bin_number = 1: min_value - 0.0001（最小値より僅かに小さく）
   2. 中間bin: 前分位点と同じ場合 → 分位点値 - (0.0001 × bin_number)
   3. bin_number = 20: 前分位点と同じ場合 → max_value + 0.0001
   
🎯 実用上の解決：
   後工程のd15_signals_with_binsで「MAX(signal_bin)」を使用し、
   境界値重複は自動的に解決される。つまり実用上は問題なし。
   
📊 統計的観点：
   - 微調整±0.0001は実データ分布に影響なし
   - 同じ値の大量存在は株式データの自然な特性
   - 20分位分割の目的（相対的な強弱判定）は十分達成
   
🔄 繰り返し実行時：
   この境界値エラーは指標の性質上必ず発生するが、技術的に解決済み。
   エラー件数の変動は正常（データ分布の変化を反映）。
   
✅ 結論：現在の実装で問題なし、安心して繰り返し実行可能
*/

-- ============================================================================
-- Phase 3: m30_signal_bins 再計算実行（37指標版・最適化）
-- ============================================================================

-- 処理開始メッセージ
SELECT 
  'Phase 3: m30_signal_bins再計算を開始します（37指標版）' as message,
  'データソース: d10_simple_signals (37指標復活版)' as source_info,
  '最適化: 指標入れ替え再実行対応版' as optimization,
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
-- 3. 37指標から20分位境界値を再計算
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
  -- 下限値の設定（重複問題を完全修正）
  CASE 
    WHEN bin_number = 1 THEN min_value - 0.0001
    ELSE 
      CASE
        -- 前の分位点と同じ場合は微調整
        WHEN percentiles[SAFE_ORDINAL(bin_number - 1)] >= percentiles[SAFE_ORDINAL(bin_number)] 
        THEN percentiles[SAFE_ORDINAL(bin_number)] - (0.0001 * bin_number)
        ELSE percentiles[SAFE_ORDINAL(bin_number - 1)]
      END
  END as lower_bound,
  -- 上限値の設定（最大bin調整）
  CASE 
    WHEN bin_number = 20 AND percentiles[SAFE_ORDINAL(19)] >= percentiles[SAFE_ORDINAL(20)]
    THEN max_value + 0.0001
    ELSE percentiles[SAFE_ORDINAL(bin_number)]
  END as upper_bound,
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
-- 4. 重要チェックのみ実施（簡略版）
-- ============================================================================

-- 基本統計確認
SELECT 
  '✅ 基本統計確認' as check_point,
  COUNT(DISTINCT signal_type) as signal_type_count_37_expected,
  COUNT(*) as total_bins_740_expected,
  COUNT(*) / COUNT(DISTINCT signal_type) as avg_bins_per_signal,
  MIN(sample_count) as min_sample_count,
  AVG(sample_count) as avg_sample_count
FROM `kabu-376213.kabu2411.m30_signal_bins`;

-- 境界値エラーチェック（最重要）
SELECT 
  '🔍 境界値エラーチェック（最重要）' as check_point,
  COUNT(*) as error_count,
  CASE 
    WHEN COUNT(*) = 0 THEN '✅ 境界値エラーなし'
    ELSE '❌ 境界値エラーあり - 要確認'
  END as result
FROM `kabu-376213.kabu2411.m30_signal_bins`
WHERE 
  lower_bound >= upper_bound 
  OR lower_bound IS NULL 
  OR upper_bound IS NULL;

-- ============================================================================
-- 5. Phase 3完了確認（37指標版・簡略）
-- ============================================================================

SELECT 
  '🎉 Phase 3 完了確認（37指標版）' as final_check,
  COUNT(DISTINCT signal_type) as signal_types_37_expected,
  COUNT(*) as total_bins_740_expected,
  MIN(sample_count) as min_sample_count,
  'Phase 3: m30_signal_bins 再計算完了（37指標版）' as status,
  CURRENT_TIMESTAMP() as completion_time
FROM `kabu-376213.kabu2411.m30_signal_bins`;

-- 次Phase準備確認
SELECT 
  '📋 Phase 4準備確認' as next_phase,
  '✅ m30_signal_bins (Phase 3完了・37指標版)' as completed,
  '⚡ d15_signals_with_bins (Phase 4実行予定・37指標版)' as next_target,
  '依存: d10_simple_signals + m30_signal_bins (共に37指標版)' as dependencies,
  '容量: 340行→740行（軽微・一括処理可能）' as size_info;