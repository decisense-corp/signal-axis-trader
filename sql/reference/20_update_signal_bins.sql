/*
ファイル: 20_update_signal_bins.sql
説明: シグナル区分（20分位）を最新データで再計算（週次実行）
作成日: 2025-01-01
修正日: 2025-06-08 - 初期クエリのシンプルさを参考に超最適化
実行時間: 約30秒-1分
*/

-- ============================================================================
-- 1. 更新対象期間の設定（直近90日のデータを使用）
-- ============================================================================
DECLARE calculation_start_date DATE DEFAULT DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 90 DAY);
DECLARE calculation_end_date DATE DEFAULT CURRENT_DATE('Asia/Tokyo');

-- 処理開始メッセージ
SELECT 
  'シグナル区分の更新を開始します' as message,
  calculation_start_date as start_date,
  calculation_end_date as end_date;

-- ============================================================================
-- 2. 既存の区分データをバックアップ（履歴保存）
-- ============================================================================
-- 履歴テーブルが存在しない場合は作成
CREATE TABLE IF NOT EXISTS `kabu-376213.kabu2411.m02_signal_bins_history` (
  signal_type STRING,
  signal_bin INT64,
  lower_bound FLOAT64,
  upper_bound FLOAT64,
  percentile_rank FLOAT64,
  sample_count INT64,
  mean_value FLOAT64,
  median_value FLOAT64,
  std_value FLOAT64,
  calculation_date DATE,
  created_at TIMESTAMP,
  backup_date DATE
);

-- 現在の区分をバックアップ
INSERT INTO `kabu-376213.kabu2411.m02_signal_bins_history`
SELECT 
  *,
  CURRENT_DATE() as backup_date
FROM 
  `kabu-376213.kabu2411.m02_signal_bins`;

-- ============================================================================
-- 3. 新しい区分の計算（超シンプル版）
-- ============================================================================
-- 既存データを削除
TRUNCATE TABLE `kabu-376213.kabu2411.m02_signal_bins`;

-- 新しいデータを直接挿入
INSERT INTO `kabu-376213.kabu2411.m02_signal_bins`
WITH signal_percentiles AS (
  -- 各シグナルタイプの20分位点を計算
  SELECT
    signal_type,
    APPROX_QUANTILES(signal_value, 20) AS percentiles,
    COUNT(*) as sample_count,
    AVG(signal_value) as mean_value,
    APPROX_QUANTILES(signal_value, 2)[OFFSET(1)] as median_value,
    STDDEV(signal_value) as std_value
  FROM
    `kabu-376213.kabu2411.d01_signals_raw`
  WHERE
    signal_date >= calculation_start_date
    AND signal_date <= calculation_end_date
    AND signal_value IS NOT NULL
    AND ABS(signal_value) < 10000  -- 基本的な異常値除外
  GROUP BY
    signal_type
)
SELECT
  signal_type,
  bin_number as signal_bin,
  LAG(percentiles[SAFE_ORDINAL(bin_number)], 1, -999999) OVER (PARTITION BY signal_type ORDER BY bin_number) as lower_bound,
  percentiles[SAFE_ORDINAL(bin_number)] as upper_bound,
  bin_number * 5 as percentile_rank,
  sample_count,
  ROUND(mean_value, 4) as mean_value,
  ROUND(median_value, 4) as median_value,
  ROUND(std_value, 4) as std_value,
  CURRENT_DATE() as calculation_date,
  CURRENT_TIMESTAMP() as created_at
FROM
  signal_percentiles,
  UNNEST(GENERATE_ARRAY(1, 20)) AS bin_number
WHERE
  percentiles[SAFE_ORDINAL(bin_number)] IS NOT NULL
ORDER BY
  signal_type, signal_bin;

-- ============================================================================
-- 4. 更新結果の確認（シンプル版）
-- ============================================================================
SELECT
  'シグナル区分の更新が完了しました' as message,
  COUNT(DISTINCT signal_type) as signal_types,
  COUNT(*) as total_bins,
  AVG(sample_count) as avg_samples,
  MIN(sample_count) as min_samples,
  CURRENT_TIMESTAMP() as completed_at
FROM
  `kabu-376213.kabu2411.m02_signal_bins`;