/*
ファイル: 04_initial_performance_aggregation_fixed_v2.sql
説明: シグナル値を20区分に分割し、4軸グループごとの実績を集計（相関サブクエリエラー修正版）
作成日: 2025-01-01
修正日: 2025-06-02 - 相関サブクエリをJOINに変更
実行時間: 約15-20分（BigQuery並列処理）
*/

-- ============================================================================
-- 1. シグナル区分（bins）の計算と保存
-- ============================================================================
-- 各シグナルタイプの値を20分位に分割
TRUNCATE TABLE `kabu-376213.kabu2411.m02_signal_bins`;

INSERT INTO `kabu-376213.kabu2411.m02_signal_bins`
WITH signal_percentiles AS (
  SELECT
    signal_type,
    -- 20分位点を計算（5%, 10%, ..., 95%, 100%）
    APPROX_QUANTILES(signal_value, 20) AS percentiles,
    COUNT(*) as sample_count,
    AVG(signal_value) as mean_value,
    APPROX_QUANTILES(signal_value, 2)[OFFSET(1)] as median_value,
    STDDEV(signal_value) as std_value
  FROM
    `kabu-376213.kabu2411.d01_signals_raw`
  WHERE
    signal_value IS NOT NULL
    AND ABS(signal_value) < 10000  -- 異常値除外
  GROUP BY
    signal_type
)
SELECT
  signal_type,
  bin_number as signal_bin,
  LAG(boundary, 1, -999999) OVER (PARTITION BY signal_type ORDER BY bin_number) as lower_bound,
  boundary as upper_bound,
  bin_number * 5 as percentile_rank,  -- 5%, 10%, ..., 100%
  sample_count,
  mean_value,
  median_value,
  std_value,
  CURRENT_DATE() as calculation_date,
  CURRENT_TIMESTAMP() as created_at
FROM
  signal_percentiles,
  UNNEST(percentiles) AS boundary WITH OFFSET AS bin_number
WHERE
  bin_number > 0  -- 0番目（最小値）は下限として使うので除外
ORDER BY
  signal_type, bin_number;

-- ============================================================================
-- 2. シグナルに区分番号を付与した一時テーブル
-- ============================================================================
CREATE TEMP TABLE signals_with_bins AS
WITH signal_bin_mapping AS (
  SELECT
    sr.signal_date,
    sr.reference_date,
    sr.stock_code,
    sr.stock_name,
    sr.signal_type,
    sr.signal_value,
    -- 該当する区分番号を特定
    MAX(sb.signal_bin) as signal_bin
  FROM
    `kabu-376213.kabu2411.d01_signals_raw` sr
  INNER JOIN
    `kabu-376213.kabu2411.m02_signal_bins` sb
  ON
    sr.signal_type = sb.signal_type
    AND sr.signal_value <= sb.upper_bound
    AND sr.signal_value > sb.lower_bound
  GROUP BY
    sr.signal_date,
    sr.reference_date,
    sr.stock_code,
    sr.stock_name,
    sr.signal_type,
    sr.signal_value
)
SELECT
  sbm.*,
  -- 翌日の結果データを結合
  q_next.Open as next_open,
  q_next.High as next_high,
  q_next.Low as next_low,
  q_next.Close as next_close,
  q_next.Volume as next_volume
FROM
  signal_bin_mapping sbm
LEFT JOIN
  `kabu-376213.kabu2411.daily_quotes` q_next
ON
  sbm.stock_code = REGEXP_REPLACE(q_next.Code, '0$', '')
  AND sbm.signal_date = q_next.Date
WHERE
  sbm.signal_bin IS NOT NULL;

-- ============================================================================
-- 3. 4軸グループごとの実績計算（Buy/Sell両方）
-- ============================================================================
TRUNCATE TABLE `kabu-376213.kabu2411.d02_signal_performance_4axis`;

INSERT INTO `kabu-376213.kabu2411.d02_signal_performance_4axis`
WITH performance_calculation AS (
  -- Buy（買い）の実績計算
  SELECT
    signal_type,
    signal_bin,
    'Buy' as trade_type,
    stock_code,
    stock_name,
    COUNT(*) as total_count,
    -- 寄り→引けの利益率
    AVG(SAFE_DIVIDE(next_close - next_open, next_open) * 100) as avg_profit_rate,
    STDDEV(SAFE_DIVIDE(next_close - next_open, next_open) * 100) as std_profit_rate,
    -- 勝ちトレード（利益率 > 0）
    COUNTIF(next_close > next_open) as win_count,
    -- 各種統計
    APPROX_QUANTILES(SAFE_DIVIDE(next_close - next_open, next_open) * 100, 2)[OFFSET(1)] as median_profit_rate,
    MAX(SAFE_DIVIDE(next_close - next_open, next_open) * 100) as max_profit_rate,
    MIN(SAFE_DIVIDE(next_close - next_open, next_open) * 100) as min_profit_rate,
    -- 期間情報
    MIN(signal_date) as first_signal_date,
    MAX(signal_date) as last_signal_date
  FROM
    signals_with_bins
  WHERE
    next_open > 0 AND next_close > 0
  GROUP BY
    signal_type, signal_bin, stock_code, stock_name
  
  UNION ALL
  
  -- Sell（売り）の実績計算
  SELECT
    signal_type,
    signal_bin,
    'Sell' as trade_type,
    stock_code,
    stock_name,
    COUNT(*) as total_count,
    -- 寄り→引けの利益率（売りなので逆）
    AVG(SAFE_DIVIDE(next_open - next_close, next_open) * 100) as avg_profit_rate,
    STDDEV(SAFE_DIVIDE(next_open - next_close, next_open) * 100) as std_profit_rate,
    -- 勝ちトレード（利益率 > 0）
    COUNTIF(next_open > next_close) as win_count,
    -- 各種統計
    APPROX_QUANTILES(SAFE_DIVIDE(next_open - next_close, next_open) * 100, 2)[OFFSET(1)] as median_profit_rate,
    MAX(SAFE_DIVIDE(next_open - next_close, next_open) * 100) as max_profit_rate,
    MIN(SAFE_DIVIDE(next_open - next_close, next_open) * 100) as min_profit_rate,
    -- 期間情報
    MIN(signal_date) as first_signal_date,
    MAX(signal_date) as last_signal_date
  FROM
    signals_with_bins
  WHERE
    next_open > 0 AND next_close > 0
  GROUP BY
    signal_type, signal_bin, stock_code, stock_name
),
signals_with_trade_type AS (
  -- signals_with_binsにtrade_typeを展開
  SELECT
    swb.*,
    trade_type
  FROM
    signals_with_bins swb
  CROSS JOIN
    UNNEST(['Buy', 'Sell']) AS trade_type
),
recent_performance AS (
  -- 直近30日と90日の実績を別途計算
  SELECT
    signal_type,
    signal_bin,
    trade_type,
    stock_code,
    -- 30日
    COUNTIF(signal_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)) as last_30d_count,
    SAFE_DIVIDE(
      COUNTIF(signal_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND 
              ((trade_type = 'Buy' AND next_close > next_open) OR 
               (trade_type = 'Sell' AND next_open > next_close))),
      NULLIF(COUNTIF(signal_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)), 0)
    ) * 100 as last_30d_win_rate,
    AVG(
      CASE 
        WHEN signal_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN
          CASE
            WHEN trade_type = 'Buy' THEN SAFE_DIVIDE(next_close - next_open, next_open) * 100
            WHEN trade_type = 'Sell' THEN SAFE_DIVIDE(next_open - next_close, next_open) * 100
          END
      END
    ) as last_30d_avg_profit,
    -- 90日
    COUNTIF(signal_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) as last_90d_count,
    SAFE_DIVIDE(
      COUNTIF(signal_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AND 
              ((trade_type = 'Buy' AND next_close > next_open) OR 
               (trade_type = 'Sell' AND next_open > next_close))),
      NULLIF(COUNTIF(signal_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)), 0)
    ) * 100 as last_90d_win_rate,
    AVG(
      CASE 
        WHEN signal_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) THEN
          CASE
            WHEN trade_type = 'Buy' THEN SAFE_DIVIDE(next_close - next_open, next_open) * 100
            WHEN trade_type = 'Sell' THEN SAFE_DIVIDE(next_open - next_close, next_open) * 100
          END
      END
    ) as last_90d_avg_profit
  FROM signals_with_trade_type
  WHERE
    next_open > 0 AND next_close > 0
  GROUP BY
    signal_type, signal_bin, trade_type, stock_code
)
-- 最終結果の結合
SELECT
  pc.signal_type,
  pc.signal_bin,
  pc.trade_type,
  pc.stock_code,
  pc.stock_name,
  pc.total_count,
  pc.win_count,
  ROUND(SAFE_DIVIDE(pc.win_count, pc.total_count) * 100, 2) as win_rate,
  ROUND(pc.avg_profit_rate, 4) as avg_profit_rate,
  ROUND(pc.median_profit_rate, 4) as median_profit_rate,
  ROUND(pc.std_profit_rate, 4) as std_profit_rate,
  ROUND(SAFE_DIVIDE(pc.avg_profit_rate, NULLIF(pc.std_profit_rate, 0)), 4) as sharpe_ratio,
  ROUND(pc.max_profit_rate, 4) as max_profit_rate,
  ROUND(pc.min_profit_rate, 4) as min_profit_rate,
  -- 直近実績
  COALESCE(rp.last_30d_count, 0) as last_30d_count,
  ROUND(COALESCE(rp.last_30d_win_rate, 0), 2) as last_30d_win_rate,
  ROUND(COALESCE(rp.last_30d_avg_profit, 0), 4) as last_30d_avg_profit,
  COALESCE(rp.last_90d_count, 0) as last_90d_count,
  ROUND(COALESCE(rp.last_90d_win_rate, 0), 2) as last_90d_win_rate,
  ROUND(COALESCE(rp.last_90d_avg_profit, 0), 4) as last_90d_avg_profit,
  -- メタデータ
  pc.first_signal_date,
  pc.last_signal_date,
  CURRENT_DATE() as last_updated,
  CURRENT_TIMESTAMP() as updated_at
FROM
  performance_calculation pc
LEFT JOIN
  recent_performance rp
ON
  pc.signal_type = rp.signal_type
  AND pc.signal_bin = rp.signal_bin
  AND pc.trade_type = rp.trade_type
  AND pc.stock_code = rp.stock_code
WHERE
  pc.total_count >= 5;  -- 最低5サンプル必要

-- ============================================================================
-- 4. 有効な4軸グループの特定
-- ============================================================================
TRUNCATE TABLE `kabu-376213.kabu2411.d03_effective_4axis_groups`;

INSERT INTO `kabu-376213.kabu2411.d03_effective_4axis_groups`
SELECT
  signal_type,
  signal_bin,
  trade_type,
  stock_code,
  -- 有効性判定（基準：勝率50%以上、平均利益率0.1%以上、サンプル数30以上）
  CASE
    WHEN total_count >= 30 
     AND win_rate >= 50 
     AND avg_profit_rate >= 0.1
     AND sharpe_ratio >= 0.1
    THEN true
    ELSE false
  END as is_effective,
  -- 理由
  CASE
    WHEN total_count < 30 THEN 'サンプル不足'
    WHEN win_rate < 50 THEN '勝率不足'
    WHEN avg_profit_rate < 0.1 THEN '利益率不足'
    WHEN sharpe_ratio < 0.1 THEN 'リスク調整後リターン不足'
    ELSE '基準クリア'
  END as effectiveness_reason,
  -- 統計サマリ
  total_count,
  win_rate,
  avg_profit_rate,
  sharpe_ratio,
  -- スコア計算
  -- 信頼性スコア（サンプル数ベース）
  LEAST(total_count / 100, 1.0) * 100 as reliability_score,
  -- 安定性スコア（シャープレシオベース）
  LEAST(GREATEST(sharpe_ratio, 0) / 1.0, 1.0) * 100 as stability_score,
  -- 直近スコア（30日実績ベース）
  CASE
    WHEN last_30d_count >= 10 THEN
      LEAST(GREATEST(last_30d_avg_profit, 0) / 1.0, 1.0) * 100
    ELSE 50  -- データ不足時は中立
  END as recency_score,
  -- 総合スコア
  (
    LEAST(total_count / 100, 1.0) * 0.2 +  -- 信頼性 20%
    LEAST(GREATEST(sharpe_ratio, 0) / 1.0, 1.0) * 0.3 +  -- 安定性 30%
    LEAST(GREATEST(avg_profit_rate, 0) / 1.0, 1.0) * 0.3 +  -- 収益性 30%
    CASE
      WHEN last_30d_count >= 10 THEN
        LEAST(GREATEST(last_30d_avg_profit, 0) / 1.0, 1.0) * 0.2  -- 直近実績 20%
      ELSE 0.1  -- データ不足時
    END
  ) * 100 as composite_score,
  CURRENT_DATE() as evaluation_date,
  CURRENT_TIMESTAMP() as updated_at
FROM
  `kabu-376213.kabu2411.d02_signal_performance_4axis`;

-- ============================================================================
-- 5. 処理結果の確認
-- ============================================================================
-- 区分統計
SELECT 
  '区分計算が完了しました' AS message,
  COUNT(DISTINCT signal_type) as signal_types,
  COUNT(*) as total_bins,
  AVG(sample_count) as avg_samples_per_type
FROM 
  `kabu-376213.kabu2411.m02_signal_bins`;

-- 4軸グループ統計
SELECT
  trade_type,
  COUNT(*) as total_groups,
  COUNTIF(is_effective) as effective_groups,
  ROUND(COUNTIF(is_effective) / COUNT(*) * 100, 1) as effective_rate,
  ROUND(AVG(CASE WHEN is_effective THEN win_rate END), 1) as avg_win_rate,
  ROUND(AVG(CASE WHEN is_effective THEN avg_profit_rate END), 2) as avg_profit_rate,
  ROUND(AVG(CASE WHEN is_effective THEN sharpe_ratio END), 2) as avg_sharpe_ratio
FROM
  `kabu-376213.kabu2411.d03_effective_4axis_groups`
GROUP BY
  trade_type;

-- カテゴリ別の有効グループ数
SELECT
  st.signal_category,
  eg.trade_type,
  COUNT(DISTINCT eg.signal_type) as signal_types,
  COUNT(*) as total_groups,
  COUNTIF(eg.is_effective) as effective_groups,
  ROUND(AVG(CASE WHEN eg.is_effective THEN eg.composite_score END), 1) as avg_score
FROM
  `kabu-376213.kabu2411.d03_effective_4axis_groups` eg
JOIN
  `kabu-376213.kabu2411.m01_signal_types` st
ON
  eg.signal_type = st.signal_type
GROUP BY
  st.signal_category, eg.trade_type
ORDER BY
  st.signal_category, eg.trade_type;