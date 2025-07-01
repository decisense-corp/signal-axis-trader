/*
ファイル: 21_update_4axis_performance.sql
説明: 4軸グループの実績を最新データで再計算（週次実行）
作成日: 2025-01-01
修正日: 2025-06-08 - INサブクエリのエラーを修正（複数カラムを返していた問題）
実行時間: 約5-7分
*/

-- ============================================================================
-- 1. 更新対象の特定
-- ============================================================================
DECLARE update_start_date DATE DEFAULT DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 7 DAY);
DECLARE latest_signal_date DATE DEFAULT (
  SELECT MAX(signal_date) 
  FROM `kabu-376213.kabu2411.d01_signals_raw`
);

-- 処理開始メッセージ
SELECT 
  '4軸グループ実績の更新を開始します' as message,
  update_start_date as update_from,
  latest_signal_date as latest_data,
  (SELECT COUNT(DISTINCT signal_date) FROM `kabu-376213.kabu2411.d01_signals_raw` 
   WHERE signal_date >= update_start_date) as new_days;

-- ============================================================================
-- 2. 新規シグナルの区分付け（最新の区分を適用）
-- ============================================================================
CREATE TEMP TABLE recent_signals_with_bins AS
WITH recent_signals AS (
  -- 最近追加されたシグナル
  SELECT
    sr.signal_date,
    sr.reference_date,
    sr.stock_code,
    sr.stock_name,
    sr.signal_type,
    sr.signal_value,
    -- 最新の区分を適用
    MAX(sb.signal_bin) as signal_bin
  FROM
    `kabu-376213.kabu2411.d01_signals_raw` sr
  INNER JOIN
    `kabu-376213.kabu2411.m02_signal_bins` sb
  ON
    sr.signal_type = sb.signal_type
    AND sr.signal_value <= sb.upper_bound
    AND sr.signal_value > sb.lower_bound
  WHERE
    sr.signal_date >= update_start_date
  GROUP BY
    sr.signal_date,
    sr.reference_date,
    sr.stock_code,
    sr.stock_name,
    sr.signal_type,
    sr.signal_value
)
SELECT
  rs.*,
  -- 結果データを結合
  q_next.Open as next_open,
  q_next.High as next_high,
  q_next.Low as next_low,
  q_next.Close as next_close,
  q_next.Volume as next_volume
FROM
  recent_signals rs
LEFT JOIN
  `kabu-376213.kabu2411.daily_quotes` q_next
ON
  rs.stock_code = REGEXP_REPLACE(q_next.Code, '0$', '')
  AND rs.signal_date = q_next.Date
WHERE
  rs.signal_bin IS NOT NULL
  AND q_next.Open > 0 
  AND q_next.Close > 0;

-- ============================================================================
-- 3. 差分更新用の実績計算（初期クエリのパターンを参考に）
-- ============================================================================
CREATE TEMP TABLE performance_updates AS
WITH performance_calculation AS (
  -- Buy実績
  SELECT
    signal_type,
    signal_bin,
    'Buy' as trade_type,
    stock_code,
    stock_name,
    COUNT(*) as new_count,
    AVG(SAFE_DIVIDE(next_close - next_open, next_open) * 100) as new_avg_profit,
    COUNTIF(next_close > next_open) as new_wins,
    MIN(signal_date) as first_date,
    MAX(signal_date) as last_date
  FROM
    recent_signals_with_bins
  GROUP BY
    signal_type, signal_bin, stock_code, stock_name
  
  UNION ALL
  
  -- Sell実績
  SELECT
    signal_type,
    signal_bin,
    'Sell' as trade_type,
    stock_code,
    stock_name,
    COUNT(*) as new_count,
    AVG(SAFE_DIVIDE(next_open - next_close, next_open) * 100) as new_avg_profit,
    COUNTIF(next_open > next_close) as new_wins,
    MIN(signal_date) as first_date,
    MAX(signal_date) as last_date
  FROM
    recent_signals_with_bins
  GROUP BY
    signal_type, signal_bin, stock_code, stock_name
)
SELECT * FROM performance_calculation;

-- ============================================================================
-- 4. 全期間の実績を再計算（初期クエリのパターンに準拠）
-- ============================================================================
CREATE TEMP TABLE full_performance_recalc AS
WITH all_signals_for_update AS (
  -- 更新対象の4軸グループの全履歴を取得
  SELECT
    sr.signal_date,
    sr.reference_date,
    sr.stock_code,
    sr.stock_name,
    sr.signal_type,
    sr.signal_value,
    MAX(sb.signal_bin) as signal_bin,
    q.Open as next_open,
    q.Close as next_close
  FROM
    `kabu-376213.kabu2411.d01_signals_raw` sr
  INNER JOIN
    `kabu-376213.kabu2411.m02_signal_bins` sb
  ON
    sr.signal_type = sb.signal_type
    AND sr.signal_value <= sb.upper_bound
    AND sr.signal_value > sb.lower_bound
  INNER JOIN
    performance_updates pu
  ON
    sr.signal_type = pu.signal_type
    AND sb.signal_bin = pu.signal_bin
    AND sr.stock_code = pu.stock_code
  LEFT JOIN
    `kabu-376213.kabu2411.daily_quotes` q
  ON
    sr.stock_code = REGEXP_REPLACE(q.Code, '0$', '')
    AND sr.signal_date = q.Date
  WHERE
    q.Open > 0 AND q.Close > 0
  GROUP BY
    sr.signal_date,
    sr.reference_date,
    sr.stock_code,
    sr.stock_name,
    sr.signal_type,
    sr.signal_value,
    q.Open,
    q.Close
),
signals_with_trade_type AS (
  -- 各シグナルにtrade_typeを展開
  SELECT
    afu.*,
    trade_type
  FROM
    all_signals_for_update afu
  CROSS JOIN
    UNNEST(['Buy', 'Sell']) AS trade_type
),
recalculated_performance AS (
  -- Buy実績の再計算
  SELECT
    signal_type,
    signal_bin,
    'Buy' as trade_type,
    stock_code,
    stock_name,
    COUNT(*) as total_count,
    AVG(SAFE_DIVIDE(next_close - next_open, next_open) * 100) as avg_profit_rate,
    STDDEV(SAFE_DIVIDE(next_close - next_open, next_open) * 100) as std_profit_rate,
    COUNTIF(next_close > next_open) as win_count,
    APPROX_QUANTILES(SAFE_DIVIDE(next_close - next_open, next_open) * 100, 2)[OFFSET(1)] as median_profit_rate,
    MAX(SAFE_DIVIDE(next_close - next_open, next_open) * 100) as max_profit_rate,
    MIN(SAFE_DIVIDE(next_close - next_open, next_open) * 100) as min_profit_rate,
    MIN(signal_date) as first_signal_date,
    MAX(signal_date) as last_signal_date
  FROM
    signals_with_trade_type
  WHERE
    trade_type = 'Buy'
  GROUP BY
    signal_type, signal_bin, stock_code, stock_name
  
  UNION ALL
  
  -- Sell実績の再計算
  SELECT
    signal_type,
    signal_bin,
    'Sell' as trade_type,
    stock_code,
    stock_name,
    COUNT(*) as total_count,
    AVG(SAFE_DIVIDE(next_open - next_close, next_open) * 100) as avg_profit_rate,
    STDDEV(SAFE_DIVIDE(next_open - next_close, next_open) * 100) as std_profit_rate,
    COUNTIF(next_open > next_close) as win_count,
    APPROX_QUANTILES(SAFE_DIVIDE(next_open - next_close, next_open) * 100, 2)[OFFSET(1)] as median_profit_rate,
    MAX(SAFE_DIVIDE(next_open - next_close, next_open) * 100) as max_profit_rate,
    MIN(SAFE_DIVIDE(next_open - next_close, next_open) * 100) as min_profit_rate,
    MIN(signal_date) as first_signal_date,
    MAX(signal_date) as last_signal_date
  FROM
    signals_with_trade_type
  WHERE
    trade_type = 'Sell'
  GROUP BY
    signal_type, signal_bin, stock_code, stock_name
),
recent_period_stats AS (
  -- 30日・90日の実績（初期クエリの構造を使用）
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
  GROUP BY
    signal_type, signal_bin, trade_type, stock_code
)
-- 最終結果の結合
SELECT
  rp.signal_type,
  rp.signal_bin,
  rp.trade_type,
  rp.stock_code,
  rp.stock_name,
  rp.total_count,
  rp.win_count,
  ROUND(SAFE_DIVIDE(rp.win_count, rp.total_count) * 100, 2) as win_rate,
  ROUND(rp.avg_profit_rate, 4) as avg_profit_rate,
  ROUND(rp.median_profit_rate, 4) as median_profit_rate,
  ROUND(rp.std_profit_rate, 4) as std_profit_rate,
  ROUND(SAFE_DIVIDE(rp.avg_profit_rate, NULLIF(rp.std_profit_rate, 0)), 4) as sharpe_ratio,
  ROUND(rp.max_profit_rate, 4) as max_profit_rate,
  ROUND(rp.min_profit_rate, 4) as min_profit_rate,
  COALESCE(rps.last_30d_count, 0) as last_30d_count,
  ROUND(COALESCE(rps.last_30d_win_rate, 0), 2) as last_30d_win_rate,
  ROUND(COALESCE(rps.last_30d_avg_profit, 0), 4) as last_30d_avg_profit,
  COALESCE(rps.last_90d_count, 0) as last_90d_count,
  ROUND(COALESCE(rps.last_90d_win_rate, 0), 2) as last_90d_win_rate,
  ROUND(COALESCE(rps.last_90d_avg_profit, 0), 4) as last_90d_avg_profit,
  rp.first_signal_date,
  rp.last_signal_date,
  CURRENT_DATE() as last_updated,
  CURRENT_TIMESTAMP() as updated_at
FROM
  recalculated_performance rp
LEFT JOIN
  recent_period_stats rps
ON
  rp.signal_type = rps.signal_type
  AND rp.signal_bin = rps.signal_bin
  AND rp.trade_type = rps.trade_type
  AND rp.stock_code = rps.stock_code;

-- ============================================================================
-- 5. 既存データの更新（修正版：複合主キーを使用）
-- ============================================================================
-- 更新対象のレコードを削除
DELETE FROM `kabu-376213.kabu2411.d02_signal_performance_4axis` t1
WHERE EXISTS (
  SELECT 1
  FROM full_performance_recalc t2
  WHERE t1.signal_type = t2.signal_type
    AND t1.signal_bin = t2.signal_bin
    AND t1.trade_type = t2.trade_type
    AND t1.stock_code = t2.stock_code
);

-- 再計算したデータを挿入
INSERT INTO `kabu-376213.kabu2411.d02_signal_performance_4axis`
SELECT * FROM full_performance_recalc;

-- 新規4軸グループの追加（初めて出現した組み合わせ）
INSERT INTO `kabu-376213.kabu2411.d02_signal_performance_4axis`
WITH new_groups AS (
  SELECT DISTINCT
    signal_type,
    signal_bin,
    trade_type,
    stock_code,
    stock_name
  FROM
    recent_signals_with_bins rsw
  CROSS JOIN
    (SELECT 'Buy' as trade_type UNION ALL SELECT 'Sell') tt
  WHERE
    NOT EXISTS (
      SELECT 1
      FROM `kabu-376213.kabu2411.d02_signal_performance_4axis` pf
      WHERE pf.signal_type = rsw.signal_type
        AND pf.signal_bin = rsw.signal_bin
        AND pf.trade_type = tt.trade_type
        AND pf.stock_code = rsw.stock_code
    )
)
SELECT
  ng.signal_type,
  ng.signal_bin,
  ng.trade_type,
  ng.stock_code,
  ng.stock_name,
  COALESCE(p.total_count, 0) as total_count,
  COALESCE(p.win_count, 0) as win_count,
  COALESCE(p.win_rate, 0) as win_rate,
  COALESCE(p.avg_profit_rate, 0) as avg_profit_rate,
  COALESCE(p.median_profit_rate, 0) as median_profit_rate,
  COALESCE(p.std_profit_rate, 0) as std_profit_rate,
  COALESCE(p.sharpe_ratio, 0) as sharpe_ratio,
  COALESCE(p.max_profit_rate, 0) as max_profit_rate,
  COALESCE(p.min_profit_rate, 0) as min_profit_rate,
  0 as last_30d_count,
  0 as last_30d_win_rate,
  0 as last_30d_avg_profit,
  0 as last_90d_count,
  0 as last_90d_win_rate,
  0 as last_90d_avg_profit,
  p.first_signal_date,
  p.last_signal_date,
  CURRENT_DATE() as last_updated,
  CURRENT_TIMESTAMP() as updated_at
FROM
  new_groups ng
LEFT JOIN
  full_performance_recalc p
ON
  ng.signal_type = p.signal_type
  AND ng.signal_bin = p.signal_bin
  AND ng.trade_type = p.trade_type
  AND ng.stock_code = p.stock_code;

-- ============================================================================
-- 6. 更新結果の確認
-- ============================================================================
WITH update_summary AS (
  SELECT
    trade_type,
    COUNT(*) as total_groups,
    COUNTIF(last_updated = CURRENT_DATE()) as updated_today,
    COUNTIF(last_30d_count > 0) as active_30d,
    COUNTIF(last_90d_count > 0) as active_90d,
    ROUND(AVG(CASE WHEN last_30d_count > 0 THEN last_30d_avg_profit END), 2) as avg_30d_profit
  FROM
    `kabu-376213.kabu2411.d02_signal_performance_4axis`
  GROUP BY
    trade_type
)
SELECT
  '4軸グループ実績の更新が完了しました' as message,
  ARRAY_AGG(
    STRUCT(
      trade_type,
      total_groups,
      updated_today,
      ROUND(updated_today / total_groups * 100, 1) as update_rate,
      active_30d,
      active_90d,
      avg_30d_profit
    )
    ORDER BY trade_type
  ) as summary,
  CURRENT_TIMESTAMP() as completed_at
FROM
  update_summary;

-- パフォーマンス向上TOP10
SELECT
  signal_type,
  trade_type,
  stock_code,
  stock_name,
  total_count,
  ROUND(win_rate, 1) as win_rate,
  ROUND(avg_profit_rate, 2) as avg_profit,
  ROUND(last_30d_avg_profit, 2) as last_30d_profit,
  ROUND(last_30d_avg_profit - avg_profit_rate, 2) as improvement
FROM
  `kabu-376213.kabu2411.d02_signal_performance_4axis`
WHERE
  last_30d_count >= 5
  AND last_30d_avg_profit > avg_profit_rate
  AND total_count >= 30
ORDER BY
  last_30d_avg_profit - avg_profit_rate DESC
LIMIT 10;