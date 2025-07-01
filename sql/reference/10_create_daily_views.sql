/*
ファイル: 10_create_daily_views_fixed_v2.sql
説明: 日次運用で使用するビューの定義（composite_scoreエラー修正版）
作成日: 2025-01-01
修正日: 2025-06-02 - composite_score参照エラーを修正
*/

-- ============================================================================
-- 1. 当日のシグナル一覧ビュー
-- ============================================================================
CREATE OR REPLACE VIEW `kabu-376213.kabu2411.v01_current_signals` AS
WITH latest_signals AS (
  SELECT
    sr.signal_date,
    sr.reference_date,
    sr.stock_code,
    sr.stock_name,
    sr.signal_type,
    sr.signal_value,
    sr.signal_category,
    -- 該当する区分を特定
    MAX(sb.signal_bin) as signal_bin,
    MAX(sb.percentile_rank) as signal_percentile
  FROM
    `kabu-376213.kabu2411.d01_signals_raw` sr
  INNER JOIN
    `kabu-376213.kabu2411.m02_signal_bins` sb
  ON
    sr.signal_type = sb.signal_type
    AND sr.signal_value <= sb.upper_bound
    AND sr.signal_value > sb.lower_bound
  WHERE
    sr.signal_date = CURRENT_DATE('Asia/Tokyo')
  GROUP BY
    sr.signal_date,
    sr.reference_date,
    sr.stock_code,
    sr.stock_name,
    sr.signal_type,
    sr.signal_value,
    sr.signal_category
)
SELECT
  ls.*,
  -- Buy側の4軸グループ実績
  pf_buy.total_count as buy_total_count,
  pf_buy.win_rate as buy_win_rate,
  pf_buy.avg_profit_rate as buy_avg_profit,
  pf_buy.sharpe_ratio as buy_sharpe_ratio,
  pf_buy.last_30d_count as buy_last_30d_count,
  pf_buy.last_30d_avg_profit as buy_last_30d_profit,
  -- Sell側の4軸グループ実績
  pf_sell.total_count as sell_total_count,
  pf_sell.win_rate as sell_win_rate,
  pf_sell.avg_profit_rate as sell_avg_profit,
  pf_sell.sharpe_ratio as sell_sharpe_ratio,
  pf_sell.last_30d_count as sell_last_30d_count,
  pf_sell.last_30d_avg_profit as sell_last_30d_profit,
  -- 有効性フラグ
  eg_buy.is_effective as buy_is_effective,
  eg_buy.composite_score as buy_composite_score,
  eg_sell.is_effective as sell_is_effective,
  eg_sell.composite_score as sell_composite_score
FROM
  latest_signals ls
-- Buy側の実績を結合
LEFT JOIN
  `kabu-376213.kabu2411.d02_signal_performance_4axis` pf_buy
ON
  ls.signal_type = pf_buy.signal_type
  AND ls.signal_bin = pf_buy.signal_bin
  AND ls.stock_code = pf_buy.stock_code
  AND pf_buy.trade_type = 'Buy'
-- Sell側の実績を結合
LEFT JOIN
  `kabu-376213.kabu2411.d02_signal_performance_4axis` pf_sell
ON
  ls.signal_type = pf_sell.signal_type
  AND ls.signal_bin = pf_sell.signal_bin
  AND ls.stock_code = pf_sell.stock_code
  AND pf_sell.trade_type = 'Sell'
-- 有効性情報を結合
LEFT JOIN
  `kabu-376213.kabu2411.d03_effective_4axis_groups` eg_buy
ON
  ls.signal_type = eg_buy.signal_type
  AND ls.signal_bin = eg_buy.signal_bin
  AND ls.stock_code = eg_buy.stock_code
  AND eg_buy.trade_type = 'Buy'
LEFT JOIN
  `kabu-376213.kabu2411.d03_effective_4axis_groups` eg_sell
ON
  ls.signal_type = eg_sell.signal_type
  AND ls.signal_bin = eg_sell.signal_bin
  AND ls.stock_code = eg_sell.stock_code
  AND eg_sell.trade_type = 'Sell';

-- ============================================================================
-- 2. 予測精度検証ビュー（30日間）
-- ============================================================================
CREATE OR REPLACE VIEW `kabu-376213.kabu2411.v02_prediction_accuracy` AS
WITH prediction_results AS (
  SELECT
    p.signal_date,
    p.stock_code,
    p.signal_type,
    p.signal_bin,
    p.trade_type,
    p.expected_profit_rate,
    p.confidence_score,
    r.actual_profit_rate,
    r.result_status,
    -- 予測誤差
    ABS(p.expected_profit_rate - r.actual_profit_rate) as prediction_error,
    -- 方向性の一致（利益/損失の予測が当たったか）
    CASE
      WHEN (p.expected_profit_rate > 0 AND r.actual_profit_rate > 0) OR
           (p.expected_profit_rate < 0 AND r.actual_profit_rate < 0)
      THEN 1
      ELSE 0
    END as direction_match
  FROM
    `kabu-376213.kabu2411.h01_signal_predictions` p
  INNER JOIN
    `kabu-376213.kabu2411.h02_trading_results` r
  ON
    p.prediction_id = r.prediction_id
  WHERE
    p.signal_date >= DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 30 DAY)
    AND r.actual_profit_rate IS NOT NULL
)
SELECT
  signal_type,
  trade_type,
  COUNT(*) as total_predictions,
  -- 予測精度指標
  ROUND(AVG(direction_match) * 100, 1) as direction_accuracy,
  ROUND(AVG(prediction_error), 2) as avg_prediction_error,
  ROUND(STDDEV(prediction_error), 2) as std_prediction_error,
  -- 実績vs予測
  ROUND(AVG(expected_profit_rate), 2) as avg_expected_profit,
  ROUND(AVG(actual_profit_rate), 2) as avg_actual_profit,
  ROUND(AVG(actual_profit_rate) - AVG(expected_profit_rate), 2) as profit_bias,
  -- 信頼度別の精度
  ROUND(AVG(CASE WHEN confidence_score >= 70 THEN direction_match END) * 100, 1) as high_confidence_accuracy,
  ROUND(AVG(CASE WHEN confidence_score < 70 THEN direction_match END) * 100, 1) as low_confidence_accuracy,
  -- 期間別の精度
  ROUND(AVG(CASE WHEN signal_date >= DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 7 DAY) THEN direction_match END) * 100, 1) as last_7d_accuracy,
  -- 結果分布
  COUNTIF(result_status = 'WIN') as win_count,
  COUNTIF(result_status = 'LOSS') as loss_count,
  ROUND(COUNTIF(result_status = 'WIN') / COUNT(*) * 100, 1) as actual_win_rate
FROM
  prediction_results
GROUP BY
  signal_type, trade_type
HAVING
  COUNT(*) >= 10  -- 最低10件の予測が必要
ORDER BY
  direction_accuracy DESC;

-- ============================================================================
-- 3. モニタリングダッシュボード用統計ビュー
-- ============================================================================
CREATE OR REPLACE VIEW `kabu-376213.kabu2411.v03_monitoring_dashboard` AS
WITH daily_stats AS (
  -- 日次統計
  SELECT
    signal_date,
    COUNT(DISTINCT stock_code) as active_stocks,
    COUNT(*) as total_signals,
    COUNTIF(buy_is_effective OR sell_is_effective) as effective_signals,
    AVG(CASE WHEN buy_is_effective THEN buy_avg_profit END) as avg_buy_profit,
    AVG(CASE WHEN sell_is_effective THEN sell_avg_profit END) as avg_sell_profit
  FROM
    `kabu-376213.kabu2411.v01_current_signals`
  WHERE
    signal_date >= DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 30 DAY)
  GROUP BY
    signal_date
),
category_performance AS (
  -- カテゴリ別パフォーマンス
  SELECT
    st.signal_category,
    eg.trade_type,
    COUNT(DISTINCT eg.stock_code) as active_stocks,
    COUNT(*) as effective_groups,
    ROUND(AVG(eg.win_rate), 1) as avg_win_rate,
    ROUND(AVG(eg.avg_profit_rate), 2) as avg_profit_rate,
    ROUND(AVG(eg.composite_score), 1) as avg_score
  FROM
    `kabu-376213.kabu2411.d03_effective_4axis_groups` eg
  JOIN
    `kabu-376213.kabu2411.m01_signal_types` st
  ON
    eg.signal_type = st.signal_type
  WHERE
    eg.is_effective = true
  GROUP BY
    st.signal_category, eg.trade_type
),
top_performers AS (
  -- トップパフォーマー（直近30日）
  SELECT
    pf.signal_type,
    pf.trade_type,
    pf.stock_code,
    pf.stock_name,
    pf.last_30d_count,
    pf.last_30d_win_rate,
    pf.last_30d_avg_profit,
    eg.composite_score,
    RANK() OVER (PARTITION BY pf.trade_type ORDER BY pf.last_30d_avg_profit DESC) as profit_rank,
    RANK() OVER (PARTITION BY pf.trade_type ORDER BY eg.composite_score DESC) as score_rank
  FROM
    `kabu-376213.kabu2411.d02_signal_performance_4axis` pf
  LEFT JOIN
    `kabu-376213.kabu2411.d03_effective_4axis_groups` eg
  ON
    pf.signal_type = eg.signal_type
    AND pf.signal_bin = eg.signal_bin
    AND pf.trade_type = eg.trade_type
    AND pf.stock_code = eg.stock_code
  WHERE
    pf.last_30d_count >= 10
    AND pf.last_30d_avg_profit > 0
)
SELECT
  -- 全体統計
  (SELECT COUNT(DISTINCT stock_code) FROM `kabu-376213.kabu2411.d01_signals_raw` WHERE signal_date = CURRENT_DATE('Asia/Tokyo')) as today_stocks,
  (SELECT COUNT(*) FROM `kabu-376213.kabu2411.d01_signals_raw` WHERE signal_date = CURRENT_DATE('Asia/Tokyo')) as today_signals,
  (SELECT COUNT(*) FROM `kabu-376213.kabu2411.d03_effective_4axis_groups` WHERE is_effective = true) as total_effective_groups,
  
  -- 日次トレンド（配列として返す）
  ARRAY(
    SELECT AS STRUCT
      signal_date,
      active_stocks,
      total_signals,
      effective_signals,
      ROUND(effective_signals / total_signals * 100, 1) as effective_rate,
      ROUND(avg_buy_profit, 2) as avg_buy_profit,
      ROUND(avg_sell_profit, 2) as avg_sell_profit
    FROM daily_stats
    ORDER BY signal_date DESC
    LIMIT 30
  ) as daily_trend,
  
  -- カテゴリ別パフォーマンス（配列として返す）
  ARRAY(
    SELECT AS STRUCT *
    FROM category_performance
    ORDER BY signal_category, trade_type
  ) as category_stats,
  
  -- トップパフォーマー（配列として返す）
  ARRAY(
    SELECT AS STRUCT
      signal_type,
      trade_type,
      stock_code,
      stock_name,
      last_30d_count,
      ROUND(last_30d_win_rate, 1) as win_rate,
      ROUND(last_30d_avg_profit, 2) as avg_profit,
      ROUND(composite_score, 1) as score
    FROM top_performers
    WHERE profit_rank <= 10 OR score_rank <= 10
    ORDER BY trade_type, last_30d_avg_profit DESC
  ) as top_performers;

-- ============================================================================
-- 4. 取引候補ランキングビュー
-- ============================================================================
CREATE OR REPLACE VIEW `kabu-376213.kabu2411.v04_trading_candidates_ranking` AS
WITH candidate_scores AS (
  SELECT
    stock_code,
    stock_name,
    signal_type,
    signal_bin,
    signal_value,
    signal_percentile,
    -- Buy候補としてのスコア
    CASE
      WHEN buy_is_effective THEN
        buy_composite_score * 
        (1 + GREATEST(buy_last_30d_profit, 0) / 100) * 
        LEAST(buy_last_30d_count / 10, 1.0)
      ELSE 0
    END as buy_score,
    buy_avg_profit,
    buy_win_rate,
    buy_sharpe_ratio,
    -- Sell候補としてのスコア
    CASE
      WHEN sell_is_effective THEN
        sell_composite_score * 
        (1 + GREATEST(sell_last_30d_profit, 0) / 100) * 
        LEAST(sell_last_30d_count / 10, 1.0)
      ELSE 0
    END as sell_score,
    sell_avg_profit,
    sell_win_rate,
    sell_sharpe_ratio
  FROM
    `kabu-376213.kabu2411.v01_current_signals`
  WHERE
    signal_date = CURRENT_DATE('Asia/Tokyo')
),
buy_candidates AS (
  -- Buy候補TOP50
  SELECT
    'Buy' as trade_type,
    stock_code,
    stock_name,
    signal_type,
    signal_bin,
    ROUND(signal_value, 2) as signal_value,
    signal_percentile,
    ROUND(buy_score, 1) as score,
    ROUND(buy_avg_profit, 2) as expected_profit,
    ROUND(buy_win_rate, 1) as win_rate,
    ROUND(buy_sharpe_ratio, 2) as sharpe_ratio,
    RANK() OVER (ORDER BY buy_score DESC) as rank
  FROM
    candidate_scores
  WHERE
    buy_score > 0
  ORDER BY
    buy_score DESC
  LIMIT 50
),
sell_candidates AS (
  -- Sell候補TOP50
  SELECT
    'Sell' as trade_type,
    stock_code,
    stock_name,
    signal_type,
    signal_bin,
    ROUND(signal_value, 2) as signal_value,
    signal_percentile,
    ROUND(sell_score, 1) as score,
    ROUND(sell_avg_profit, 2) as expected_profit,
    ROUND(sell_win_rate, 1) as win_rate,
    ROUND(sell_sharpe_ratio, 2) as sharpe_ratio,
    RANK() OVER (ORDER BY sell_score DESC) as rank
  FROM
    candidate_scores
  WHERE
    sell_score > 0
  ORDER BY
    sell_score DESC
  LIMIT 50
)
SELECT * FROM buy_candidates
UNION ALL
SELECT * FROM sell_candidates;

-- ============================================================================
-- 5. シグナル分布統計ビュー
-- ============================================================================
CREATE OR REPLACE VIEW `kabu-376213.kabu2411.v05_signal_distribution` AS
SELECT
  sr.signal_type,
  st.signal_category,
  st.is_score_type,
  COUNT(*) as signal_count,
  COUNT(DISTINCT sr.stock_code) as stock_count,
  -- 値の分布
  ROUND(MIN(sr.signal_value), 2) as min_value,
  ROUND(APPROX_QUANTILES(sr.signal_value, 4)[OFFSET(1)], 2) as q1_value,
  ROUND(APPROX_QUANTILES(sr.signal_value, 2)[OFFSET(1)], 2) as median_value,
  ROUND(APPROX_QUANTILES(sr.signal_value, 4)[OFFSET(3)], 2) as q3_value,
  ROUND(MAX(sr.signal_value), 2) as max_value,
  ROUND(AVG(sr.signal_value), 2) as mean_value,
  ROUND(STDDEV(sr.signal_value), 2) as std_value,
  -- 有効グループの統計
  COUNTIF(cs.buy_is_effective) as buy_effective_count,
  COUNTIF(cs.sell_is_effective) as sell_effective_count,
  ROUND(AVG(CASE WHEN cs.buy_is_effective THEN cs.buy_avg_profit END), 2) as avg_buy_profit,
  ROUND(AVG(CASE WHEN cs.sell_is_effective THEN cs.sell_avg_profit END), 2) as avg_sell_profit
FROM
  `kabu-376213.kabu2411.d01_signals_raw` sr
JOIN
  `kabu-376213.kabu2411.m01_signal_types` st
ON
  sr.signal_type = st.signal_type
LEFT JOIN
  `kabu-376213.kabu2411.v01_current_signals` cs
ON
  sr.signal_date = cs.signal_date
  AND sr.stock_code = cs.stock_code
  AND sr.signal_type = cs.signal_type
WHERE
  sr.signal_date = CURRENT_DATE('Asia/Tokyo')
GROUP BY
  sr.signal_type, st.signal_category, st.is_score_type
ORDER BY
  st.signal_category, sr.signal_type;

-- ============================================================================
-- ビュー作成完了の確認
-- ============================================================================
SELECT 
  '日次ビューの作成が完了しました' AS message,
  CURRENT_TIMESTAMP() AS completed_at;