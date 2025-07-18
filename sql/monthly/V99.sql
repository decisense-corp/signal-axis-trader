-- ============================================================================
-- V99_top50_signals（3層目）
-- BUY/SELL統合TOP50選択
-- 【2025-07-18: スコア系統合を一旦切り戻し】
-- ============================================================================

CREATE OR REPLACE VIEW `kabu-376213.kabu2411.V99_top50_signals` AS
WITH 
-- BUY側の全パターン取得（TOP20制限なし）
buy_patterns AS (
  SELECT 
    signal_type_1,
    signal_bin_1,
    signal_type_2,
    signal_bin_2,
    pattern_key,
    optimal_stop_loss_margin,
    optimal_profit_margin,
    validation_avg_profit_rate,
    composite_score,
    rank
  FROM `kabu-376213.kabu2411.V64_cream_patterns_buy`
  -- WHERE rank <= 20  -- TOP20制限を撤廃
),

-- SELL側の全パターン取得（TOP20制限なし）
sell_patterns AS (
  SELECT 
    signal_type_1,
    signal_bin_1,
    signal_type_2,
    signal_bin_2,
    pattern_key,
    optimal_stop_loss_margin,
    optimal_profit_margin,
    validation_avg_profit_rate,
    composite_score,
    rank
  FROM `kabu-376213.kabu2411.V74_cream_patterns_sell`
  -- WHERE rank <= 20  -- TOP20制限を撤廃
),

-- BUY側の明日のシグナル
buy_tomorrow_signals AS (
  SELECT DISTINCT
    d.target_date,
    d.stock_code,
    d.stock_name,
    'Buy' as trade_type,
    bp.rank as pattern_rank,
    bp.pattern_key,
    bp.optimal_stop_loss_margin as stop_loss_pct,
    bp.optimal_profit_margin as take_profit_pct,
    bp.validation_avg_profit_rate as expected_profit_pct,
    bp.composite_score,
    -- 流動性情報
    COALESCE(d1.prev_close, d2.prev_close) as prev_close,
    COALESCE(d1.tradable_shares, d2.tradable_shares) as tradable_shares,
    -- 統計情報（両パターンの平均）
    (COALESCE(d1.win_rate, 0) + COALESCE(d2.win_rate, 0)) / 2 as avg_win_rate,
    (COALESCE(d1.avg_profit_rate, 0) + COALESCE(d2.avg_profit_rate, 0)) / 2 as avg_historical_profit
  FROM `kabu-376213.kabu2411.D20_tomorrow_signals` d
  CROSS JOIN buy_patterns bp
  INNER JOIN `kabu-376213.kabu2411.D20_tomorrow_signals` d1
    ON d1.target_date = d.target_date
    AND d1.stock_code = d.stock_code
    AND d1.trade_type = 'BUY'
    AND d1.signal_type = bp.signal_type_1
    AND d1.signal_bin = bp.signal_bin_1
  INNER JOIN `kabu-376213.kabu2411.D20_tomorrow_signals` d2
    ON d2.target_date = d.target_date
    AND d2.stock_code = d.stock_code
    AND d2.trade_type = 'BUY'
    AND d2.signal_type = bp.signal_type_2
    AND d2.signal_bin = bp.signal_bin_2
  WHERE 
    d.trade_type = 'BUY'
),

-- SELL側の明日のシグナル
sell_tomorrow_signals AS (
  SELECT DISTINCT
    d.target_date,
    d.stock_code,
    d.stock_name,
    'Sell' as trade_type,
    sp.rank as pattern_rank,
    sp.pattern_key,
    sp.optimal_stop_loss_margin as stop_loss_pct,
    sp.optimal_profit_margin as take_profit_pct,
    sp.validation_avg_profit_rate as expected_profit_pct,
    sp.composite_score,
    -- 流動性情報
    COALESCE(d1.prev_close, d2.prev_close) as prev_close,
    COALESCE(d1.tradable_shares, d2.tradable_shares) as tradable_shares,
    -- 統計情報（両パターンの平均）
    (COALESCE(d1.win_rate, 0) + COALESCE(d2.win_rate, 0)) / 2 as avg_win_rate,
    (COALESCE(d1.avg_profit_rate, 0) + COALESCE(d2.avg_profit_rate, 0)) / 2 as avg_historical_profit
  FROM `kabu-376213.kabu2411.D20_tomorrow_signals` d
  CROSS JOIN sell_patterns sp
  INNER JOIN `kabu-376213.kabu2411.D20_tomorrow_signals` d1
    ON d1.target_date = d.target_date
    AND d1.stock_code = d.stock_code
    AND d1.trade_type = 'SELL'
    AND d1.signal_type = sp.signal_type_1
    AND d1.signal_bin = sp.signal_bin_1
  INNER JOIN `kabu-376213.kabu2411.D20_tomorrow_signals` d2
    ON d2.target_date = d.target_date
    AND d2.stock_code = d.stock_code
    AND d2.trade_type = 'SELL'
    AND d2.signal_type = sp.signal_type_2
    AND d2.signal_bin = sp.signal_bin_2
  WHERE 
    d.trade_type = 'SELL'
),

-- BUYとSELLを統合
all_signals AS (
  SELECT * FROM buy_tomorrow_signals
  UNION ALL
  SELECT * FROM sell_tomorrow_signals
),

-- 重複排除と上位選択
unique_signals AS (
  SELECT 
    *,
    -- 同一銘柄で複数シグナルがある場合の優先順位
    ROW_NUMBER() OVER (
      PARTITION BY stock_code 
      ORDER BY expected_profit_pct DESC, composite_score DESC
    ) as stock_priority
  FROM all_signals
)

-- 最終出力（TOP50）
SELECT 
  target_date,
  stock_code,
  trade_type as TradeType,
  NULL as PurchaseQuantity,
  -- 利確額
  ROUND(prev_close * take_profit_pct / 100, 0) as TakeProfitRange,
  -- 損切額
  ROUND(prev_close * stop_loss_pct / 100, 0) as StopLossRange,
  stock_name,
  expected_profit_pct,
  prev_close,
  tradable_shares,
  -- 推奨取引額（前日終値 × 売買可能株数）
  ROUND(prev_close * tradable_shares, 0) as recommended_trade_amount,
  avg_win_rate,
  avg_historical_profit,
  pattern_rank,
  pattern_key
FROM unique_signals
WHERE stock_priority = 1  -- 各銘柄で最良のパターンのみ
QUALIFY RANK() OVER (ORDER BY expected_profit_pct DESC) <= 50  -- TOP50選択
ORDER BY expected_profit_pct DESC;