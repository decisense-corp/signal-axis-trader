-- ============================================================================
-- V63_two_axis_validation_results_buy（1層目）
-- 検証期間の実績計算
-- 
-- 【重要】検証期間の変更方法：
-- このView内の '2025-06-01' を全て検索・置換してください
-- 現在の設定：2025-06-01以降
-- ============================================================================

CREATE OR REPLACE VIEW `kabu-376213.kabu2411.V63_two_axis_validation_results_buy` AS
WITH 
-- 最適化済みパターンの取得
optimized_patterns AS (
  SELECT 
    signal_type_1,
    signal_bin_1,
    signal_type_2,
    signal_bin_2,
    pattern_key,
    optimal_stop_loss_margin,
    optimal_profit_margin,
    avg_profit_rate as learning_avg_profit_rate,
    win_rate as learning_win_rate,
    sharpe_ratio as learning_sharpe_ratio,
    stop_loss_rate as learning_stop_loss_rate
  FROM `kabu-376213.kabu2411.D62_two_axis_patterns_optimized_buy`
),

-- 検証期間の取引データ
validation_trades AS (
  SELECT 
    op.pattern_key,
    op.signal_type_1,
    op.signal_bin_1,
    op.signal_type_2,
    op.signal_bin_2,
    op.optimal_stop_loss_margin,
    op.optimal_profit_margin,
    op.learning_avg_profit_rate,
    op.learning_win_rate,
    op.learning_sharpe_ratio,
    op.learning_stop_loss_rate,
    d.signal_date,
    d.stock_code,
    d.day_open,
    d.day_high,
    d.day_low,
    d.day_close,
    -- BUY版の利益率計算
    CASE
      -- 損切判定（安値が下落）
      WHEN (d.day_low - d.day_open) / d.day_open * 100 <= -op.optimal_stop_loss_margin 
        THEN -op.optimal_stop_loss_margin
      -- 利確判定（高値が上昇）
      WHEN (d.day_high - d.day_open) / d.day_open * 100 >= op.optimal_profit_margin 
        THEN op.optimal_profit_margin
      -- 引け決済
      ELSE (d.day_close - d.day_open) / d.day_open * 100
    END as profit_rate,
    -- 決済タイプ
    CASE
      WHEN (d.day_low - d.day_open) / d.day_open * 100 <= -op.optimal_stop_loss_margin 
        THEN 'stop_loss'
      WHEN (d.day_high - d.day_open) / d.day_open * 100 >= op.optimal_profit_margin 
        THEN 'take_profit'
      ELSE 'close'
    END as exit_type
  FROM optimized_patterns op
  CROSS JOIN `kabu-376213.kabu2411.D10_trading_signals` d
  WHERE 
    -- 検証期間（2025-06-01以降）
    d.signal_date >= DATE('2025-06-01')
    AND d.trade_type = 'BUY'
    AND d.day_open > 0
    -- 両方の条件を満たす銘柄
    AND EXISTS (
      SELECT 1 FROM `kabu-376213.kabu2411.D10_trading_signals` d1
      WHERE d1.signal_date = d.signal_date
        AND d1.stock_code = d.stock_code
        AND d1.trade_type = 'BUY'
        AND d1.signal_type = op.signal_type_1
        AND d1.signal_bin = op.signal_bin_1
    )
    AND EXISTS (
      SELECT 1 FROM `kabu-376213.kabu2411.D10_trading_signals` d2
      WHERE d2.signal_date = d.signal_date
        AND d2.stock_code = d.stock_code
        AND d2.trade_type = 'BUY'
        AND d2.signal_type = op.signal_type_2
        AND d2.signal_bin = op.signal_bin_2
    )
)

-- 検証期間の集計
SELECT 
  pattern_key,
  signal_type_1,
  signal_bin_1,
  signal_type_2,
  signal_bin_2,
  optimal_stop_loss_margin,
  optimal_profit_margin,
  learning_avg_profit_rate,
  learning_win_rate,
  learning_sharpe_ratio,
  learning_stop_loss_rate,
  COUNT(*) as validation_trades,
  ROUND(AVG(profit_rate), 4) as validation_avg_profit_rate,
  ROUND(STDDEV(profit_rate), 4) as validation_profit_stddev,
  -- 勝率
  ROUND(100.0 * SUM(CASE WHEN profit_rate > 0 THEN 1 ELSE 0 END) / COUNT(*), 2) as validation_win_rate,
  -- シャープレシオ
  ROUND(SAFE_DIVIDE(AVG(profit_rate), STDDEV(profit_rate)), 3) as validation_sharpe_ratio,
  -- 決済タイプ別カウント
  SUM(CASE WHEN exit_type = 'stop_loss' THEN 1 ELSE 0 END) as validation_stop_loss_count,
  SUM(CASE WHEN exit_type = 'take_profit' THEN 1 ELSE 0 END) as validation_take_profit_count,
  SUM(CASE WHEN exit_type = 'close' THEN 1 ELSE 0 END) as validation_close_count,
  -- 損切率
  ROUND(100.0 * SUM(CASE WHEN exit_type = 'stop_loss' THEN 1 ELSE 0 END) / COUNT(*), 2) as validation_stop_loss_rate,
  -- 学習期間との差分
  ROUND(AVG(profit_rate) - learning_avg_profit_rate, 4) as profit_rate_diff,
  -- 劣化率
  ROUND((AVG(profit_rate) - learning_avg_profit_rate) / learning_avg_profit_rate * 100, 2) as profit_degradation_pct,
  'BUY' as trade_type
FROM validation_trades
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11;