-- ============================================================================
-- V75_two_axis_validation_results_sell（スコア版）
-- 検証期間の実績計算
-- 
-- 【重要】検証期間の変更方法：
-- このView内の '2025-06-01' を全て検索・置換してください
-- 現在の設定：2025-06-01以降
-- ============================================================================

CREATE OR REPLACE VIEW `kabu-376213.kabu2411.V75_two_axis_validation_results_sell` AS
WITH 
-- 最適化済みパターンの取得
optimized_patterns AS (
  SELECT 
    score_type_1,
    score_bin_1,
    score_type_2,
    score_bin_2,
    pattern_key,
    optimal_stop_loss_margin,
    optimal_profit_margin,
    avg_profit_rate as learning_avg_profit_rate,
    win_rate as learning_win_rate,
    sharpe_ratio as learning_sharpe_ratio,
    stop_loss_rate as learning_stop_loss_rate
  FROM `kabu-376213.kabu2411.D74_two_axis_patterns_optimized_sell`
),

-- 検証期間の取引データ
validation_trades AS (
  SELECT 
    op.pattern_key,
    op.score_type_1,
    op.score_bin_1,
    op.score_type_2,
    op.score_bin_2,
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
    -- SELL版の利益率計算
    CASE
      -- SELLの損切判定（高値が上昇）
      WHEN (d.day_high - d.day_open) / d.day_open * 100 >= op.optimal_stop_loss_margin 
        THEN -op.optimal_stop_loss_margin
      -- SELLの利確判定（安値が下落）
      WHEN (d.day_low - d.day_open) / d.day_open * 100 <= -op.optimal_profit_margin 
        THEN op.optimal_profit_margin
      -- 引け決済（SELLの場合：始値-終値）
      ELSE (d.day_open - d.day_close) / d.day_open * 100
    END as profit_rate,
    -- 決済タイプ
    CASE
      WHEN (d.day_high - d.day_open) / d.day_open * 100 >= op.optimal_stop_loss_margin 
        THEN 'stop_loss'
      WHEN (d.day_low - d.day_open) / d.day_open * 100 <= -op.optimal_profit_margin 
        THEN 'take_profit'
      ELSE 'close'
    END as exit_type
  FROM optimized_patterns op
  CROSS JOIN `kabu-376213.kabu2411.D30_trading_scores` d
  WHERE 
    -- 検証期間（2025-06-01以降）
    d.signal_date >= DATE('2025-06-01')
    AND d.trade_type = 'SELL'
    AND d.day_open > 0
    -- 両方の条件を満たす銘柄
    AND EXISTS (
      SELECT 1 FROM `kabu-376213.kabu2411.D30_trading_scores` d1
      WHERE d1.signal_date = d.signal_date
        AND d1.stock_code = d.stock_code
        AND d1.trade_type = 'SELL'
        AND d1.score_type = op.score_type_1
        AND d1.score_bin = op.score_bin_1
    )
    AND EXISTS (
      SELECT 1 FROM `kabu-376213.kabu2411.D30_trading_scores` d2
      WHERE d2.signal_date = d.signal_date
        AND d2.stock_code = d.stock_code
        AND d2.trade_type = 'SELL'
        AND d2.score_type = op.score_type_2
        AND d2.score_bin = op.score_bin_2
    )
)

-- 検証期間の集計
SELECT 
  pattern_key,
  score_type_1,
  score_bin_1,
  score_type_2,
  score_bin_2,
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
  'SELL' as trade_type
FROM validation_trades
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11;

-- ============================================================================
-- 実行後の確認クエリ
-- ============================================================================

-- 検証結果サマリー
SELECT 
  COUNT(*) as total_patterns,
  ROUND(AVG(validation_avg_profit_rate), 4) as avg_validation_profit,
  ROUND(AVG(learning_avg_profit_rate), 4) as avg_learning_profit,
  ROUND(AVG(profit_degradation_pct), 2) as avg_degradation_pct,
  SUM(CASE WHEN validation_avg_profit_rate > 0 THEN 1 ELSE 0 END) as profitable_patterns,
  SUM(CASE WHEN validation_avg_profit_rate > learning_avg_profit_rate THEN 1 ELSE 0 END) as improved_patterns
FROM `kabu-376213.kabu2411.V75_two_axis_validation_results_sell`;

-- 劣化率の分布
SELECT 
  CASE 
    WHEN profit_degradation_pct < -50 THEN '大幅悪化（-50%未満）'
    WHEN profit_degradation_pct < -20 THEN '悪化（-50%～-20%）'
    WHEN profit_degradation_pct < 0 THEN '軽微な悪化（-20%～0%）'
    WHEN profit_degradation_pct < 20 THEN '改善（0%～20%）'
    ELSE '大幅改善（20%以上）'
  END as degradation_category,
  COUNT(*) as pattern_count,
  ROUND(AVG(validation_avg_profit_rate), 4) as avg_profit_rate
FROM `kabu-376213.kabu2411.V75_two_axis_validation_results_sell`
GROUP BY degradation_category
ORDER BY 
  CASE degradation_category
    WHEN '大幅改善（20%以上）' THEN 1
    WHEN '改善（0%～20%）' THEN 2
    WHEN '軽微な悪化（-20%～0%）' THEN 3
    WHEN '悪化（-50%～-20%）' THEN 4
    WHEN '大幅悪化（-50%未満）' THEN 5
  END;

-- TOP10パターン（検証期間）
SELECT 
  pattern_key,
  validation_trades,
  validation_avg_profit_rate,
  validation_win_rate,
  validation_sharpe_ratio,
  learning_avg_profit_rate,
  profit_degradation_pct,
  optimal_stop_loss_margin,
  optimal_profit_margin
FROM `kabu-376213.kabu2411.V75_two_axis_validation_results_sell`
WHERE validation_trades >= 10  -- 最低取引数
ORDER BY validation_avg_profit_rate DESC
LIMIT 10;

-- 安定性の高いパターン（劣化が少ない）
SELECT 
  pattern_key,
  validation_trades,
  validation_avg_profit_rate,
  learning_avg_profit_rate,
  profit_degradation_pct,
  validation_win_rate,
  learning_win_rate
FROM `kabu-376213.kabu2411.V75_two_axis_validation_results_sell`
WHERE validation_trades >= 10
  AND ABS(profit_degradation_pct) <= 20  -- 劣化率±20%以内
  AND validation_avg_profit_rate > 0
ORDER BY validation_avg_profit_rate DESC
LIMIT 20;

-- BUY/SELL検証結果の比較
SELECT 
  'BUY' as trade_type,
  COUNT(*) as pattern_count,
  ROUND(AVG(validation_avg_profit_rate), 4) as avg_profit,
  ROUND(AVG(profit_degradation_pct), 2) as avg_degradation,
  SUM(CASE WHEN validation_avg_profit_rate > 0 THEN 1 ELSE 0 END) as profitable_count,
  ROUND(100.0 * SUM(CASE WHEN validation_avg_profit_rate > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as profitable_pct
FROM `kabu-376213.kabu2411.V65_two_axis_validation_results_buy`
UNION ALL
SELECT 
  'SELL' as trade_type,
  COUNT(*) as pattern_count,
  ROUND(AVG(validation_avg_profit_rate), 4) as avg_profit,
  ROUND(AVG(profit_degradation_pct), 2) as avg_degradation,
  SUM(CASE WHEN validation_avg_profit_rate > 0 THEN 1 ELSE 0 END) as profitable_count,
  ROUND(100.0 * SUM(CASE WHEN validation_avg_profit_rate > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as profitable_pct
FROM `kabu-376213.kabu2411.V75_two_axis_validation_results_sell`
ORDER BY trade_type;