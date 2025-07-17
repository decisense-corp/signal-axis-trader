-- ============================================================================
-- V64_cream_patterns_buy（2層目）
-- 厳選条件でフィルタリング
-- ============================================================================

CREATE OR REPLACE VIEW `kabu-376213.kabu2411.V64_cream_patterns_buy` AS
WITH cream_candidates AS (
  SELECT 
    v.*,
    -- 複合スコア（検証期間の利益率を重視しつつ、学習期間との整合性も考慮）
    CASE
      -- 両期間でプラスかつ安定的なパターンを高評価
      WHEN v.validation_avg_profit_rate > 0 
       AND v.learning_avg_profit_rate > 0 
       AND ABS(v.profit_degradation_pct) < 100  -- 極端な変動は除外
      THEN v.validation_avg_profit_rate * 0.7 + v.learning_avg_profit_rate * 0.3
      -- 検証期間のみプラスの場合は割引
      WHEN v.validation_avg_profit_rate > 0 
      THEN v.validation_avg_profit_rate * 0.5
      ELSE -999
    END as composite_score,
    -- 安定性スコア（両期間での一貫性）
    CASE
      WHEN v.validation_avg_profit_rate > 0 AND v.learning_avg_profit_rate > 0
      THEN 1 - ABS(v.profit_degradation_pct) / 100
      ELSE 0
    END as stability_score
  FROM `kabu-376213.kabu2411.V63_two_axis_validation_results_buy` v
  WHERE 
    -- 基本条件（BUYの成功基準を適用）
    v.learning_stop_loss_rate <= 10  -- 損切率10%以下
    AND v.learning_avg_profit_rate BETWEEN 0.2 AND 0.4  -- 学習期間の利益率
    AND v.learning_sharpe_ratio > 0.1  -- シャープレシオ
    AND v.validation_avg_profit_rate > 0  -- 検証期間でプラス
    AND v.validation_trades >= 50  -- 検証期間で十分な取引数
)
SELECT 
  signal_type_1,
  signal_bin_1,
  signal_type_2,
  signal_bin_2,
  pattern_key,
  optimal_stop_loss_margin,
  optimal_profit_margin,
  -- 学習期間の統計
  learning_avg_profit_rate,
  learning_win_rate,
  learning_sharpe_ratio,
  learning_stop_loss_rate,
  -- 検証期間の統計
  validation_trades,
  validation_avg_profit_rate,
  validation_win_rate,
  validation_sharpe_ratio,
  validation_stop_loss_rate,
  -- 差分と劣化率
  profit_rate_diff,
  profit_degradation_pct,
  -- スコア
  ROUND(composite_score, 4) as composite_score,
  ROUND(stability_score, 3) as stability_score,
  -- ランキング
  ROW_NUMBER() OVER (ORDER BY composite_score DESC) as rank,
  'BUY' as trade_type
FROM cream_candidates
ORDER BY composite_score DESC;