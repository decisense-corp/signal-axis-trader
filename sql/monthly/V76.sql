-- ============================================================================
-- V76_cream_patterns_sell（スコア版）
-- 優良パターンの厳選
-- 
-- 厳選基準（V74と同じ）：
-- - 損切率10%以下
-- - 学習期間利益率 0.25%～0.45%（SELLは少し緩め）
-- - シャープレシオ 0.1以上
-- - 検証期間で利益がプラス
-- - 最低取引数50回以上
-- ============================================================================

CREATE OR REPLACE VIEW `kabu-376213.kabu2411.V76_cream_patterns_sell` AS
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
  FROM `kabu-376213.kabu2411.V75_two_axis_validation_results_sell` v
  WHERE 
    -- 基本条件（SELLの成功基準を適用）
    v.learning_stop_loss_rate <= 10  -- 損切率10%以下
    AND v.learning_avg_profit_rate BETWEEN 0.25 AND 0.45  -- 学習期間の利益率（SELLは少し緩め）
    AND v.learning_sharpe_ratio > 0.1  -- シャープレシオ
    AND v.validation_avg_profit_rate > 0  -- 検証期間でプラス
    AND v.validation_trades >= 50  -- 検証期間で十分な取引数
)
SELECT 
  score_type_1,
  score_bin_1,
  score_type_2,
  score_bin_2,
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
  'SELL' as trade_type
FROM cream_candidates
ORDER BY composite_score DESC;

-- ============================================================================
-- 実行後の確認クエリ
-- ============================================================================

-- 厳選結果サマリー
SELECT 
  COUNT(*) as selected_patterns,
  ROUND(AVG(validation_avg_profit_rate), 4) as avg_validation_profit,
  ROUND(AVG(learning_avg_profit_rate), 4) as avg_learning_profit,
  ROUND(AVG(composite_score), 4) as avg_composite_score,
  ROUND(AVG(stability_score), 3) as avg_stability_score
FROM `kabu-376213.kabu2411.V76_cream_patterns_sell`;

-- TOP10パターン
SELECT 
  pattern_key,
  validation_avg_profit_rate,
  learning_avg_profit_rate,
  profit_degradation_pct,
  composite_score,
  stability_score,
  rank
FROM `kabu-376213.kabu2411.V76_cream_patterns_sell`
WHERE rank <= 10
ORDER BY rank;

-- スコアタイプ別の出現頻度
SELECT 
  score_type,
  COUNT(*) as pattern_count,
  ROUND(AVG(validation_avg_profit_rate), 4) as avg_profit
FROM (
  SELECT score_type_1 as score_type, validation_avg_profit_rate 
  FROM `kabu-376213.kabu2411.V76_cream_patterns_sell`
  UNION ALL
  SELECT score_type_2 as score_type, validation_avg_profit_rate 
  FROM `kabu-376213.kabu2411.V76_cream_patterns_sell`
)
GROUP BY score_type
ORDER BY pattern_count DESC;

-- BUY/SELL厳選パターン数の比較
SELECT 
  'BUY' as trade_type,
  COUNT(*) as pattern_count,
  ROUND(AVG(validation_avg_profit_rate), 4) as avg_profit,
  ROUND(AVG(composite_score), 4) as avg_score
FROM `kabu-376213.kabu2411.V66_cream_patterns_buy`
UNION ALL
SELECT 
  'SELL' as trade_type,
  COUNT(*) as pattern_count,
  ROUND(AVG(validation_avg_profit_rate), 4) as avg_profit,
  ROUND(AVG(composite_score), 4) as avg_score
FROM `kabu-376213.kabu2411.V76_cream_patterns_sell`
ORDER BY trade_type;