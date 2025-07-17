-- ============================================================================
-- D72_two_axis_patterns_optimized_sell
-- 2軸パターンの損切・利確幅最適化（SELL版）
-- 
-- 期間設定（ここを変更すれば全体に反映）
-- ============================================================================
DECLARE learning_start_date DATE DEFAULT '2022-06-01';
DECLARE learning_end_date DATE DEFAULT '2025-05-31';

-- ============================================================================
-- テーブル作成
-- ============================================================================

-- 既存テーブル削除
DROP TABLE IF EXISTS `kabu-376213.kabu2411.D72_two_axis_patterns_optimized_sell`;

-- 新規作成
CREATE TABLE `kabu-376213.kabu2411.D72_two_axis_patterns_optimized_sell` AS
WITH 
-- 1. 候補パターンの取得
candidate_patterns AS (
  SELECT * 
  FROM `kabu-376213.kabu2411.D71_two_axis_candidate_patterns_sell`
  WHERE learning_start = learning_start_date
    AND learning_end = learning_end_date
),

-- 2. 各パターンの取引データを準備
pattern_trades AS (
  SELECT 
    cp.pattern_key,
    cp.signal_type_1,
    cp.signal_bin_1,
    cp.signal_type_2,
    cp.signal_bin_2,
    d.signal_date,
    d.stock_code,
    d.day_open,
    d.day_high,
    d.day_low,
    d.day_close
  FROM candidate_patterns cp
  CROSS JOIN `kabu-376213.kabu2411.D10_trading_signals` d
  WHERE 
    -- 学習期間のみ
    d.signal_date BETWEEN learning_start_date AND learning_end_date
    AND d.trade_type = 'SELL'
    AND d.day_open > 0
    -- 両方の条件を満たす銘柄を抽出
    AND EXISTS (
      SELECT 1 FROM `kabu-376213.kabu2411.D10_trading_signals` d1
      WHERE d1.signal_date = d.signal_date
        AND d1.stock_code = d.stock_code
        AND d1.trade_type = 'SELL'
        AND d1.signal_type = cp.signal_type_1
        AND d1.signal_bin = cp.signal_bin_1
    )
    AND EXISTS (
      SELECT 1 FROM `kabu-376213.kabu2411.D10_trading_signals` d2
      WHERE d2.signal_date = d.signal_date
        AND d2.stock_code = d.stock_code
        AND d2.trade_type = 'SELL'
        AND d2.signal_type = cp.signal_type_2
        AND d2.signal_bin = cp.signal_bin_2
    )
),

-- 3. パラメータグリッドの生成
param_grid AS (
  SELECT 
    stop_loss_margin_int / 10.0 as stop_loss_margin,
    profit_margin_int / 100.0 as profit_margin
  FROM 
    UNNEST(GENERATE_ARRAY(10, 50, 1)) as stop_loss_margin_int,  -- 1.0%～5.0%（0.1%刻み）
    UNNEST(GENERATE_ARRAY(250, 1000, 25)) as profit_margin_int  -- 2.5%～10.0%（0.25%刻み）
),

-- 4. 全パラメータでのシミュレーション（SELL版）
simulation_results AS (
  SELECT 
    pt.pattern_key,
    pt.signal_type_1,
    pt.signal_bin_1,
    pt.signal_type_2,
    pt.signal_bin_2,
    pg.stop_loss_margin,
    pg.profit_margin,
    COUNT(*) as total_trades,
    
    -- SELLの場合の利確・損切・引け決済のカウント
    COUNT(CASE 
      -- SELLの損切判定（高値が損切ラインを超えた場合）
      WHEN (pt.day_high - pt.day_open) / pt.day_open * 100 >= pg.stop_loss_margin THEN 1 
    END) as stop_loss_count,
    
    COUNT(CASE 
      -- 損切に到達せず、利確に到達した場合（安値が利確ラインを下回った場合）
      WHEN (pt.day_high - pt.day_open) / pt.day_open * 100 < pg.stop_loss_margin
       AND (pt.day_low - pt.day_open) / pt.day_open * 100 <= -pg.profit_margin THEN 1 
    END) as take_profit_count,
    
    COUNT(CASE 
      -- どちらにも到達しなかった場合
      WHEN (pt.day_high - pt.day_open) / pt.day_open * 100 < pg.stop_loss_margin
       AND (pt.day_low - pt.day_open) / pt.day_open * 100 > -pg.profit_margin THEN 1 
    END) as close_count,
    
    -- SELL版の平均利益率（損切優先判定）
    ROUND(AVG(
      CASE
        -- SELLの損切判定を優先（高値が上昇）
        WHEN (pt.day_high - pt.day_open) / pt.day_open * 100 >= pg.stop_loss_margin 
          THEN -pg.stop_loss_margin
        -- SELLの利確判定（安値が下落）
        WHEN (pt.day_low - pt.day_open) / pt.day_open * 100 <= -pg.profit_margin 
          THEN pg.profit_margin
        -- 引け決済（SELLの場合：始値-終値）
        ELSE (pt.day_open - pt.day_close) / pt.day_open * 100
      END
    ), 4) as avg_profit_rate,
    
    -- 標準偏差
    ROUND(STDDEV(
      CASE
        WHEN (pt.day_high - pt.day_open) / pt.day_open * 100 >= pg.stop_loss_margin 
          THEN -pg.stop_loss_margin
        WHEN (pt.day_low - pt.day_open) / pt.day_open * 100 <= -pg.profit_margin 
          THEN pg.profit_margin
        ELSE (pt.day_open - pt.day_close) / pt.day_open * 100
      END
    ), 4) as profit_stddev
    
  FROM pattern_trades pt
  CROSS JOIN param_grid pg
  GROUP BY 1, 2, 3, 4, 5, 6, 7
),

-- 5. 各パターンで最適なパラメータを選択
best_params AS (
  SELECT 
    *,
    -- 勝率計算（シンプルな方法に修正）
    ROUND(100.0 * (take_profit_count + 
      CASE 
        WHEN avg_profit_rate > 0 AND close_count > 0
        THEN close_count * avg_profit_rate / (avg_profit_rate + ABS(stop_loss_margin))
        ELSE 0 
      END) / total_trades, 2) as win_rate,
    -- シャープレシオ
    ROUND(SAFE_DIVIDE(avg_profit_rate, profit_stddev), 3) as sharpe_ratio,
    -- リスクリワード比
    ROUND(profit_margin / stop_loss_margin, 2) as risk_reward_ratio,
    -- 損切率
    ROUND(100.0 * stop_loss_count / total_trades, 2) as stop_loss_rate,
    -- ランキング（平均利益率ベース）
    ROW_NUMBER() OVER (
      PARTITION BY pattern_key 
      ORDER BY avg_profit_rate DESC
    ) as rank
  FROM simulation_results
  WHERE total_trades >= 300  -- 最低サンプル数
)

-- 6. 最終結果の出力（各パターンの最適パラメータのみ）
SELECT 
  signal_type_1,
  signal_bin_1,
  signal_type_2,
  signal_bin_2,
  pattern_key,
  stop_loss_margin as optimal_stop_loss_margin,
  profit_margin as optimal_profit_margin,
  total_trades,
  stop_loss_count,
  take_profit_count,
  close_count,
  avg_profit_rate,
  profit_stddev,
  win_rate,
  sharpe_ratio,
  risk_reward_ratio,
  stop_loss_rate,
  ROUND(100.0 * take_profit_count / total_trades, 2) as take_profit_rate,
  ROUND(100.0 * close_count / total_trades, 2) as close_rate,
  'SELL' as trade_type,
  learning_start_date as learning_start,
  learning_end_date as learning_end,
  CURRENT_TIMESTAMP() as optimized_at
FROM best_params
WHERE rank = 1
ORDER BY avg_profit_rate DESC;

-- ============================================================================
-- 実行後の確認クエリ
-- ============================================================================

-- 処理結果サマリー
SELECT 
  '📊 最適化結果サマリー' as summary_type,
  COUNT(*) as total_patterns,
  ROUND(AVG(optimal_stop_loss_margin), 2) as avg_stop_loss,
  ROUND(AVG(optimal_profit_margin), 2) as avg_profit_margin,
  ROUND(AVG(avg_profit_rate), 4) as avg_profit_rate,
  ROUND(AVG(win_rate), 2) as avg_win_rate,
  ROUND(AVG(sharpe_ratio), 3) as avg_sharpe_ratio,
  ROUND(AVG(stop_loss_rate), 2) as avg_stop_loss_rate
FROM `kabu-376213.kabu2411.D72_two_axis_patterns_optimized_sell`;

-- 損切幅の分布
SELECT 
  optimal_stop_loss_margin,
  COUNT(*) as pattern_count,
  ROUND(AVG(avg_profit_rate), 4) as avg_profit_rate,
  ROUND(AVG(win_rate), 2) as avg_win_rate
FROM `kabu-376213.kabu2411.D72_two_axis_patterns_optimized_sell`
GROUP BY optimal_stop_loss_margin
ORDER BY pattern_count DESC
LIMIT 10;

-- TOP10パターン
SELECT 
  pattern_key,
  optimal_stop_loss_margin,
  optimal_profit_margin,
  avg_profit_rate,
  win_rate,
  sharpe_ratio,
  stop_loss_rate,
  take_profit_rate,
  close_rate
FROM `kabu-376213.kabu2411.D72_two_axis_patterns_optimized_sell`
ORDER BY avg_profit_rate DESC
LIMIT 10;