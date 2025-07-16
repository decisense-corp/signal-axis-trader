-- ============================================================================
-- 🏛️ D30_optimized_parameters_sharpe - シャープレシオ最適化版
-- 新条件：サンプル30以上、勝率55%以上、平均利益率1%以上
-- 最適化対象：シャープレシオ（リスク調整後リターン）
-- ============================================================================

-- Step 1: 既存テーブルをバックアップ（念のため）
CREATE OR REPLACE TABLE `kabu-376213.kabu2411.D30_optimized_parameters_backup` AS
SELECT * FROM `kabu-376213.kabu2411.D30_optimized_parameters`;

-- Step 2: 新しい条件で最適化
CREATE OR REPLACE TABLE `kabu-376213.kabu2411.D30_optimized_parameters`
(
  stock_code STRING NOT NULL,
  signal_type STRING NOT NULL,
  signal_bin INT64 NOT NULL,
  trade_type STRING NOT NULL,
  optimal_profit_margin FLOAT64,
  optimal_stop_loss_margin FLOAT64,
  total_trades INT64,
  win_rate FLOAT64,
  avg_profit_rate FLOAT64,
  std_deviation FLOAT64,
  sharpe_ratio FLOAT64,
  risk_reward_ratio FLOAT64,
  profit_improvement FLOAT64,
  optimization_score FLOAT64,
  created_at TIMESTAMP
)
PARTITION BY DATE(created_at)
CLUSTER BY stock_code, signal_type, trade_type;

-- Step 3: データ投入
INSERT INTO `kabu-376213.kabu2411.D30_optimized_parameters`
WITH 
-- 新条件でターゲットパターンを抽出
target_patterns AS (
  SELECT DISTINCT
    stock_code,
    signal_type,
    signal_bin,
    trade_type,
    total_samples,
    win_rate,
    avg_profit_rate
  FROM `kabu-376213.kabu2411.D20_tomorrow_signals`
  WHERE target_date = (
    SELECT MAX(target_date) 
    FROM `kabu-376213.kabu2411.D20_tomorrow_signals`
  )
    -- 🆕 新しい絞り込み条件
    AND total_samples >= 30        -- サンプル30以上（20→30）
    AND win_rate >= 55            -- 勝率55%以上（65%→55%）
    AND avg_profit_rate >= 1.0    -- 平均利益率1%以上（0.5%→1.0%）
),

-- 条件を満たすパターンの過去データ取得
historical_trades AS (
  SELECT 
    d10.stock_code,
    d10.signal_type,
    d10.signal_bin,
    d10.trade_type,
    d10.signal_date,
    d10.day_open,
    d10.day_high,
    d10.day_low,
    d10.day_close,
    d10.baseline_profit_rate
  FROM `kabu-376213.kabu2411.D10_trading_signals` d10
  INNER JOIN target_patterns tp
    ON d10.stock_code = tp.stock_code
    AND d10.signal_type = tp.signal_type
    AND d10.signal_bin = tp.signal_bin
    AND d10.trade_type = tp.trade_type
  WHERE d10.day_open > 0
),

-- パラメータグリッドでのシミュレーション
simulation_base AS (
  SELECT
    ht.*,
    profit_margin_int,
    stop_loss_margin_int
  FROM historical_trades ht
  CROSS JOIN 
    UNNEST(GENERATE_ARRAY(25, 100, 5)) AS profit_margin_int  -- 2.5%〜10.0%（0.5%刻み）
  CROSS JOIN
    UNNEST(GENERATE_ARRAY(10, 50, 2)) AS stop_loss_margin_int  -- 1.0%〜5.0%（0.2%刻み）
),

-- シミュレーション結果の計算
simulation_results AS (
  SELECT
    stock_code,
    signal_type,
    signal_bin,
    trade_type,
    profit_margin_int,
    stop_loss_margin_int,
    COUNT(*) AS total_trades,
    
    -- 勝敗判定
    SUM(CASE 
      WHEN trade_type = 'BUY' THEN
        CASE
          WHEN day_low <= day_open * (1 - stop_loss_margin_int / 1000.0) THEN 0
          WHEN day_high >= day_open * (1 + profit_margin_int / 1000.0) THEN 1
          WHEN day_close >= day_open THEN 1
          ELSE 0
        END
      ELSE -- SELL
        CASE
          WHEN day_high >= day_open * (1 + stop_loss_margin_int / 1000.0) THEN 0
          WHEN day_low <= day_open * (1 - profit_margin_int / 1000.0) THEN 1
          WHEN day_open >= day_close THEN 1
          ELSE 0
        END
    END) AS win_count,
    
    -- 平均利益率
    ROUND(AVG(CASE 
      WHEN trade_type = 'BUY' THEN
        CASE
          WHEN day_low <= day_open * (1 - stop_loss_margin_int / 1000.0) 
            THEN -(stop_loss_margin_int / 10.0)
          WHEN day_high >= day_open * (1 + profit_margin_int / 1000.0) 
            THEN profit_margin_int / 10.0
          ELSE (day_close - day_open) / day_open * 100
        END
      ELSE -- SELL
        CASE
          WHEN day_high >= day_open * (1 + stop_loss_margin_int / 1000.0) 
            THEN -(stop_loss_margin_int / 10.0)
          WHEN day_low <= day_open * (1 - profit_margin_int / 1000.0) 
            THEN profit_margin_int / 10.0
          ELSE (day_open - day_close) / day_open * 100
        END
    END), 3) AS avg_profit_rate,
    
    -- 標準偏差
    ROUND(STDDEV(CASE 
      WHEN trade_type = 'BUY' THEN
        CASE
          WHEN day_low <= day_open * (1 - stop_loss_margin_int / 1000.0) 
            THEN -(stop_loss_margin_int / 10.0)
          WHEN day_high >= day_open * (1 + profit_margin_int / 1000.0) 
            THEN profit_margin_int / 10.0
          ELSE (day_close - day_open) / day_open * 100
        END
      ELSE -- SELL
        CASE
          WHEN day_high >= day_open * (1 + stop_loss_margin_int / 1000.0) 
            THEN -(stop_loss_margin_int / 10.0)
          WHEN day_low <= day_open * (1 - profit_margin_int / 1000.0) 
            THEN profit_margin_int / 10.0
          ELSE (day_open - day_close) / day_open * 100
        END
    END), 3) AS std_deviation
    
  FROM simulation_base
  GROUP BY 
    stock_code, signal_type, signal_bin, trade_type,
    profit_margin_int, stop_loss_margin_int
),

-- 各パターンで最適なパラメータを選択
ranked_results AS (
  SELECT 
    *,
    ROUND(win_count * 100.0 / total_trades, 2) AS win_rate,
    -- シャープレシオ
    ROUND(SAFE_DIVIDE(avg_profit_rate, NULLIF(std_deviation, 0)), 3) AS sharpe_ratio,
    -- リスクリワード比
    ROUND(profit_margin_int * 1.0 / stop_loss_margin_int, 2) AS risk_reward_ratio
  FROM simulation_results
  WHERE total_trades >= 15  -- 最低15件のデータ
),
final_ranked AS (
  SELECT 
    *,
    -- 🆕 シャープレシオを最適化スコアに
    sharpe_ratio AS optimization_score,
    -- 🆕 シャープレシオ順でランキング
    ROW_NUMBER() OVER (
      PARTITION BY stock_code, signal_type, signal_bin, trade_type
      ORDER BY sharpe_ratio DESC, avg_profit_rate DESC, win_rate DESC
    ) AS rank
  FROM ranked_results
  WHERE sharpe_ratio > 0  -- シャープレシオが正の値のみ
)

-- 最終結果
SELECT
  stock_code,
  signal_type,
  signal_bin,
  trade_type,
  profit_margin_int / 10.0 AS optimal_profit_margin,
  stop_loss_margin_int / 10.0 AS optimal_stop_loss_margin,
  total_trades,
  win_rate,
  avg_profit_rate,
  std_deviation,
  sharpe_ratio,
  risk_reward_ratio,
  CAST(NULL AS FLOAT64) AS profit_improvement,
  optimization_score,
  CURRENT_TIMESTAMP() AS created_at
FROM final_ranked
WHERE rank = 1;

-- ============================================================================
-- 結果確認クエリ
-- ============================================================================

-- 新旧条件の比較
WITH old_stats AS (
  SELECT 
    COUNT(*) as pattern_count,
    ROUND(AVG(win_rate), 1) as avg_win_rate,
    ROUND(AVG(avg_profit_rate), 2) as avg_profit_rate,
    ROUND(AVG(sharpe_ratio), 2) as avg_sharpe_ratio
  FROM `kabu-376213.kabu2411.D30_optimized_parameters_backup`
),
new_stats AS (
  SELECT 
    COUNT(*) as pattern_count,
    ROUND(AVG(win_rate), 1) as avg_win_rate,
    ROUND(AVG(avg_profit_rate), 2) as avg_profit_rate,
    ROUND(AVG(sharpe_ratio), 2) as avg_sharpe_ratio
  FROM `kabu-376213.kabu2411.D30_optimized_parameters`
)
SELECT 
  '📊 最適化条件比較' as analysis,
  '旧条件（利益率重視）' as type,
  old_stats.* 
FROM old_stats
UNION ALL
SELECT 
  '📊 最適化条件比較' as analysis,
  '新条件（シャープレシオ重視）' as type,
  new_stats.* 
FROM new_stats;

-- シャープレシオTOP20
SELECT 
  '🏆 シャープレシオTOP20' as ranking,
  stock_code,
  stock_name,
  signal_type,
  trade_type,
  ROUND(sharpe_ratio, 3) as sharpe_ratio,
  ROUND(avg_profit_rate, 2) as avg_profit_rate,
  ROUND(win_rate, 1) as win_rate,
  optimal_profit_margin,
  optimal_stop_loss_margin,
  risk_reward_ratio
FROM `kabu-376213.kabu2411.D30_optimized_parameters` d30
LEFT JOIN (
  SELECT DISTINCT stock_code, stock_name 
  FROM `kabu-376213.kabu2411.D20_tomorrow_signals`
) stock_names USING(stock_code)
ORDER BY sharpe_ratio DESC
LIMIT 20;

-- パラメータ分布の変化
SELECT 
  '📈 最適パラメータ分布' as analysis,
  trade_type,
  COUNT(*) as pattern_count,
  ROUND(AVG(optimal_profit_margin), 2) as avg_profit_margin,
  ROUND(AVG(optimal_stop_loss_margin), 2) as avg_stop_loss_margin,
  ROUND(AVG(risk_reward_ratio), 2) as avg_risk_reward,
  ROUND(AVG(sharpe_ratio), 3) as avg_sharpe_ratio
FROM `kabu-376213.kabu2411.D30_optimized_parameters`
GROUP BY trade_type;