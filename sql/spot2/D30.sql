-- ============================================================================
-- 🏛️ 古のテクノロジー復活：D30_optimized_parameters
-- 利益確定・ロスカット幅の最適化テーブル
-- ============================================================================
-- 依存テーブル:
--   - D10_trading_signals（全期間の取引実績）
--   - D20_tomorrow_signals（明日のシグナル予定）
-- 
-- 目的：
--   銘柄×シグナル×売買タイプごとに、過去データから
--   最適な利益確定幅とロスカット幅を算出する
-- ============================================================================

-- Step 1: テーブル作成（まず構造を定義）
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

-- Step 2: データ投入
INSERT INTO `kabu-376213.kabu2411.D30_optimized_parameters`
WITH 
-- ターゲットとなる優秀パターンの抽出（D20から条件を満たすもの）
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
    AND total_samples >= 20        -- 最低20回の取引実績
    AND win_rate >= 65            -- 勝率65%以上
    AND avg_profit_rate >= 0.5    -- 平均利益率0.5%以上
),

-- 過去の取引データ取得（高値・安値含む）
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
  WHERE d10.day_open > 0  -- 始値が有効なデータのみ
),

-- パラメータグリッドでのシミュレーション
simulation_base AS (
  SELECT
    ht.*,
    profit_margin_int,
    stop_loss_margin_int
  FROM historical_trades ht
  CROSS JOIN 
    -- 利益確定幅：2.5%〜10.0%（0.25%刻み）
    UNNEST(GENERATE_ARRAY(25, 100, 5)) AS profit_margin_int  -- 5刻み=0.5%刻みで開始
  CROSS JOIN
    -- ロスカット幅：1.0%〜5.0%（0.2%刻み）  
    UNNEST(GENERATE_ARRAY(10, 50, 2)) AS stop_loss_margin_int  -- 2刻み=0.2%刻みで開始
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
    
    -- BUYとSELLで計算ロジックを分ける
    SUM(CASE 
      WHEN trade_type = 'BUY' THEN
        CASE
          -- BUY: ロスカット判定（安値が始値×(1-ロスカット率)以下）
          WHEN day_low <= day_open * (1 - stop_loss_margin_int / 1000.0) THEN 0
          -- BUY: 利益確定判定（高値が始値×(1+利確率)以上）
          WHEN day_high >= day_open * (1 + profit_margin_int / 1000.0) THEN 1
          -- BUY: どちらでもない場合、終値≥始値なら勝ち
          WHEN day_close >= day_open THEN 1
          ELSE 0
        END
      ELSE -- SELL
        CASE
          -- SELL: ロスカット判定（高値が始値×(1+ロスカット率)以上）
          WHEN day_high >= day_open * (1 + stop_loss_margin_int / 1000.0) THEN 0
          -- SELL: 利益確定判定（安値が始値×(1-利確率)以下）
          WHEN day_low <= day_open * (1 - profit_margin_int / 1000.0) THEN 1
          -- SELL: どちらでもない場合、始値≥終値なら勝ち
          WHEN day_open >= day_close THEN 1
          ELSE 0
        END
    END) AS win_count,
    
    -- 平均利益率の計算
    ROUND(AVG(CASE 
      WHEN trade_type = 'BUY' THEN
        CASE
          -- BUY: ロスカット発動
          WHEN day_low <= day_open * (1 - stop_loss_margin_int / 1000.0) 
            THEN -(stop_loss_margin_int / 10.0)
          -- BUY: 利益確定発動
          WHEN day_high >= day_open * (1 + profit_margin_int / 1000.0) 
            THEN profit_margin_int / 10.0
          -- BUY: 終値ベース
          ELSE (day_close - day_open) / day_open * 100
        END
      ELSE -- SELL
        CASE
          -- SELL: ロスカット発動
          WHEN day_high >= day_open * (1 + stop_loss_margin_int / 1000.0) 
            THEN -(stop_loss_margin_int / 10.0)
          -- SELL: 利益確定発動
          WHEN day_low <= day_open * (1 - profit_margin_int / 1000.0) 
            THEN profit_margin_int / 10.0
          -- SELL: 終値ベース
          ELSE (day_open - day_close) / day_open * 100
        END
    END), 3) AS avg_profit_rate,
    
    -- 標準偏差（リスク指標）
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
    -- シャープレシオ（リターン/リスク）
    ROUND(SAFE_DIVIDE(avg_profit_rate, NULLIF(std_deviation, 0)), 3) AS sharpe_ratio,
    -- リスクリワード比
    ROUND(profit_margin_int * 1.0 / stop_loss_margin_int, 2) AS risk_reward_ratio,
    -- 最適化スコア（現在は平均利益率のみ、後で変更可能）
    avg_profit_rate AS optimization_score
  FROM simulation_results
  WHERE total_trades >= 10  -- 最低10件のデータがある組み合わせのみ
),
-- ランキング計算を別のCTEに分離
final_ranked AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY stock_code, signal_type, signal_bin, trade_type
      ORDER BY avg_profit_rate DESC, win_rate DESC, sharpe_ratio DESC
    ) AS rank
  FROM ranked_results
)

-- 最終結果
SELECT
  stock_code,
  signal_type,
  signal_bin,
  trade_type,
  
  -- 最適パラメータ
  profit_margin_int / 10.0 AS optimal_profit_margin,      -- 利益確定幅（%）
  stop_loss_margin_int / 10.0 AS optimal_stop_loss_margin, -- ロスカット幅（%）
  
  -- パフォーマンス指標
  total_trades,
  win_rate,
  avg_profit_rate,
  std_deviation,
  sharpe_ratio,
  risk_reward_ratio,
  
  -- 改善度（ベースラインとの比較用、後で計算追加可能）
  CAST(NULL AS FLOAT64) AS profit_improvement,  -- TODO: baseline_profit_rateとの比較
  
  -- メタデータ
  optimization_score,
  CURRENT_TIMESTAMP() AS created_at

FROM final_ranked
WHERE rank = 1;

-- ============================================================================
-- 実行後の確認クエリ
-- ============================================================================

-- 最適化結果のサマリー
SELECT 
  '📊 D30最適化完了' as status,
  COUNT(*) as total_patterns,
  COUNT(DISTINCT stock_code) as unique_stocks,
  ROUND(AVG(optimal_profit_margin), 2) as avg_profit_margin,
  ROUND(AVG(optimal_stop_loss_margin), 2) as avg_stop_loss_margin,
  ROUND(AVG(win_rate), 1) as avg_win_rate,
  ROUND(AVG(avg_profit_rate), 2) as avg_optimized_profit,
  ROUND(AVG(sharpe_ratio), 2) as avg_sharpe_ratio
FROM `kabu-376213.kabu2411.D30_optimized_parameters`;

-- 売買タイプ別の傾向
SELECT 
  trade_type,
  COUNT(*) as pattern_count,
  ROUND(AVG(optimal_profit_margin), 2) as avg_profit_margin,
  ROUND(AVG(optimal_stop_loss_margin), 2) as avg_stop_loss_margin,
  ROUND(AVG(risk_reward_ratio), 2) as avg_risk_reward
FROM `kabu-376213.kabu2411.D30_optimized_parameters`
GROUP BY trade_type;

-- リスクリワード比の分布
SELECT 
  CASE 
    WHEN risk_reward_ratio < 1 THEN '< 1.0'
    WHEN risk_reward_ratio < 2 THEN '1.0-2.0'
    WHEN risk_reward_ratio < 3 THEN '2.0-3.0'
    ELSE '>= 3.0'
  END as risk_reward_range,
  COUNT(*) as pattern_count,
  ROUND(AVG(win_rate), 1) as avg_win_rate,
  ROUND(AVG(avg_profit_rate), 2) as avg_profit_rate
FROM `kabu-376213.kabu2411.D30_optimized_parameters`
GROUP BY risk_reward_range
ORDER BY risk_reward_range;