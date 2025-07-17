/*
ファイル: create_D40_tomorrow_scores.sql
説明: D40_tomorrow_scores テーブル作成と日次データ投入
作成日: 2025年1月17日
目的: 明日の取引スコアと全期間統計情報を統合（D20のスコア版）
特徴:
  - D20_tomorrow_signalsからデータ取得してスコア計算
  - D30_trading_scoresから全期間統計を集計
  - 流動性情報を含む
実行時間: 約5-10分
*/

-- ============================================================================
-- Part 1: テーブル作成
-- ============================================================================

-- 処理開始メッセージ
SELECT 
  '🚀 D40_tomorrow_scores テーブル作成開始' as message,
  '特徴: スコアベース明日予測 + 全期間統計' as features,
  'データソース: D20_tomorrow_signals + D30_trading_scores' as data_source,
  CURRENT_TIMESTAMP() as start_time;

-- 既存テーブル削除（存在する場合）
DROP TABLE IF EXISTS `kabu-376213.kabu2411.D40_tomorrow_scores`;

-- 新テーブル作成（D20と同じ構造）
CREATE TABLE `kabu-376213.kabu2411.D40_tomorrow_scores` (
  -- 基本情報
  target_date DATE NOT NULL,             -- 取引予定日（明日）
  score_type STRING NOT NULL,            -- スコア種別（28種類）
  score_bin INT64 NOT NULL,              -- スコア分位（1-20）
  trade_type STRING NOT NULL,            -- 取引種別（'BUY'/'SELL'）
  stock_code STRING NOT NULL,            -- 銘柄コード
  stock_name STRING,                     -- 銘柄名
  score_value FLOAT64,                   -- スコア値
  
  -- 流動性情報
  prev_close FLOAT64,                    -- 前日終値
  prev_volume FLOAT64,                   -- 前営業日の出来高
  prev_trading_value FLOAT64,            -- 前営業日の売買代金
  tradable_shares INT64,                 -- 売買可能株数
  
  -- 全期間統計情報
  total_samples INT64,                   -- 総サンプル数
  win_samples INT64,                     -- 勝ちサンプル数
  win_rate FLOAT64,                      -- 勝率（%）
  avg_profit_rate FLOAT64,               -- 平均利益率（%）
  std_deviation FLOAT64,                 -- 標準偏差
  sharpe_ratio FLOAT64,                  -- シャープレシオ
  max_profit_rate FLOAT64,               -- 最大利益率
  min_profit_rate FLOAT64,               -- 最小利益率
  first_signal_date DATE,                -- 初回シグナル日
  last_signal_date DATE,                 -- 最終シグナル日
  
  -- システム項目
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY target_date
CLUSTER BY stock_code, trade_type;

SELECT 
  '✅ テーブル作成完了' as status,
  '次: 明日スコアデータ投入' as next_step;

-- ============================================================================
-- Part 2: 日次データ投入（全件削除→再作成）
-- ============================================================================

-- 既存データ全削除
DELETE FROM `kabu-376213.kabu2411.D40_tomorrow_scores` WHERE TRUE;

-- データ投入開始
INSERT INTO `kabu-376213.kabu2411.D40_tomorrow_scores`
WITH 
-- 1. 最新のtarget_dateを取得
latest_target AS (
  SELECT MAX(target_date) as target_date
  FROM `kabu-376213.kabu2411.D20_tomorrow_signals`
),

-- 2. D20の最新日データからスコア計算
score_calculation AS (
  SELECT 
    lt.target_date,
    d.stock_code,
    d.stock_name,
    d.signal_type,
    d.signal_bin,
    d.trade_type,
    
    -- 流動性情報
    d.prev_close,
    d.prev_volume,
    d.prev_trading_value,
    d.tradable_shares,
    
    -- 各指標の係数を取得
    c.coef_h3p, c.coef_h1p, c.coef_l3p, c.coef_l1p,
    c.coef_cu3p, c.coef_cu1p, c.coef_cd3p, c.coef_cd1p,
    c.coef_ud75p, c.coef_dd75p, c.coef_uc3p, c.coef_dc3p,
    c.coef_direction, c.coef_vol3p, c.coef_vol5p
    
  FROM `kabu-376213.kabu2411.D20_tomorrow_signals` d
  JOIN `kabu-376213.kabu2411.D81_signal_coefficients_8indicators` c
    ON d.signal_type = c.signal_type 
    AND d.signal_bin = c.signal_bin
    AND d.trade_type = c.trade_type
  CROSS JOIN latest_target lt
  WHERE d.target_date = lt.target_date
),

-- 3. 各スコアタイプの対数和を計算
log_scores AS (
  SELECT 
    target_date,
    stock_code,
    ANY_VALUE(stock_name) as stock_name,
    trade_type,
    ANY_VALUE(prev_close) as prev_close,
    ANY_VALUE(prev_volume) as prev_volume,
    ANY_VALUE(prev_trading_value) as prev_trading_value,
    ANY_VALUE(tradable_shares) as tradable_shares,
    
    -- 各スコアの対数和（37シグナルの係数の積）
    SUM(LN(GREATEST(coef_h3p, 0.01))) as log_score_h3p,
    SUM(LN(GREATEST(coef_h1p, 0.01))) as log_score_h1p,
    SUM(LN(GREATEST(coef_l3p, 0.01))) as log_score_l3p,
    SUM(LN(GREATEST(coef_l1p, 0.01))) as log_score_l1p,
    SUM(LN(GREATEST(coef_cu3p, 0.01))) as log_score_cu3p,
    SUM(LN(GREATEST(coef_cu1p, 0.01))) as log_score_cu1p,
    SUM(LN(GREATEST(coef_cd3p, 0.01))) as log_score_cd3p,
    SUM(LN(GREATEST(coef_cd1p, 0.01))) as log_score_cd1p,
    SUM(LN(GREATEST(coef_ud75p, 0.01))) as log_score_ud75p,
    SUM(LN(GREATEST(coef_dd75p, 0.01))) as log_score_dd75p,
    SUM(LN(GREATEST(coef_uc3p, 0.01))) as log_score_uc3p,
    SUM(LN(GREATEST(coef_dc3p, 0.01))) as log_score_dc3p,
    SUM(LN(GREATEST(coef_direction, 0.01))) as log_score_direction,
    SUM(LN(GREATEST(coef_vol3p, 0.01))) as log_score_vol3p,
    SUM(LN(GREATEST(coef_vol5p, 0.01))) as log_score_vol5p
  FROM score_calculation
  GROUP BY target_date, stock_code, trade_type
),

-- 4. UNPIVOT形式に変換
unpivoted_scores AS (
  -- BUY側スコア
  SELECT target_date, stock_code, stock_name, 'H3P_BUY' as score_type, log_score_h3p as score_value, prev_close, prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores WHERE trade_type = 'BUY'
  UNION ALL
  SELECT target_date, stock_code, stock_name, 'H1P_BUY', log_score_h1p, prev_close, prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores WHERE trade_type = 'BUY'
  UNION ALL
  SELECT target_date, stock_code, stock_name, 'L3P_BUY', log_score_l3p, prev_close, prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores WHERE trade_type = 'BUY'
  UNION ALL
  SELECT target_date, stock_code, stock_name, 'L1P_BUY', log_score_l1p, prev_close, prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores WHERE trade_type = 'BUY'
  UNION ALL
  SELECT target_date, stock_code, stock_name, 'CU3P_BUY', log_score_cu3p, prev_close, prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores WHERE trade_type = 'BUY'
  UNION ALL
  SELECT target_date, stock_code, stock_name, 'CU1P_BUY', log_score_cu1p, prev_close, prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores WHERE trade_type = 'BUY'
  UNION ALL
  SELECT target_date, stock_code, stock_name, 'CD3P_BUY', log_score_cd3p, prev_close, prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores WHERE trade_type = 'BUY'
  UNION ALL
  SELECT target_date, stock_code, stock_name, 'CD1P_BUY', log_score_cd1p, prev_close, prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores WHERE trade_type = 'BUY'
  UNION ALL
  SELECT target_date, stock_code, stock_name, 'UD75P_BUY', log_score_ud75p, prev_close, prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores WHERE trade_type = 'BUY'
  UNION ALL
  SELECT target_date, stock_code, stock_name, 'DD75P_BUY', log_score_dd75p, prev_close, prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores WHERE trade_type = 'BUY'
  UNION ALL
  SELECT target_date, stock_code, stock_name, 'UC3P_BUY', log_score_uc3p, prev_close, prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores WHERE trade_type = 'BUY'
  UNION ALL
  SELECT target_date, stock_code, stock_name, 'DC3P_BUY', log_score_dc3p, prev_close, prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores WHERE trade_type = 'BUY'
  UNION ALL
  SELECT target_date, stock_code, stock_name, 'DIRECTION_BUY', log_score_direction, prev_close, prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores WHERE trade_type = 'BUY'
  
  -- SELL側スコア
  UNION ALL
  SELECT target_date, stock_code, stock_name, 'H3P_SELL', log_score_h3p, prev_close, prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores WHERE trade_type = 'SELL'
  UNION ALL
  SELECT target_date, stock_code, stock_name, 'H1P_SELL', log_score_h1p, prev_close, prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores WHERE trade_type = 'SELL'
  UNION ALL
  SELECT target_date, stock_code, stock_name, 'L3P_SELL', log_score_l3p, prev_close, prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores WHERE trade_type = 'SELL'
  UNION ALL
  SELECT target_date, stock_code, stock_name, 'L1P_SELL', log_score_l1p, prev_close, prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores WHERE trade_type = 'SELL'
  UNION ALL
  SELECT target_date, stock_code, stock_name, 'CU3P_SELL', log_score_cu3p, prev_close, prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores WHERE trade_type = 'SELL'
  UNION ALL
  SELECT target_date, stock_code, stock_name, 'CU1P_SELL', log_score_cu1p, prev_close, prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores WHERE trade_type = 'SELL'
  UNION ALL
  SELECT target_date, stock_code, stock_name, 'CD3P_SELL', log_score_cd3p, prev_close, prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores WHERE trade_type = 'SELL'
  UNION ALL
  SELECT target_date, stock_code, stock_name, 'CD1P_SELL', log_score_cd1p, prev_close, prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores WHERE trade_type = 'SELL'
  UNION ALL
  SELECT target_date, stock_code, stock_name, 'UD75P_SELL', log_score_ud75p, prev_close, prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores WHERE trade_type = 'SELL'
  UNION ALL
  SELECT target_date, stock_code, stock_name, 'DD75P_SELL', log_score_dd75p, prev_close, prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores WHERE trade_type = 'SELL'
  UNION ALL
  SELECT target_date, stock_code, stock_name, 'UC3P_SELL', log_score_uc3p, prev_close, prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores WHERE trade_type = 'SELL'
  UNION ALL
  SELECT target_date, stock_code, stock_name, 'DC3P_SELL', log_score_dc3p, prev_close, prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores WHERE trade_type = 'SELL'
  UNION ALL
  SELECT target_date, stock_code, stock_name, 'DIRECTION_SELL', log_score_direction, prev_close, prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores WHERE trade_type = 'SELL'
  
  -- ボラティリティ（BUY/SELL共通）
  UNION ALL
  SELECT target_date, stock_code, stock_name, 'VOL3P', log_score_vol3p, prev_close, prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores WHERE trade_type = 'BUY'
  UNION ALL
  SELECT target_date, stock_code, stock_name, 'VOL5P', log_score_vol5p, prev_close, prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores WHERE trade_type = 'BUY'
),

-- 5. スコアbinを計算
scores_with_bins AS (
  SELECT 
    s.*,
    -- M20_score_binsからbinを決定
    COALESCE(
      (SELECT MAX(sb.score_bin) 
       FROM `kabu-376213.kabu2411.M20_score_bins` sb
       WHERE sb.score_type = s.score_type
         AND s.score_value > sb.lower_bound 
         AND s.score_value <= sb.upper_bound), 
      1
    ) as score_bin
  FROM unpivoted_scores s
),

-- 6. D30から全期間統計を取得
all_time_statistics AS (
  SELECT 
    score_type,
    score_bin,
    trade_type,
    stock_code,
    COUNT(*) as total_samples,
    SUM(CASE WHEN is_win THEN 1 ELSE 0 END) as win_samples,
    AVG(CASE WHEN is_win THEN 1.0 ELSE 0.0 END) * 100 as win_rate,
    AVG(baseline_profit_rate) as avg_profit_rate,
    STDDEV(baseline_profit_rate) as std_deviation,
    SAFE_DIVIDE(
      AVG(baseline_profit_rate), 
      NULLIF(STDDEV(baseline_profit_rate), 0)
    ) as sharpe_ratio,
    MAX(baseline_profit_rate) as max_profit_rate,
    MIN(baseline_profit_rate) as min_profit_rate,
    MIN(signal_date) as first_signal_date,
    MAX(signal_date) as last_signal_date
  FROM `kabu-376213.kabu2411.D30_trading_scores`
  GROUP BY score_type, score_bin, trade_type, stock_code
),

-- 7. 最終結果の結合
final_results AS (
  SELECT 
    swb.target_date,
    swb.score_type,
    swb.score_bin,
    tt as trade_type,  -- UNNESTの値を使用
    swb.stock_code,
    swb.stock_name,
    swb.score_value,
    swb.prev_close,
    swb.prev_volume,
    swb.prev_trading_value,
    swb.tradable_shares,
    
    COALESCE(ats.total_samples, 0) as total_samples,
    COALESCE(ats.win_samples, 0) as win_samples,
    COALESCE(ats.win_rate, 0.0) as win_rate,
    COALESCE(ats.avg_profit_rate, 0.0) as avg_profit_rate,
    COALESCE(ats.std_deviation, 0.0) as std_deviation,
    COALESCE(ats.sharpe_ratio, 0.0) as sharpe_ratio,
    COALESCE(ats.max_profit_rate, 0.0) as max_profit_rate,
    COALESCE(ats.min_profit_rate, 0.0) as min_profit_rate,
    ats.first_signal_date,
    ats.last_signal_date,
    
    CURRENT_TIMESTAMP() as created_at
    
  FROM scores_with_bins swb
  CROSS JOIN UNNEST(['BUY', 'SELL']) as tt  -- エイリアスのみ
  LEFT JOIN all_time_statistics ats
    ON swb.score_type = ats.score_type
    AND swb.score_bin = ats.score_bin
    AND tt = ats.trade_type  -- 修正：ttを直接使用
    AND swb.stock_code = ats.stock_code
  WHERE swb.score_bin IS NOT NULL
)

-- 最終SELECT
SELECT 
  target_date,
  score_type,
  score_bin,
  trade_type,
  stock_code,
  stock_name,
  score_value,
  prev_close,
  prev_volume,
  prev_trading_value,
  tradable_shares,
  total_samples,
  win_samples,
  win_rate,
  avg_profit_rate,
  std_deviation,
  sharpe_ratio,
  max_profit_rate,
  min_profit_rate,
  first_signal_date,
  last_signal_date,
  created_at
FROM final_results
ORDER BY 
  avg_profit_rate DESC,
  win_rate DESC,
  stock_code,
  score_type,
  trade_type;

-- ============================================================================
-- Part 3: 投入結果確認
-- ============================================================================

-- 投入完了確認
SELECT 
  '✅ D40_tomorrow_scores データ投入完了' as status,
  COUNT(*) as total_records,
  COUNT(DISTINCT score_type) as score_types_count,
  COUNT(DISTINCT stock_code) as stocks_count,
  MIN(target_date) as target_date,
  CURRENT_TIMESTAMP() as end_time
FROM `kabu-376213.kabu2411.D40_tomorrow_scores`;

-- スコアタイプ別確認
SELECT 
  '📊 スコアタイプ別レコード数' as check_type,
  score_type,
  COUNT(*) as records_count,
  COUNT(DISTINCT stock_code) as stocks_count,
  COUNT(DISTINCT score_bin) as bins_count,
  AVG(total_samples) as avg_samples
FROM `kabu-376213.kabu2411.D40_tomorrow_scores`
GROUP BY score_type
ORDER BY score_type
LIMIT 10;

-- 高パフォーマンススコア確認
SELECT 
  '⭐ 高パフォーマンススコア TOP10' as check_type,
  score_type,
  score_bin,
  trade_type,
  stock_name,
  score_value,
  total_samples,
  ROUND(win_rate, 1) as win_rate_pct,
  ROUND(avg_profit_rate, 2) as avg_profit_pct,
  tradable_shares
FROM `kabu-376213.kabu2411.D40_tomorrow_scores`
WHERE total_samples >= 20  -- 十分なサンプル数
  AND win_rate >= 55       -- 高勝率
  AND avg_profit_rate >= 0.5  -- 高期待値
ORDER BY avg_profit_rate DESC
LIMIT 10;