/*
ファイル: create_D30_trading_scores_fixed.sql
説明: D30_trading_scores 修正版（15指標×BUY/SELL）
作成日: 2025年1月17日
修正内容:
  - score_typeを15種類に統一（H3P, L3P等）
  - 各スコアに対してBUY/SELL両方の戦略を適用
  - 元のtrade_typeとは独立して処理
実行時間: 約15-20分予想
*/

-- ============================================================================
-- Part 1: データ投入前の確認とクリア
-- ============================================================================

-- 処理開始メッセージ
SELECT 
  '🚀 D30_trading_scores 修正版投入開始' as message,
  'スコアベース取引シグナル（15種類×BUY/SELL）' as feature,
  '対象期間: 2022-07-01 〜 最新（全期間）' as target_period,
  CURRENT_TIMESTAMP() as start_time;

-- 既存データをクリア
DELETE FROM `kabu-376213.kabu2411.D30_trading_scores` WHERE TRUE;

-- ============================================================================
-- Part 2: 全期間データ投入（15指標版）
-- ============================================================================

-- データ投入
INSERT INTO `kabu-376213.kabu2411.D30_trading_scores`
WITH 
-- 1. D10とD81から全期間のスコアを計算
score_calculation AS (
  SELECT 
    d.signal_date,
    d.stock_code,
    d.stock_name,
    d.signal_type,
    d.signal_bin,
    d.trade_type as original_trade_type,  -- 元の取引種別を保持
    
    -- 価格データ
    d.prev_close,
    d.day_open,
    d.day_high,
    d.day_low,
    d.day_close,
    d.prev_close_to_open_gap,
    d.open_to_high_gap,
    d.open_to_low_gap,
    d.open_to_close_gap,
    d.daily_range,
    d.baseline_profit_rate,
    d.is_win,
    d.trading_volume,
    d.prev_volume,
    d.prev_trading_value,
    d.tradable_shares,
    
    -- 各指標の係数を取得
    c.coef_h3p, c.coef_h1p, c.coef_l3p, c.coef_l1p,
    c.coef_cu3p, c.coef_cu1p, c.coef_cd3p, c.coef_cd1p,
    c.coef_ud75p, c.coef_dd75p, c.coef_uc3p, c.coef_dc3p,
    c.coef_direction, c.coef_vol3p, c.coef_vol5p
    
  FROM `kabu-376213.kabu2411.D10_trading_signals` d
  JOIN `kabu-376213.kabu2411.D81_signal_coefficients_8indicators` c
    ON d.signal_type = c.signal_type 
    AND d.signal_bin = c.signal_bin
    AND d.trade_type = c.trade_type
  WHERE d.signal_date >= '2022-07-01'  -- 全期間
),

-- 2. 各スコアタイプの対数和を計算（BUY/SELL別々に保持）
log_scores AS (
  SELECT 
    signal_date,
    stock_code,
    ANY_VALUE(stock_name) as stock_name,
    original_trade_type,
    
    -- 価格データ
    ANY_VALUE(prev_close) as prev_close,
    ANY_VALUE(day_open) as day_open,
    ANY_VALUE(day_high) as day_high,
    ANY_VALUE(day_low) as day_low,
    ANY_VALUE(day_close) as day_close,
    ANY_VALUE(prev_close_to_open_gap) as prev_close_to_open_gap,
    ANY_VALUE(open_to_high_gap) as open_to_high_gap,
    ANY_VALUE(open_to_low_gap) as open_to_low_gap,
    ANY_VALUE(open_to_close_gap) as open_to_close_gap,
    ANY_VALUE(daily_range) as daily_range,
    ANY_VALUE(baseline_profit_rate) as baseline_profit_rate,
    ANY_VALUE(is_win) as is_win,
    ANY_VALUE(trading_volume) as trading_volume,
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
  GROUP BY signal_date, stock_code, original_trade_type
),

-- 3. UNPIVOT形式に変換（15種類のスコアタイプ）
unpivoted_scores AS (
  -- 既存8指標
  SELECT signal_date, stock_code, stock_name, original_trade_type, 'H3P' as score_type, log_score_h3p as score_value, 
         prev_close, day_open, day_high, day_low, day_close, prev_close_to_open_gap, open_to_high_gap, 
         open_to_low_gap, open_to_close_gap, daily_range, baseline_profit_rate, is_win, trading_volume, 
         prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores
  UNION ALL
  SELECT signal_date, stock_code, stock_name, original_trade_type, 'H1P', log_score_h1p,
         prev_close, day_open, day_high, day_low, day_close, prev_close_to_open_gap, open_to_high_gap, 
         open_to_low_gap, open_to_close_gap, daily_range, baseline_profit_rate, is_win, trading_volume, 
         prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores
  UNION ALL
  SELECT signal_date, stock_code, stock_name, original_trade_type, 'L3P', log_score_l3p,
         prev_close, day_open, day_high, day_low, day_close, prev_close_to_open_gap, open_to_high_gap, 
         open_to_low_gap, open_to_close_gap, daily_range, baseline_profit_rate, is_win, trading_volume, 
         prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores
  UNION ALL
  SELECT signal_date, stock_code, stock_name, original_trade_type, 'L1P', log_score_l1p,
         prev_close, day_open, day_high, day_low, day_close, prev_close_to_open_gap, open_to_high_gap, 
         open_to_low_gap, open_to_close_gap, daily_range, baseline_profit_rate, is_win, trading_volume, 
         prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores
  UNION ALL
  SELECT signal_date, stock_code, stock_name, original_trade_type, 'CU3P', log_score_cu3p,
         prev_close, day_open, day_high, day_low, day_close, prev_close_to_open_gap, open_to_high_gap, 
         open_to_low_gap, open_to_close_gap, daily_range, baseline_profit_rate, is_win, trading_volume, 
         prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores
  UNION ALL
  SELECT signal_date, stock_code, stock_name, original_trade_type, 'CU1P', log_score_cu1p,
         prev_close, day_open, day_high, day_low, day_close, prev_close_to_open_gap, open_to_high_gap, 
         open_to_low_gap, open_to_close_gap, daily_range, baseline_profit_rate, is_win, trading_volume, 
         prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores
  UNION ALL
  SELECT signal_date, stock_code, stock_name, original_trade_type, 'CD3P', log_score_cd3p,
         prev_close, day_open, day_high, day_low, day_close, prev_close_to_open_gap, open_to_high_gap, 
         open_to_low_gap, open_to_close_gap, daily_range, baseline_profit_rate, is_win, trading_volume, 
         prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores
  UNION ALL
  SELECT signal_date, stock_code, stock_name, original_trade_type, 'CD1P', log_score_cd1p,
         prev_close, day_open, day_high, day_low, day_close, prev_close_to_open_gap, open_to_high_gap, 
         open_to_low_gap, open_to_close_gap, daily_range, baseline_profit_rate, is_win, trading_volume, 
         prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores
  
  -- 新4指標
  UNION ALL
  SELECT signal_date, stock_code, stock_name, original_trade_type, 'UD75P', log_score_ud75p,
         prev_close, day_open, day_high, day_low, day_close, prev_close_to_open_gap, open_to_high_gap, 
         open_to_low_gap, open_to_close_gap, daily_range, baseline_profit_rate, is_win, trading_volume, 
         prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores
  UNION ALL
  SELECT signal_date, stock_code, stock_name, original_trade_type, 'DD75P', log_score_dd75p,
         prev_close, day_open, day_high, day_low, day_close, prev_close_to_open_gap, open_to_high_gap, 
         open_to_low_gap, open_to_close_gap, daily_range, baseline_profit_rate, is_win, trading_volume, 
         prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores
  UNION ALL
  SELECT signal_date, stock_code, stock_name, original_trade_type, 'UC3P', log_score_uc3p,
         prev_close, day_open, day_high, day_low, day_close, prev_close_to_open_gap, open_to_high_gap, 
         open_to_low_gap, open_to_close_gap, daily_range, baseline_profit_rate, is_win, trading_volume, 
         prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores
  UNION ALL
  SELECT signal_date, stock_code, stock_name, original_trade_type, 'DC3P', log_score_dc3p,
         prev_close, day_open, day_high, day_low, day_close, prev_close_to_open_gap, open_to_high_gap, 
         open_to_low_gap, open_to_close_gap, daily_range, baseline_profit_rate, is_win, trading_volume, 
         prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores
  
  -- 方向性
  UNION ALL
  SELECT signal_date, stock_code, stock_name, original_trade_type, 'DIRECTION', log_score_direction,
         prev_close, day_open, day_high, day_low, day_close, prev_close_to_open_gap, open_to_high_gap, 
         open_to_low_gap, open_to_close_gap, daily_range, baseline_profit_rate, is_win, trading_volume, 
         prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores
  
  -- ボラティリティ（重複を避けるためDISTINCT）
  UNION ALL
  SELECT DISTINCT signal_date, stock_code, stock_name, original_trade_type, 'VOL3P', log_score_vol3p,
         prev_close, day_open, day_high, day_low, day_close, prev_close_to_open_gap, open_to_high_gap, 
         open_to_low_gap, open_to_close_gap, daily_range, baseline_profit_rate, is_win, trading_volume, 
         prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores
  UNION ALL
  SELECT DISTINCT signal_date, stock_code, stock_name, original_trade_type, 'VOL5P', log_score_vol5p,
         prev_close, day_open, day_high, day_low, day_close, prev_close_to_open_gap, open_to_high_gap, 
         open_to_low_gap, open_to_close_gap, daily_range, baseline_profit_rate, is_win, trading_volume, 
         prev_volume, prev_trading_value, tradable_shares 
  FROM log_scores
),

-- 4. スコアbinを計算（新しいM20_score_binsを使用）
scores_with_bins AS (
  SELECT 
    s.*,
    -- M20_score_binsからbinを決定（15種類版）
    COALESCE(
      (SELECT MAX(sb.score_bin) 
       FROM `kabu-376213.kabu2411.M20_score_bins` sb
       WHERE sb.score_type = s.score_type  -- H3P_BUYではなくH3P
         AND s.score_value > sb.lower_bound 
         AND s.score_value <= sb.upper_bound), 
      1
    ) as score_bin
  FROM unpivoted_scores s
)

-- 5. 最終結果（各スコアに対してBUY/SELL両方の戦略を生成）
SELECT 
  signal_date,
  score_type,  -- 15種類（H3P, L3P等）
  score_bin,
  strategy_type as trade_type,  -- 戦略としてのBUY/SELL
  stock_code,
  stock_name,
  score_value,
  prev_close,
  day_open,
  day_high,
  day_low,
  day_close,
  prev_close_to_open_gap,
  open_to_high_gap,
  open_to_low_gap,
  open_to_close_gap,
  daily_range,
  -- 戦略に応じた利益率計算
  CASE 
    WHEN strategy_type = 'BUY' THEN 
      open_to_close_gap / NULLIF(day_open, 0) * 100  -- BUY戦略：寄→引の上昇率
    ELSE 
      -open_to_close_gap / NULLIF(day_open, 0) * 100  -- SELL戦略：寄→引の下落率
  END as baseline_profit_rate,
  CASE 
    WHEN strategy_type = 'BUY' THEN 
      CASE WHEN open_to_close_gap > 0 THEN TRUE ELSE FALSE END
    ELSE 
      CASE WHEN open_to_close_gap < 0 THEN TRUE ELSE FALSE END
  END as is_win,
  trading_volume,
  prev_volume,
  prev_trading_value,
  tradable_shares,
  CURRENT_TIMESTAMP() as created_at
FROM scores_with_bins
CROSS JOIN UNNEST(['BUY', 'SELL']) as strategy_type;  -- 各スコアに対してBUY/SELL戦略を適用

-- ============================================================================
-- Part 3: 完了確認
-- ============================================================================

-- 総レコード数確認
SELECT 
  '✅ D30_trading_scores 修正版投入完了' as status,
  COUNT(*) as total_records,
  COUNT(DISTINCT score_type) as score_types_should_be_15,
  COUNT(DISTINCT stock_code) as stock_count,
  COUNT(DISTINCT trade_type) as trade_types,
  MIN(signal_date) as min_date,
  MAX(signal_date) as max_date,
  CURRENT_TIMESTAMP() as completion_time
FROM `kabu-376213.kabu2411.D30_trading_scores`;

-- スコアタイプ別レコード数確認
SELECT 
  '📊 スコアタイプ別データ分布' as check_type,
  score_type,
  COUNT(*) as record_count,
  COUNT(DISTINCT CONCAT(score_type, '_', trade_type)) as strategy_combinations,
  COUNT(DISTINCT stock_code) as stock_count,
  ROUND(AVG(score_value), 4) as avg_score_value,
  COUNT(DISTINCT score_bin) as bin_count
FROM `kabu-376213.kabu2411.D30_trading_scores`
WHERE signal_date = (SELECT MAX(signal_date) FROM `kabu-376213.kabu2411.D30_trading_scores`)
GROUP BY score_type
ORDER BY score_type;

-- 戦略別パフォーマンス確認（サンプル）
SELECT 
  '📈 戦略別パフォーマンス（H3P例）' as check_type,
  score_type,
  trade_type,
  COUNT(*) as trades,
  ROUND(AVG(CASE WHEN is_win THEN 1.0 ELSE 0.0 END) * 100, 1) as win_rate_pct,
  ROUND(AVG(baseline_profit_rate), 3) as avg_profit_rate
FROM `kabu-376213.kabu2411.D30_trading_scores`
WHERE score_type = 'H3P'
  AND signal_date >= DATE_SUB((SELECT MAX(signal_date) FROM `kabu-376213.kabu2411.D30_trading_scores`), INTERVAL 30 DAY)
GROUP BY score_type, trade_type;