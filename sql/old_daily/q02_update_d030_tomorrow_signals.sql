/*
ファイル: d02_update_d030_tomorrow_signals.sql
説明: D030_tomorrow_signals 日次データ更新（クリーン版）
作成日: 2025年1月10日
実行タイミング: 日次17:00（市場終了後）
依存: D020_learning_stats + daily_quotes + trading_calendar
目的: 明日発生予定のシグナル計算 + 学習期間統計の統合データ作成
処理時間: 約3-5分
データ量: 約5万レコード/日（1日分のみ保持）
更新: 日次で全件削除→再作成
*/

-- ============================================================================
-- D030日次更新（明日シグナル予定 + 学習期間統計統合）
-- ============================================================================

-- 処理開始メッセージ
SELECT 
  '🚀 D030日次更新開始' as message,
  '処理内容: 明日のシグナル予定作成' as process_description,
  'データソース: 最新株価データ + D020統計データ' as data_source,
  '予想レコード数: 約5万レコード' as estimated_records,
  CURRENT_TIMESTAMP() as start_time;

-- ============================================================================
-- Step 1: 既存データ全削除
-- ============================================================================

DELETE FROM `kabu-376213.kabu2411.D030_tomorrow_signals` 
WHERE TRUE;

SELECT 
  '✅ Step 1完了: 既存データ全削除完了' as status,
  CURRENT_TIMESTAMP() as deleted_time;

-- ============================================================================
-- Step 2: 明日シグナル予定データ投入
-- ============================================================================

INSERT INTO `kabu-376213.kabu2411.D030_tomorrow_signals` (
  target_date,
  signal_type,
  signal_bin,
  trade_type,
  stock_code,
  stock_name,
  signal_value,
  total_samples,
  win_samples,
  win_rate,
  avg_profit_rate,
  std_deviation,
  sharpe_ratio,
  max_profit_rate,
  min_profit_rate,
  is_excellent_pattern,
  pattern_category,
  priority_score,
  decision_status,
  profit_target_yen,
  loss_cut_yen,
  prev_close_gap_condition,
  additional_notes,
  decided_at,
  first_signal_date,
  last_signal_date,
  created_at,
  updated_at
)
WITH 
-- 1. 過去35日分の株価データ準備
stock_quotes AS (
  SELECT 
    REGEXP_REPLACE(dq.Code, '0$', '') as stock_code,
    dq.Date as quote_date,
    dq.Open,
    dq.High, 
    dq.Low,
    dq.Close,
    dq.Volume,
    dq.TurnoverValue,
    LAG(dq.Close) OVER (
      PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') 
      ORDER BY dq.Date
    ) as prev_close_for_signal,
    LAG(dq.Volume) OVER (
      PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') 
      ORDER BY dq.Date
    ) as prev_volume_for_signal,
    LAG(dq.TurnoverValue) OVER (
      PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') 
      ORDER BY dq.Date
    ) as prev_value_for_signal
  FROM `kabu-376213.kabu2411.daily_quotes` dq
  WHERE dq.Date >= DATE_SUB(
      (SELECT MAX(Date) FROM `kabu-376213.kabu2411.daily_quotes`), 
      INTERVAL 50 DAY
    )
    AND dq.Date <= (SELECT MAX(Date) FROM `kabu-376213.kabu2411.daily_quotes`)
    AND dq.Open > 0 AND dq.Close > 0
),

-- 2. シグナル計算（最新日のみ）
signal_calculations AS (
  SELECT 
    q.stock_code,
    mts.company_name as stock_name,
    q.quote_date,
    (
      SELECT MIN(tc.Date)
      FROM `kabu-376213.kabu2411.trading_calendar` tc
      WHERE tc.Date > q.quote_date 
        AND tc.HolidayDivision = '1'
    ) as target_date,
    q.Open as quote_open,
    q.High as quote_high,
    q.Low as quote_low,
    q.Close as quote_close,
    q.Volume as quote_volume,
    q.TurnoverValue as quote_value,
    q.prev_close_for_signal,
    q.prev_volume_for_signal,
    q.prev_value_for_signal,
    
    AVG(q.Close) OVER (PARTITION BY q.stock_code ORDER BY q.quote_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as ma3_close,
    AVG(q.Close) OVER (PARTITION BY q.stock_code ORDER BY q.quote_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as ma5_close,
    AVG(q.Close) OVER (PARTITION BY q.stock_code ORDER BY q.quote_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as ma10_close,
    AVG(q.Close) OVER (PARTITION BY q.stock_code ORDER BY q.quote_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as ma20_close,
    AVG(q.Volume) OVER (PARTITION BY q.stock_code ORDER BY q.quote_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as ma3_volume,
    AVG(q.Volume) OVER (PARTITION BY q.stock_code ORDER BY q.quote_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as ma5_volume,
    AVG(q.Volume) OVER (PARTITION BY q.stock_code ORDER BY q.quote_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as ma10_volume,
    AVG(q.TurnoverValue) OVER (PARTITION BY q.stock_code ORDER BY q.quote_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as ma3_value,
    AVG(q.TurnoverValue) OVER (PARTITION BY q.stock_code ORDER BY q.quote_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as ma5_value,
    AVG(q.TurnoverValue) OVER (PARTITION BY q.stock_code ORDER BY q.quote_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as ma10_value,
    MAX(q.Close) OVER (PARTITION BY q.stock_code ORDER BY q.quote_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as max20_close,
    MIN(q.Close) OVER (PARTITION BY q.stock_code ORDER BY q.quote_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as min20_close,
    STDDEV(q.Close) OVER (PARTITION BY q.stock_code ORDER BY q.quote_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as stddev20_close,
    AVG(CASE WHEN q.Open > 0 THEN q.High / q.Open ELSE NULL END) OVER (PARTITION BY q.stock_code ORDER BY q.quote_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as avg_high_open_3d,
    AVG(CASE WHEN q.Open > 0 THEN q.High / q.Open ELSE NULL END) OVER (PARTITION BY q.stock_code ORDER BY q.quote_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as avg_high_open_7d,
    AVG(CASE WHEN q.Open > 0 THEN q.High / q.Open ELSE NULL END) OVER (PARTITION BY q.stock_code ORDER BY q.quote_date ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) as avg_high_open_9d,
    AVG(CASE WHEN q.Open > 0 THEN q.High / q.Open ELSE NULL END) OVER (PARTITION BY q.stock_code ORDER BY q.quote_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as avg_high_open_14d,
    AVG(CASE WHEN q.Open > 0 THEN q.High / q.Open ELSE NULL END) OVER (PARTITION BY q.stock_code ORDER BY q.quote_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as avg_high_open_20d,
    AVG(CASE WHEN q.Low > 0 THEN q.Open / q.Low ELSE NULL END) OVER (PARTITION BY q.stock_code ORDER BY q.quote_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as avg_open_low_3d,
    AVG(CASE WHEN q.Low > 0 THEN q.Open / q.Low ELSE NULL END) OVER (PARTITION BY q.stock_code ORDER BY q.quote_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as avg_open_low_7d,
    AVG(CASE WHEN q.Low > 0 THEN q.Open / q.Low ELSE NULL END) OVER (PARTITION BY q.stock_code ORDER BY q.quote_date ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) as avg_open_low_9d,
    AVG(CASE WHEN q.Low > 0 THEN q.Open / q.Low ELSE NULL END) OVER (PARTITION BY q.stock_code ORDER BY q.quote_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as avg_open_low_14d,
    AVG(CASE WHEN q.Low > 0 THEN q.Open / q.Low ELSE NULL END) OVER (PARTITION BY q.stock_code ORDER BY q.quote_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as avg_open_low_20d
  FROM stock_quotes q
  INNER JOIN `kabu-376213.kabu2411.master_trading_stocks` mts
    ON q.stock_code = mts.stock_code
  WHERE q.prev_close_for_signal IS NOT NULL
    AND q.quote_date = (SELECT MAX(Date) FROM `kabu-376213.kabu2411.daily_quotes`)
),

-- 3. 37指標のシグナル生成
all_signals AS (
  -- Price系 9指標
  SELECT stock_code, stock_name, quote_date, target_date, 'Close Change Rate' as signal_type, ROUND((quote_close - prev_close_for_signal) / prev_close_for_signal * 100, 4) as signal_value FROM signal_calculations WHERE prev_close_for_signal > 0 AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'Close to Prev Close Ratio' as signal_type, ROUND(quote_close / prev_close_for_signal * 100, 4) as signal_value FROM signal_calculations WHERE prev_close_for_signal > 0 AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'Close MA3 Deviation' as signal_type, ROUND(quote_close / ma3_close * 100, 4) as signal_value FROM signal_calculations WHERE ma3_close > 0 AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'Close MA5 Deviation' as signal_type, ROUND(quote_close / ma5_close * 100, 4) as signal_value FROM signal_calculations WHERE ma5_close > 0 AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'Close MA10 Deviation' as signal_type, ROUND(quote_close / ma10_close * 100, 4) as signal_value FROM signal_calculations WHERE ma10_close > 0 AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'Close to MAX20 Ratio' as signal_type, ROUND(quote_close / max20_close * 100, 4) as signal_value FROM signal_calculations WHERE max20_close > 0 AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'Close to MIN20 Ratio' as signal_type, ROUND(quote_close / min20_close * 100, 4) as signal_value FROM signal_calculations WHERE min20_close > 0 AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'Close to Open Ratio' as signal_type, ROUND(quote_close / quote_open * 100, 4) as signal_value FROM signal_calculations WHERE quote_open > 0 AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'Close Volatility' as signal_type, ROUND(SAFE_DIVIDE(stddev20_close, ma20_close) * 100, 4) as signal_value FROM signal_calculations WHERE ma20_close > 0 AND stddev20_close IS NOT NULL AND target_date IS NOT NULL

  -- PriceRange系 5指標
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'Close to Range Ratio' as signal_type, ROUND(SAFE_DIVIDE(quote_close - quote_low, quote_high - quote_low) * 100, 4) as signal_value FROM signal_calculations WHERE quote_high > quote_low AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'High to Close Drop Rate' as signal_type, ROUND(SAFE_DIVIDE(quote_high - quote_close, quote_high - quote_low) * 100, 4) as signal_value FROM signal_calculations WHERE quote_high > quote_low AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'Close to Low Rise Rate' as signal_type, ROUND(SAFE_DIVIDE(quote_close - quote_low, quote_high - quote_low) * 100, 4) as signal_value FROM signal_calculations WHERE quote_high > quote_low AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'High to Close Ratio' as signal_type, ROUND(quote_close / quote_high * 100, 4) as signal_value FROM signal_calculations WHERE quote_high > 0 AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'Close to Low Ratio' as signal_type, ROUND(quote_close / quote_low * 100, 4) as signal_value FROM signal_calculations WHERE quote_low > 0 AND target_date IS NOT NULL

  -- OpenClose系 2指標
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'Open to Close Change Rate' as signal_type, ROUND((quote_close - quote_open) / quote_open * 100, 4) as signal_value FROM signal_calculations WHERE quote_open > 0 AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'Open Close Range Efficiency' as signal_type, ROUND(SAFE_DIVIDE(quote_close - quote_open, quote_high - quote_low) * 100, 4) as signal_value FROM signal_calculations WHERE quote_high > quote_low AND target_date IS NOT NULL

  -- Open系 3指標
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'Open to Range Ratio' as signal_type, ROUND(SAFE_DIVIDE(quote_open - quote_low, quote_high - quote_low) * 100, 4) as signal_value FROM signal_calculations WHERE quote_high > quote_low AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'High to Open Drop Rate' as signal_type, ROUND(SAFE_DIVIDE(quote_high - quote_open, quote_high - quote_low) * 100, 4) as signal_value FROM signal_calculations WHERE quote_high > quote_low AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'Open to Low Rise Rate' as signal_type, ROUND(SAFE_DIVIDE(quote_open - quote_low, quote_high - quote_low) * 100, 4) as signal_value FROM signal_calculations WHERE quote_high > quote_low AND target_date IS NOT NULL

  -- Volume系 4指標
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'Volume to Prev Ratio' as signal_type, ROUND(quote_volume / prev_volume_for_signal * 100, 4) as signal_value FROM signal_calculations WHERE prev_volume_for_signal > 0 AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'Volume MA3 Deviation' as signal_type, ROUND(quote_volume / ma3_volume * 100, 4) as signal_value FROM signal_calculations WHERE ma3_volume > 0 AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'Volume MA5 Deviation' as signal_type, ROUND(quote_volume / ma5_volume * 100, 4) as signal_value FROM signal_calculations WHERE ma5_volume > 0 AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'Volume MA10 Deviation' as signal_type, ROUND(quote_volume / ma10_volume * 100, 4) as signal_value FROM signal_calculations WHERE ma10_volume > 0 AND target_date IS NOT NULL

  -- Value系 4指標
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'Value to Prev Ratio' as signal_type, ROUND(quote_value / prev_value_for_signal * 100, 4) as signal_value FROM signal_calculations WHERE prev_value_for_signal > 0 AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'Value MA3 Deviation' as signal_type, ROUND(quote_value / ma3_value * 100, 4) as signal_value FROM signal_calculations WHERE ma3_value > 0 AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'Value MA5 Deviation' as signal_type, ROUND(quote_value / ma5_value * 100, 4) as signal_value FROM signal_calculations WHERE ma5_value > 0 AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'Value MA10 Deviation' as signal_type, ROUND(quote_value / ma10_value * 100, 4) as signal_value FROM signal_calculations WHERE ma10_value > 0 AND target_date IS NOT NULL

  -- Score系 10指標
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'High Price Score 3D' as signal_type, ROUND(COALESCE(avg_high_open_3d * 50, 0) + COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + COALESCE(SAFE_DIVIDE(quote_close - quote_open, quote_open) * 20, 0), 4) as signal_value FROM signal_calculations WHERE quote_open > 0 AND avg_high_open_3d IS NOT NULL AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'High Price Score 7D' as signal_type, ROUND(COALESCE(avg_high_open_7d * 50, 0) + COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + COALESCE(SAFE_DIVIDE(quote_close - quote_open, quote_open) * 20, 0), 4) as signal_value FROM signal_calculations WHERE quote_open > 0 AND avg_high_open_7d IS NOT NULL AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'High Price Score 9D' as signal_type, ROUND(COALESCE(avg_high_open_9d * 50, 0) + COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + COALESCE(SAFE_DIVIDE(quote_close - quote_open, quote_open) * 20, 0), 4) as signal_value FROM signal_calculations WHERE quote_open > 0 AND avg_high_open_9d IS NOT NULL AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'High Price Score 14D' as signal_type, ROUND(COALESCE(avg_high_open_14d * 50, 0) + COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + COALESCE(SAFE_DIVIDE(quote_close - quote_open, quote_open) * 20, 0), 4) as signal_value FROM signal_calculations WHERE quote_open > 0 AND avg_high_open_14d IS NOT NULL AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'High Price Score 20D' as signal_type, ROUND(COALESCE(avg_high_open_20d * 50, 0) + COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + COALESCE(SAFE_DIVIDE(quote_close - quote_open, quote_open) * 20, 0), 4) as signal_value FROM signal_calculations WHERE quote_open > 0 AND avg_high_open_20d IS NOT NULL AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'Low Price Score 3D' as signal_type, ROUND(COALESCE(avg_open_low_3d * 50, 0) + COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + COALESCE(ABS(SAFE_DIVIDE(quote_close - quote_open, quote_open)) * 20, 0), 4) as signal_value FROM signal_calculations WHERE quote_open > 0 AND avg_open_low_3d IS NOT NULL AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'Low Price Score 7D' as signal_type, ROUND(COALESCE(avg_open_low_7d * 50, 0) + COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + COALESCE(ABS(SAFE_DIVIDE(quote_close - quote_open, quote_open)) * 20, 0), 4) as signal_value FROM signal_calculations WHERE quote_open > 0 AND avg_open_low_7d IS NOT NULL AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'Low Price Score 9D' as signal_type, ROUND(COALESCE(avg_open_low_9d * 50, 0) + COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + COALESCE(ABS(SAFE_DIVIDE(quote_close - quote_open, quote_open)) * 20, 0), 4) as signal_value FROM signal_calculations WHERE quote_open > 0 AND avg_open_low_9d IS NOT NULL AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'Low Price Score 14D' as signal_type, ROUND(COALESCE(avg_open_low_14d * 50, 0) + COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + COALESCE(ABS(SAFE_DIVIDE(quote_close - quote_open, quote_open)) * 20, 0), 4) as signal_value FROM signal_calculations WHERE quote_open > 0 AND avg_open_low_14d IS NOT NULL AND target_date IS NOT NULL
  UNION ALL
  SELECT stock_code, stock_name, quote_date, target_date, 'Low Price Score 20D' as signal_type, ROUND(COALESCE(avg_open_low_20d * 50, 0) + COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + COALESCE(ABS(SAFE_DIVIDE(quote_close - quote_open, quote_open)) * 20, 0), 4) as signal_value FROM signal_calculations WHERE quote_open > 0 AND avg_open_low_20d IS NOT NULL AND target_date IS NOT NULL
),

-- 4. シグナルbinマッピング
signals_with_bins AS (
  SELECT 
    s.*,
    COALESCE(
      (SELECT MAX(sb.signal_bin) 
       FROM `kabu-376213.kabu2411.M010_signal_bins` sb
       WHERE sb.signal_type = s.signal_type
         AND s.signal_value > sb.lower_bound 
         AND s.signal_value <= sb.upper_bound), 
      1
    ) as signal_bin
  FROM all_signals s
),

-- 5. BUY/SELL展開とD020統計データ結合
final_signals AS (
  SELECT 
    swb.target_date,
    swb.signal_type,
    swb.signal_bin,
    trade_type,
    swb.stock_code,
    swb.stock_name,
    swb.signal_value,
    
    COALESCE(d20.total_samples, 0) as total_samples,
    COALESCE(d20.win_samples, 0) as win_samples,
    COALESCE(d20.win_rate, 0.0) as win_rate,
    COALESCE(d20.avg_profit_rate, 0.0) as avg_profit_rate,
    COALESCE(d20.std_deviation, 0.0) as std_deviation,
    COALESCE(d20.sharpe_ratio, 0.0) as sharpe_ratio,
    COALESCE(d20.max_profit_rate, 0.0) as max_profit_rate,
    COALESCE(d20.min_profit_rate, 0.0) as min_profit_rate,
    COALESCE(d20.is_excellent_pattern, false) as is_excellent_pattern,
    COALESCE(d20.pattern_category, 'CAUTION') as pattern_category,
    COALESCE(d20.priority_score, 0.0) as priority_score,
    COALESCE(d20.decision_status, 'pending') as decision_status,
    d20.profit_target_yen,
    d20.loss_cut_yen,
    d20.prev_close_gap_condition,
    d20.additional_notes,
    d20.decided_at,
    d20.first_signal_date,
    d20.last_signal_date,
    
    CURRENT_TIMESTAMP() as created_at,
    CURRENT_TIMESTAMP() as updated_at
    
  FROM signals_with_bins swb
  CROSS JOIN UNNEST(['BUY', 'SELL']) as trade_type
  LEFT JOIN `kabu-376213.kabu2411.D020_learning_stats` d20
    ON swb.signal_type = d20.signal_type
    AND swb.signal_bin = d20.signal_bin
    AND trade_type = d20.trade_type
    AND swb.stock_code = d20.stock_code
  WHERE swb.signal_bin IS NOT NULL
)

SELECT 
  target_date,
  signal_type,
  signal_bin,
  trade_type,
  stock_code,
  stock_name,
  signal_value,
  total_samples,
  win_samples,
  win_rate,
  avg_profit_rate,
  std_deviation,
  sharpe_ratio,
  max_profit_rate,
  min_profit_rate,
  is_excellent_pattern,
  pattern_category,
  priority_score,
  decision_status,
  profit_target_yen,
  loss_cut_yen,
  prev_close_gap_condition,
  additional_notes,
  decided_at,
  first_signal_date,
  last_signal_date,
  created_at,
  updated_at
FROM final_signals
ORDER BY 
  is_excellent_pattern DESC,
  priority_score DESC,
  stock_code,
  signal_type,
  trade_type;

-- ============================================================================
-- Step 3: 投入結果確認
-- ============================================================================

SELECT 
  '✅ Step 3: 投入結果確認' as check_step,
  COUNT(*) as total_records_inserted,
  COUNT(DISTINCT signal_type) as signal_types_count,
  COUNT(DISTINCT stock_code) as stocks_count,
  SUM(CASE WHEN is_excellent_pattern = true THEN 1 ELSE 0 END) as excellent_patterns,
  MIN(target_date) as target_date,
  CURRENT_TIMESTAMP() as end_time
FROM `kabu-376213.kabu2411.D030_tomorrow_signals`;

-- ============================================================================
-- 完了メッセージ
-- ============================================================================

SELECT 
  '🏆 D030日次更新完了！' as message,
  '✅ 全件削除→再作成完了' as update_method,
  '✅ 37指標シグナル計算完了' as signal_calculation,
  '✅ D020統計データ統合完了' as statistics_integration,
  CURRENT_TIMESTAMP() as completion_time;