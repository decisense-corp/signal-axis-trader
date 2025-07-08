/*
ファイル: q01_update_d010_daily_results_single_date.sql
説明: D010_basic_results 日次増分投入（1日指定版）
作成日: 2025年1月10日
実行タイミング: 日次17:00（市場終了後）
使用方法: target_quote_dateを変更して実行
*/

-- ============================================================================
-- ⚠️ ここだけ変更してください
-- ============================================================================
DECLARE target_quote_date DATE DEFAULT '2025-07-04';  -- 処理したい日付

-- ============================================================================
-- 以下は変更不要
-- ============================================================================

-- 処理開始メッセージ
SELECT 
  '🚀 D010増分投入開始（1日指定版）' as message,
  target_quote_date as processing_date,
  (
    SELECT MIN(tc.Date)
    FROM `kabu-376213.kabu2411.trading_calendar` tc
    WHERE tc.Date > target_quote_date AND tc.HolidayDivision = '1'
  ) as signal_date,
  CURRENT_TIMESTAMP() as start_time;

-- メイン処理
INSERT INTO `kabu-376213.kabu2411.D010_basic_results`
WITH 
-- 1. 株価データ準備
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
  WHERE dq.Date >= DATE_SUB(target_quote_date, INTERVAL 35 DAY)
    AND dq.Date <= target_quote_date
    AND dq.Open > 0 AND dq.Close > 0
),

-- 2. シグナル日付計算とシグナル値計算
signal_calculations AS (
  SELECT 
    q.stock_code,
    mts.company_name as stock_name,
    q.quote_date,
    -- signal_date計算（翌営業日）
    (
      SELECT MIN(tc.Date)
      FROM `kabu-376213.kabu2411.trading_calendar` tc
      WHERE tc.Date > q.quote_date AND tc.HolidayDivision = '1'
    ) as signal_date,
    q.Open as quote_open,
    q.High as quote_high,
    q.Low as quote_low,
    q.Close as quote_close,
    q.Volume as quote_volume,
    q.TurnoverValue as quote_value,
    q.prev_close_for_signal,
    q.prev_volume_for_signal,
    q.prev_value_for_signal,
    
    -- 移動平均計算
    AVG(q.Close) OVER (
      PARTITION BY q.stock_code 
      ORDER BY q.quote_date 
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as ma3_close,
    
    AVG(q.Close) OVER (
      PARTITION BY q.stock_code 
      ORDER BY q.quote_date 
      ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) as ma5_close,
    
    AVG(q.Close) OVER (
      PARTITION BY q.stock_code 
      ORDER BY q.quote_date 
      ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) as ma10_close,
    
    AVG(q.Close) OVER (
      PARTITION BY q.stock_code 
      ORDER BY q.quote_date 
      ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) as ma20_close,
    
    -- Volume移動平均
    AVG(q.Volume) OVER (
      PARTITION BY q.stock_code 
      ORDER BY q.quote_date 
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as ma3_volume,
    
    AVG(q.Volume) OVER (
      PARTITION BY q.stock_code 
      ORDER BY q.quote_date 
      ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) as ma5_volume,
    
    AVG(q.Volume) OVER (
      PARTITION BY q.stock_code 
      ORDER BY q.quote_date 
      ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) as ma10_volume,
    
    -- TurnoverValue移動平均
    AVG(q.TurnoverValue) OVER (
      PARTITION BY q.stock_code 
      ORDER BY q.quote_date 
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as ma3_value,
    
    AVG(q.TurnoverValue) OVER (
      PARTITION BY q.stock_code 
      ORDER BY q.quote_date 
      ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) as ma5_value,
    
    AVG(q.TurnoverValue) OVER (
      PARTITION BY q.stock_code 
      ORDER BY q.quote_date 
      ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) as ma10_value,
    
    -- レンジ計算用
    MAX(q.Close) OVER (
      PARTITION BY q.stock_code 
      ORDER BY q.quote_date 
      ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) as max20_close,
    
    MIN(q.Close) OVER (
      PARTITION BY q.stock_code 
      ORDER BY q.quote_date 
      ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) as min20_close,
    
    -- 標準偏差（ボラティリティ用）
    STDDEV(q.Close) OVER (
      PARTITION BY q.stock_code 
      ORDER BY q.quote_date 
      ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) as stddev20_close,
    
    -- Score系指標用の移動平均
    AVG(CASE WHEN q.Open > 0 THEN q.High / q.Open ELSE NULL END) OVER (
      PARTITION BY q.stock_code 
      ORDER BY q.quote_date 
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as avg_high_open_3d,
    
    AVG(CASE WHEN q.Open > 0 THEN q.High / q.Open ELSE NULL END) OVER (
      PARTITION BY q.stock_code 
      ORDER BY q.quote_date 
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as avg_high_open_7d,
    
    AVG(CASE WHEN q.Open > 0 THEN q.High / q.Open ELSE NULL END) OVER (
      PARTITION BY q.stock_code 
      ORDER BY q.quote_date 
      ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
    ) as avg_high_open_9d,
    
    AVG(CASE WHEN q.Open > 0 THEN q.High / q.Open ELSE NULL END) OVER (
      PARTITION BY q.stock_code 
      ORDER BY q.quote_date 
      ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) as avg_high_open_14d,
    
    AVG(CASE WHEN q.Open > 0 THEN q.High / q.Open ELSE NULL END) OVER (
      PARTITION BY q.stock_code 
      ORDER BY q.quote_date 
      ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) as avg_high_open_20d,
    
    AVG(CASE WHEN q.Low > 0 THEN q.Open / q.Low ELSE NULL END) OVER (
      PARTITION BY q.stock_code 
      ORDER BY q.quote_date 
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as avg_open_low_3d,
    
    AVG(CASE WHEN q.Low > 0 THEN q.Open / q.Low ELSE NULL END) OVER (
      PARTITION BY q.stock_code 
      ORDER BY q.quote_date 
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as avg_open_low_7d,
    
    AVG(CASE WHEN q.Low > 0 THEN q.Open / q.Low ELSE NULL END) OVER (
      PARTITION BY q.stock_code 
      ORDER BY q.quote_date 
      ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
    ) as avg_open_low_9d,
    
    AVG(CASE WHEN q.Low > 0 THEN q.Open / q.Low ELSE NULL END) OVER (
      PARTITION BY q.stock_code 
      ORDER BY q.quote_date 
      ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) as avg_open_low_14d,
    
    AVG(CASE WHEN q.Low > 0 THEN q.Open / q.Low ELSE NULL END) OVER (
      PARTITION BY q.stock_code 
      ORDER BY q.quote_date 
      ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) as avg_open_low_20d
    
  FROM stock_quotes q
  INNER JOIN `kabu-376213.kabu2411.master_trading_stocks` mts
    ON q.stock_code = mts.stock_code
  WHERE q.prev_close_for_signal IS NOT NULL
),

-- 3. 37指標のシグナル生成
all_signals AS (

  -- ==================== Price系 9指標 ====================
  
  -- Close Change Rate
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Close Change Rate' as signal_type,
    ROUND((quote_close - prev_close_for_signal) / prev_close_for_signal * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE prev_close_for_signal > 0 
    AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- Close to Prev Close Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Close to Prev Close Ratio' as signal_type,
    ROUND(quote_close / prev_close_for_signal * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE prev_close_for_signal > 0 AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- Close MA3 Deviation
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Close MA3 Deviation' as signal_type,
    ROUND(quote_close / ma3_close * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma3_close > 0 AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- Close MA5 Deviation  
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Close MA5 Deviation' as signal_type,
    ROUND(quote_close / ma5_close * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma5_close > 0 AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- Close MA10 Deviation
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Close MA10 Deviation' as signal_type, 
    ROUND(quote_close / ma10_close * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma10_close > 0 AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- Close to MAX20 Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Close to MAX20 Ratio' as signal_type,
    ROUND(quote_close / max20_close * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE max20_close > 0 AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- Close to MIN20 Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Close to MIN20 Ratio' as signal_type,
    ROUND(quote_close / min20_close * 100, 4) as signal_value  
  FROM signal_calculations 
  WHERE min20_close > 0 AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- Close to Open Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Close to Open Ratio' as signal_type,
    ROUND(quote_close / quote_open * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_open > 0 AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- Close Volatility
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Close Volatility' as signal_type,
    ROUND(SAFE_DIVIDE(stddev20_close, ma20_close) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma20_close > 0 AND stddev20_close IS NOT NULL AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ

  -- ==================== PriceRange系 5指標 ====================
  
  UNION ALL
  
  -- Close to Range Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Close to Range Ratio' as signal_type,
    ROUND(SAFE_DIVIDE(quote_close - quote_low, quote_high - quote_low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > quote_low AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- High to Close Drop Rate
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'High to Close Drop Rate' as signal_type,
    ROUND(SAFE_DIVIDE(quote_high - quote_close, quote_high - quote_low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > quote_low AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- Close to Low Rise Rate
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Close to Low Rise Rate' as signal_type,
    ROUND(SAFE_DIVIDE(quote_close - quote_low, quote_high - quote_low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > quote_low AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- High to Close Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'High to Close Ratio' as signal_type,
    ROUND(quote_close / quote_high * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > 0 AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- Close to Low Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Close to Low Ratio' as signal_type,
    ROUND(quote_close / quote_low * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_low > 0 AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ

  -- ==================== OpenClose系 3指標 ====================
  
  UNION ALL
  
  -- Open to Close Change Rate
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Open to Close Change Rate' as signal_type,
    ROUND((quote_close - quote_open) / quote_open * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_open > 0 AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- Open Close Range Efficiency
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Open Close Range Efficiency' as signal_type,
    ROUND(SAFE_DIVIDE(quote_close - quote_open, quote_high - quote_low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > quote_low AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ

  -- ==================== Open系 3指標 ====================
  
  UNION ALL
  
  -- Open to Range Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Open to Range Ratio' as signal_type,
    ROUND(SAFE_DIVIDE(quote_open - quote_low, quote_high - quote_low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > quote_low AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- High to Open Drop Rate
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'High to Open Drop Rate' as signal_type,
    ROUND(SAFE_DIVIDE(quote_high - quote_open, quote_high - quote_low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > quote_low AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- Open to Low Rise Rate
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Open to Low Rise Rate' as signal_type,
    ROUND(SAFE_DIVIDE(quote_open - quote_low, quote_high - quote_low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > quote_low AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ

  -- ==================== Volume系 4指標 ====================
  
  UNION ALL
  
  -- Volume to Prev Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Volume to Prev Ratio' as signal_type,
    ROUND(quote_volume / prev_volume_for_signal * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE prev_volume_for_signal > 0 AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- Volume MA3 Deviation
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Volume MA3 Deviation' as signal_type,
    ROUND(quote_volume / ma3_volume * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma3_volume > 0 AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- Volume MA5 Deviation
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Volume MA5 Deviation' as signal_type,
    ROUND(quote_volume / ma5_volume * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma5_volume > 0 AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- Volume MA10 Deviation
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Volume MA10 Deviation' as signal_type,
    ROUND(quote_volume / ma10_volume * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma10_volume > 0 AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ

  -- ==================== Value系 4指標 ====================
  
  UNION ALL
  
  -- Value to Prev Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Value to Prev Ratio' as signal_type,
    ROUND(quote_value / prev_value_for_signal * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE prev_value_for_signal > 0 AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- Value MA3 Deviation
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Value MA3 Deviation' as signal_type,
    ROUND(quote_value / ma3_value * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma3_value > 0 AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- Value MA5 Deviation
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Value MA5 Deviation' as signal_type,
    ROUND(quote_value / ma5_value * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma5_value > 0 AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- Value MA10 Deviation
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Value MA10 Deviation' as signal_type,
    ROUND(quote_value / ma10_value * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma10_value > 0 AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ

  -- ==================== Score系 10指標 ====================
  
  UNION ALL
  
  -- High Price Score 3D
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'High Price Score 3D' as signal_type,
    ROUND(
      COALESCE(avg_high_open_3d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + 
      COALESCE(SAFE_DIVIDE(quote_close - quote_open, quote_open) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE quote_open > 0 AND avg_high_open_3d IS NOT NULL AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- High Price Score 7D
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'High Price Score 7D' as signal_type,
    ROUND(
      COALESCE(avg_high_open_7d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + 
      COALESCE(SAFE_DIVIDE(quote_close - quote_open, quote_open) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE quote_open > 0 AND avg_high_open_7d IS NOT NULL AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- High Price Score 9D
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'High Price Score 9D' as signal_type,
    ROUND(
      COALESCE(avg_high_open_9d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + 
      COALESCE(SAFE_DIVIDE(quote_close - quote_open, quote_open) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE quote_open > 0 AND avg_high_open_9d IS NOT NULL AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- High Price Score 14D
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'High Price Score 14D' as signal_type,
    ROUND(
      COALESCE(avg_high_open_14d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + 
      COALESCE(SAFE_DIVIDE(quote_close - quote_open, quote_open) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE quote_open > 0 AND avg_high_open_14d IS NOT NULL AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- High Price Score 20D
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'High Price Score 20D' as signal_type,
    ROUND(
      COALESCE(avg_high_open_20d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + 
      COALESCE(SAFE_DIVIDE(quote_close - quote_open, quote_open) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE quote_open > 0 AND avg_high_open_20d IS NOT NULL AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- Low Price Score 3D
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Low Price Score 3D' as signal_type,
    ROUND(
      COALESCE(avg_open_low_3d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + 
      COALESCE(ABS(SAFE_DIVIDE(quote_close - quote_open, quote_open)) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE quote_open > 0 AND avg_open_low_3d IS NOT NULL AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- Low Price Score 7D
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Low Price Score 7D' as signal_type,
    ROUND(
      COALESCE(avg_open_low_7d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + 
      COALESCE(ABS(SAFE_DIVIDE(quote_close - quote_open, quote_open)) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE quote_open > 0 AND avg_open_low_7d IS NOT NULL AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- Low Price Score 9D
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Low Price Score 9D' as signal_type,
    ROUND(
      COALESCE(avg_open_low_9d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + 
      COALESCE(ABS(SAFE_DIVIDE(quote_close - quote_open, quote_open)) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE quote_open > 0 AND avg_open_low_9d IS NOT NULL AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- Low Price Score 14D
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Low Price Score 14D' as signal_type,
    ROUND(
      COALESCE(avg_open_low_14d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + 
      COALESCE(ABS(SAFE_DIVIDE(quote_close - quote_open, quote_open)) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE quote_open > 0 AND avg_open_low_14d IS NOT NULL AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
  
  UNION ALL
  
  -- Low Price Score 20D
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Low Price Score 20D' as signal_type,
    ROUND(
      COALESCE(avg_open_low_20d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + 
      COALESCE(ABS(SAFE_DIVIDE(quote_close - quote_open, quote_open)) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE quote_open > 0 AND avg_open_low_20d IS NOT NULL AND signal_date IS NOT NULL
    AND quote_date = target_quote_date  -- 指定日のみ
),

-- 4. signal_date当日の株価データを取得（修正版：前営業日も含める）
signal_date_quotes AS (
  SELECT 
    REGEXP_REPLACE(Code, '0$', '') as stock_code,
    Date as signal_date,
    Open as signal_day_open,
    High as signal_day_high,
    Low as signal_day_low,
    Close as signal_day_close,
    Volume as signal_day_volume,
    LAG(Close) OVER (
      PARTITION BY REGEXP_REPLACE(Code, '0$', '') 
      ORDER BY Date
    ) as signal_prev_close
  FROM `kabu-376213.kabu2411.daily_quotes`
  WHERE Date >= target_quote_date  -- 前営業日も含める
    AND Date <= (
      SELECT MIN(tc.Date)
      FROM `kabu-376213.kabu2411.trading_calendar` tc
      WHERE tc.Date > target_quote_date AND tc.HolidayDivision = '1'
    )
    AND Open > 0 AND Close > 0
),

-- 5. シグナルbinを計算
signals_with_bins AS (
  SELECT 
    s.*,
    -- M010_signal_binsからbinを決定
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

-- 6. シグナルデータと当日株価データを結合（修正版）
final_data AS (
  SELECT 
    s.signal_date,
    s.signal_type,
    s.signal_bin,
    s.stock_code,
    s.stock_name,
    s.signal_value,
    
    -- signal_date当日の株価データを使用
    sdq.signal_prev_close as prev_close,
    sdq.signal_day_open as day_open,
    sdq.signal_day_high as day_high,
    sdq.signal_day_low as day_low,
    sdq.signal_day_close as day_close,
    sdq.signal_day_volume as trading_volume,
    
    -- 計算値
    sdq.signal_day_open - sdq.signal_prev_close as prev_close_to_open_gap,
    sdq.signal_day_high - sdq.signal_day_open as open_to_high_gap,
    sdq.signal_day_low - sdq.signal_day_open as open_to_low_gap,
    sdq.signal_day_close - sdq.signal_day_open as open_to_close_gap,
    sdq.signal_day_high - sdq.signal_day_low as daily_range,
    
    -- BUY（LONG）取引結果
    ROUND((sdq.signal_day_close - sdq.signal_day_open) / sdq.signal_day_open * 100, 4) as buy_profit_rate,
    CASE WHEN sdq.signal_day_close > sdq.signal_day_open THEN TRUE ELSE FALSE END as buy_is_win,
    
    -- SELL（SHORT）取引結果  
    ROUND((sdq.signal_day_open - sdq.signal_day_close) / sdq.signal_day_open * 100, 4) as sell_profit_rate,
    CASE WHEN sdq.signal_day_open > sdq.signal_day_close THEN TRUE ELSE FALSE END as sell_is_win,
    
    CURRENT_TIMESTAMP() as created_at
    
  FROM signals_with_bins s
  -- signal_date当日の株価データと結合（signal_dateで絞り込み）
  INNER JOIN signal_date_quotes sdq
    ON s.stock_code = sdq.stock_code 
    AND s.signal_date = sdq.signal_date
  WHERE s.signal_bin IS NOT NULL
    AND sdq.signal_day_open > 0 AND sdq.signal_day_close > 0
    AND sdq.signal_prev_close IS NOT NULL
    AND sdq.signal_date = s.signal_date  -- signal_dateで絞り込み
)

-- BUY取引結果
SELECT 
  signal_date,
  signal_type,
  signal_bin,
  'BUY' as trade_type,
  stock_code,
  stock_name,
  signal_value,
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
  buy_profit_rate as baseline_profit_rate,
  buy_is_win as is_win,
  trading_volume,
  created_at
FROM final_data

UNION ALL

-- SELL取引結果
SELECT 
  signal_date,
  signal_type,
  signal_bin,
  'SELL' as trade_type,
  stock_code,
  stock_name,
  signal_value,
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
  sell_profit_rate as baseline_profit_rate,
  sell_is_win as is_win,
  trading_volume,
  created_at
FROM final_data;

-- ============================================================================
-- 処理完了確認
-- ============================================================================

SELECT 
  '✅ D010増分投入完了' as status,
  target_quote_date as processed_date,
  COUNT(*) as total_inserted,
  COUNT(DISTINCT signal_date) as signal_date,
  COUNT(DISTINCT signal_type) as signal_types,
  COUNT(DISTINCT stock_code) as stocks,
  CURRENT_TIMESTAMP() as end_time
FROM `kabu-376213.kabu2411.D010_basic_results`
WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE);