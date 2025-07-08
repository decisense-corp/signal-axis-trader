/*
ファイル: 02_insert_learning_period_data_all37_fixed.sql
説明: D010_basic_results 学習期間データ投入（37指標一括・1日ずれバグ修正版）
作成日: 2025年7月4日
修正内容: 株価データをsignal_date当日から取得（quote_date→signal_date）
対象期間: 2022年7月1日〜2024年6月30日（学習期間）
目的: 37指標×学習期間の正しいデータ一括投入
実行時間: 約15-20分予想
*/

-- ============================================================================
-- 学習期間データ投入（37指標一括・1日ずれバグ修正版）
-- ============================================================================

-- 処理開始メッセージ
SELECT 
  '🚀 学習期間データ投入開始（37指標一括・1日ずれバグ修正版）' as message,
  '修正内容: 株価データをsignal_date当日から取得' as fix_description,
  '学習期間: 2022年7月1日〜2024年6月30日' as target_period,
  '対象: 37指標×BUY/SELL×全銘柄（一括投入）' as target_scope,
  '予想レコード数: 約2,500万レコード' as estimated_records,
  CURRENT_TIMESTAMP() as start_time;

-- ============================================================================
-- 事前確認
-- ============================================================================

-- テーブル存在確認
SELECT 
  'Step 1: 事前確認' as check_step,
  (SELECT COUNT(*) FROM `kabu-376213.kabu2411.M010_signal_bins`) as M010_records_should_be_740,
  (SELECT COUNT(DISTINCT signal_type) FROM `kabu-376213.kabu2411.M010_signal_bins`) as signal_types_should_be_37,
  (SELECT COUNT(*) FROM `kabu-376213.kabu2411.D010_basic_results`) as current_records_should_be_0;

-- ============================================================================
-- 学習期間データ投入実行（37指標一括）
-- ============================================================================

INSERT INTO `kabu-376213.kabu2411.D010_basic_results`
WITH 
-- 1. 株価データ準備（シグナル計算用：quote_date、取引用：signal_date）
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
  WHERE dq.Date >= '2022-07-01' AND dq.Date <= '2024-06-30'  -- 学習期間
    AND dq.Open > 0 AND dq.Close > 0  -- 基本品質チェック
),

-- 2. シグナル日付計算とシグナル値計算（修正不要部分）
signal_calculations AS (
  SELECT 
    q.stock_code,
    mts.company_name as stock_name,
    q.quote_date,
    -- signal_date計算（修正不要：翌営業日取得）
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
    
    -- 移動平均計算（シグナル値用：quote_dateベース、修正不要）
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

-- 3. 37指標のシグナル生成（修正不要：quote_dateベースで正しい）
all_signals AS (

  -- ==================== Price系 9指標 ====================
  
  -- Close Change Rate
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Close Change Rate' as signal_type,
    ROUND((quote_close - prev_close_for_signal) / prev_close_for_signal * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE prev_close_for_signal > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  -- Close to Prev Close Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Close to Prev Close Ratio' as signal_type,
    ROUND(quote_close / prev_close_for_signal * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE prev_close_for_signal > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  -- Close MA3 Deviation
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Close MA3 Deviation' as signal_type,
    ROUND(quote_close / ma3_close * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma3_close > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  -- Close MA5 Deviation  
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Close MA5 Deviation' as signal_type,
    ROUND(quote_close / ma5_close * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma5_close > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  -- Close MA10 Deviation
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Close MA10 Deviation' as signal_type, 
    ROUND(quote_close / ma10_close * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma10_close > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  -- Close to MAX20 Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Close to MAX20 Ratio' as signal_type,
    ROUND(quote_close / max20_close * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE max20_close > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  -- Close to MIN20 Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Close to MIN20 Ratio' as signal_type,
    ROUND(quote_close / min20_close * 100, 4) as signal_value  
  FROM signal_calculations 
  WHERE min20_close > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  -- Close to Open Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Close to Open Ratio' as signal_type,
    ROUND(quote_close / quote_open * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_open > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  -- Close Volatility
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Close Volatility' as signal_type,
    ROUND(SAFE_DIVIDE(stddev20_close, ma20_close) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma20_close > 0 AND stddev20_close IS NOT NULL AND signal_date IS NOT NULL

  -- ==================== PriceRange系 5指標 ====================
  
  UNION ALL
  
  -- Close to Range Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Close to Range Ratio' as signal_type,
    ROUND(SAFE_DIVIDE(quote_close - quote_low, quote_high - quote_low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > quote_low AND signal_date IS NOT NULL
  
  UNION ALL
  
  -- High to Close Drop Rate
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'High to Close Drop Rate' as signal_type,
    ROUND(SAFE_DIVIDE(quote_high - quote_close, quote_high - quote_low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > quote_low AND signal_date IS NOT NULL
  
  UNION ALL
  
  -- Close to Low Rise Rate
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Close to Low Rise Rate' as signal_type,
    ROUND(SAFE_DIVIDE(quote_close - quote_low, quote_high - quote_low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > quote_low AND signal_date IS NOT NULL
  
  UNION ALL
  
  -- High to Close Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'High to Close Ratio' as signal_type,
    ROUND(quote_close / quote_high * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  -- Close to Low Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Close to Low Ratio' as signal_type,
    ROUND(quote_close / quote_low * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_low > 0 AND signal_date IS NOT NULL

  -- ==================== OpenClose系 3指標 ====================
  
  UNION ALL
  
  -- Open to Close Change Rate
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Open to Close Change Rate' as signal_type,
    ROUND((quote_close - quote_open) / quote_open * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_open > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  -- Open Close Range Efficiency
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Open Close Range Efficiency' as signal_type,
    ROUND(SAFE_DIVIDE(quote_close - quote_open, quote_high - quote_low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > quote_low AND signal_date IS NOT NULL

  -- ==================== Open系 3指標 ====================
  
  UNION ALL
  
  -- Open to Range Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Open to Range Ratio' as signal_type,
    ROUND(SAFE_DIVIDE(quote_open - quote_low, quote_high - quote_low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > quote_low AND signal_date IS NOT NULL
  
  UNION ALL
  
  -- High to Open Drop Rate
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'High to Open Drop Rate' as signal_type,
    ROUND(SAFE_DIVIDE(quote_high - quote_open, quote_high - quote_low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > quote_low AND signal_date IS NOT NULL
  
  UNION ALL
  
  -- Open to Low Rise Rate
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Open to Low Rise Rate' as signal_type,
    ROUND(SAFE_DIVIDE(quote_open - quote_low, quote_high - quote_low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > quote_low AND signal_date IS NOT NULL

  -- ==================== Volume系 4指標 ====================
  
  UNION ALL
  
  -- Volume to Prev Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Volume to Prev Ratio' as signal_type,
    ROUND(quote_volume / prev_volume_for_signal * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE prev_volume_for_signal > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  -- Volume MA3 Deviation
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Volume MA3 Deviation' as signal_type,
    ROUND(quote_volume / ma3_volume * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma3_volume > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  -- Volume MA5 Deviation
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Volume MA5 Deviation' as signal_type,
    ROUND(quote_volume / ma5_volume * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma5_volume > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  -- Volume MA10 Deviation
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Volume MA10 Deviation' as signal_type,
    ROUND(quote_volume / ma10_volume * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma10_volume > 0 AND signal_date IS NOT NULL

  -- ==================== Value系 4指標 ====================
  
  UNION ALL
  
  -- Value to Prev Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Value to Prev Ratio' as signal_type,
    ROUND(quote_value / prev_value_for_signal * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE prev_value_for_signal > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  -- Value MA3 Deviation
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Value MA3 Deviation' as signal_type,
    ROUND(quote_value / ma3_value * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma3_value > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  -- Value MA5 Deviation
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Value MA5 Deviation' as signal_type,
    ROUND(quote_value / ma5_value * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma5_value > 0 AND signal_date IS NOT NULL
  
  UNION ALL
  
  -- Value MA10 Deviation
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    'Value MA10 Deviation' as signal_type,
    ROUND(quote_value / ma10_value * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma10_value > 0 AND signal_date IS NOT NULL

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
),

-- 4. 🔧 修正部分：signal_date当日の株価データを取得
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
  WHERE Date >= '2022-07-05' AND Date <= '2024-06-30'  -- signal_date範囲
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

-- 6. 🔧 修正部分：シグナルデータと当日株価データを結合
final_data AS (
  SELECT 
    s.signal_date,
    s.signal_type,
    s.signal_bin,
    s.stock_code,
    s.stock_name,
    s.signal_value,
    
    -- 🔧 修正：signal_date当日の株価データを使用
    sdq.signal_prev_close as prev_close,
    sdq.signal_day_open as day_open,
    sdq.signal_day_high as day_high,
    sdq.signal_day_low as day_low,
    sdq.signal_day_close as day_close,
    sdq.signal_day_volume as trading_volume,
    
    -- 計算値（修正後の値で再計算）
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
  -- 🔧 修正：signal_date当日の株価データと結合
  INNER JOIN signal_date_quotes sdq
    ON s.stock_code = sdq.stock_code 
    AND s.signal_date = sdq.signal_date
  WHERE s.signal_bin IS NOT NULL
    AND sdq.signal_day_open > 0 AND sdq.signal_day_close > 0
    AND sdq.signal_prev_close IS NOT NULL
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
-- 学習期間投入完了確認
-- ============================================================================

SELECT 
  '✅ 学習期間データ投入完了（37指標一括・1日ずれバグ修正版）' as status,
  COUNT(*) as learning_period_records,
  COUNT(DISTINCT signal_type) as signal_types_should_be_37,
  COUNT(DISTINCT stock_code) as stock_count,
  COUNT(DISTINCT trade_type) as trade_types,
  MIN(signal_date) as min_date,
  MAX(signal_date) as max_date,
  '🔧 修正完了: signal_date当日の株価データを正しく使用' as bug_fix_status,
  '次: 検証期間投入SQL実行' as next_action
FROM `kabu-376213.kabu2411.D010_basic_results`;

-- 1日ずれ修正確認（サンプル検証）
SELECT 
  '🔍 1日ずれ修正確認（サンプル）' as check_purpose,
  signal_date,
  stock_code,
  signal_type,
  prev_close,
  day_open,
  day_close,
  baseline_profit_rate,
  '修正後: signal_date当日の株価データ使用' as confirmation
FROM `kabu-376213.kabu2411.D010_basic_results`
WHERE stock_code = '8354'  -- サンプル銘柄
  AND signal_date >= '2024-06-25'  -- 最新数日分
ORDER BY signal_date DESC, signal_type
LIMIT 5;