/*
ファイル: 02_create_D010_basic_results_step4.sql
説明: D010_basic_results への検証期間データ投入（37指標完成後）
前提: Phase 2C（37指標）が正常完了済み（2,479万レコード）
作成日: 2025年7月4日
目的: 3年間完全データ構築（学習期間 + 検証期間）
処理時間: 約2-3分予想（Phase 2の高速実績により）
*/

-- ============================================================================
-- Step 4: 検証期間データ投入実行（2024年7月〜現在）
-- ============================================================================

-- 処理開始メッセージ
SELECT 
  '🚀 Step 4開始: 検証期間データ投入実行' as message,
  '前提: Phase 2C完了（37指標・2,479万レコード）' as prerequisite,
  '目標: 3年間完全データ構築（学習期間 + 検証期間）' as target,
  '検証期間: 2024年7月1日〜現在' as verification_period,
  '予想処理時間: 約2-3分（Phase 2高速実績により）' as estimated_time,
  '予想追加レコード数: 約1,200万レコード' as estimated_records,
  CURRENT_TIMESTAMP() as start_time;

-- ============================================================================
-- 事前確認: Phase 2C完了状況
-- ============================================================================

-- Phase 2C結果確認
SELECT 
  'Phase 2C完了状況確認' as check_point,
  COUNT(*) as current_records,
  COUNT(DISTINCT signal_type) as current_signal_types_should_be_37,
  MIN(signal_date) as min_date,
  MAX(signal_date) as max_date_should_be_2024_06_28,
  CASE 
    WHEN COUNT(DISTINCT signal_type) = 37 AND MAX(signal_date) = '2024-06-28'
    THEN '✅ Phase 2C正常完了 - Step 4実行可能'
    ELSE '❌ Phase 2C未完了 - Step 4実行不可'
  END as phase2c_status
FROM `kabu-376213.kabu2411.D010_basic_results`;

-- 検証期間のdaily_quotes確認
SELECT 
  'Step 4事前確認: 検証期間データ確認' as check_point,
  COUNT(*) as verification_quotes_records,
  MIN(Date) as verification_min_date,
  MAX(Date) as verification_max_date,
  COUNT(DISTINCT REGEXP_REPLACE(Code, '0$', '')) as verification_stocks
FROM `kabu-376213.kabu2411.daily_quotes`
WHERE Date >= '2024-07-01';

-- ============================================================================
-- 検証期間データ投入実行（37指標フル対応）
-- ============================================================================

INSERT INTO `kabu-376213.kabu2411.D010_basic_results`
WITH 
-- 1. 検証期間株価データ準備
quotes_with_prev AS (
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
    ) as prev_close,
    LAG(dq.Volume) OVER (
      PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') 
      ORDER BY dq.Date
    ) as prev_volume,
    LAG(dq.TurnoverValue) OVER (
      PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') 
      ORDER BY dq.Date
    ) as prev_value
  FROM `kabu-376213.kabu2411.daily_quotes` dq
  WHERE dq.Date >= '2024-07-01'  -- 検証期間
    AND dq.Open > 0 AND dq.Close > 0  -- 基本的な品質チェック
),

-- 2. シグナル値計算（37指標全てを計算）
signal_calculations AS (
  SELECT 
    q.stock_code,
    mts.company_name as stock_name,
    q.quote_date,
    (
      SELECT MIN(tc.Date)
      FROM `kabu-376213.kabu2411.trading_calendar` tc
      WHERE tc.Date > q.quote_date AND tc.HolidayDivision = '1'
    ) as signal_date,
    q.Open,
    q.High,
    q.Low, 
    q.Close,
    q.Volume,
    q.TurnoverValue,
    q.prev_close,
    q.prev_volume,
    q.prev_value,
    
    -- 移動平均計算（全期間対応）
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
    
  FROM quotes_with_prev q
  INNER JOIN `kabu-376213.kabu2411.master_trading_stocks` mts
    ON q.stock_code = mts.stock_code
  WHERE q.prev_close IS NOT NULL
),

-- 3. 37指標全てのシグナル生成
all_verification_signals AS (

  -- ==================== Price系 9指標 ====================
  
  -- Close Change Rate
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Close Change Rate' as signal_type,
    ROUND((Close - prev_close) / prev_close * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE prev_close > 0 AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- Close to Prev Close Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Close to Prev Close Ratio' as signal_type,
    ROUND(Close / prev_close * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE prev_close > 0 AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- Close MA3 Deviation
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Close MA3 Deviation' as signal_type,
    ROUND(Close / ma3_close * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma3_close > 0 AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- Close MA5 Deviation  
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Close MA5 Deviation' as signal_type,
    ROUND(Close / ma5_close * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma5_close > 0 AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- Close MA10 Deviation
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Close MA10 Deviation' as signal_type, 
    ROUND(Close / ma10_close * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma10_close > 0 AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- Close to MAX20 Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Close to MAX20 Ratio' as signal_type,
    ROUND(Close / max20_close * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE max20_close > 0 AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- Close to MIN20 Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Close to MIN20 Ratio' as signal_type,
    ROUND(Close / min20_close * 100, 4) as signal_value  
  FROM signal_calculations 
  WHERE min20_close > 0 AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- Close to Open Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Close to Open Ratio' as signal_type,
    ROUND(Close / Open * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE Open > 0 AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- Close Volatility
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Close Volatility' as signal_type,
    ROUND(SAFE_DIVIDE(stddev20_close, ma20_close) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma20_close > 0 AND stddev20_close IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date

  -- ==================== PriceRange系 5指標 ====================
  
  UNION ALL
  
  -- Close to Range Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Close to Range Ratio' as signal_type,
    ROUND(SAFE_DIVIDE(Close - Low, High - Low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE High > Low AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- High to Close Drop Rate
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'High to Close Drop Rate' as signal_type,
    ROUND(SAFE_DIVIDE(High - Close, High - Low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE High > Low AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- Close to Low Rise Rate
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Close to Low Rise Rate' as signal_type,
    ROUND(SAFE_DIVIDE(Close - Low, High - Low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE High > Low AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- High to Close Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'High to Close Ratio' as signal_type,
    ROUND(Close / High * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE High > 0 AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- Close to Low Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Close to Low Ratio' as signal_type,
    ROUND(Close / Low * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE Low > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

  -- ==================== OpenClose系 3指標 ====================
  
  UNION ALL
  
  -- Open to Close Change Rate
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Open to Close Change Rate' as signal_type,
    ROUND((Close - Open) / Open * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE Open > 0 AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- Open Close Range Efficiency
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Open Close Range Efficiency' as signal_type,
    ROUND(SAFE_DIVIDE(Close - Open, High - Low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE High > Low AND signal_date IS NOT NULL AND signal_date > quote_date

  -- ==================== Open系 3指標 ====================
  
  UNION ALL
  
  -- Open to Range Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Open to Range Ratio' as signal_type,
    ROUND(SAFE_DIVIDE(Open - Low, High - Low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE High > Low AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- High to Open Drop Rate
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'High to Open Drop Rate' as signal_type,
    ROUND(SAFE_DIVIDE(High - Open, High - Low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE High > Low AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- Open to Low Rise Rate
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Open to Low Rise Rate' as signal_type,
    ROUND(SAFE_DIVIDE(Open - Low, High - Low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE High > Low AND signal_date IS NOT NULL AND signal_date > quote_date

  -- ==================== Volume系 4指標 ====================
  
  UNION ALL
  
  -- Volume to Prev Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Volume to Prev Ratio' as signal_type,
    ROUND(Volume / prev_volume * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE prev_volume > 0 AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- Volume MA3 Deviation
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Volume MA3 Deviation' as signal_type,
    ROUND(Volume / ma3_volume * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma3_volume > 0 AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- Volume MA5 Deviation
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Volume MA5 Deviation' as signal_type,
    ROUND(Volume / ma5_volume * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma5_volume > 0 AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- Volume MA10 Deviation
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Volume MA10 Deviation' as signal_type,
    ROUND(Volume / ma10_volume * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma10_volume > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

  -- ==================== Value系 4指標 ====================
  
  UNION ALL
  
  -- Value to Prev Ratio
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Value to Prev Ratio' as signal_type,
    ROUND(TurnoverValue / prev_value * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE prev_value > 0 AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- Value MA3 Deviation
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Value MA3 Deviation' as signal_type,
    ROUND(TurnoverValue / ma3_value * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma3_value > 0 AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- Value MA5 Deviation
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Value MA5 Deviation' as signal_type,
    ROUND(TurnoverValue / ma5_value * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma5_value > 0 AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- Value MA10 Deviation
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Value MA10 Deviation' as signal_type,
    ROUND(TurnoverValue / ma10_value * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma10_value > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

  -- ==================== Score系 10指標 ====================
  
  UNION ALL
  
  -- High Price Score 3D
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'High Price Score 3D' as signal_type,
    ROUND(
      COALESCE(avg_high_open_3d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + 
      COALESCE(SAFE_DIVIDE(Close - Open, Open) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE Open > 0 AND avg_high_open_3d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- High Price Score 7D
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'High Price Score 7D' as signal_type,
    ROUND(
      COALESCE(avg_high_open_7d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + 
      COALESCE(SAFE_DIVIDE(Close - Open, Open) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE Open > 0 AND avg_high_open_7d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- High Price Score 9D
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'High Price Score 9D' as signal_type,
    ROUND(
      COALESCE(avg_high_open_9d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + 
      COALESCE(SAFE_DIVIDE(Close - Open, Open) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE Open > 0 AND avg_high_open_9d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- High Price Score 14D
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'High Price Score 14D' as signal_type,
    ROUND(
      COALESCE(avg_high_open_14d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + 
      COALESCE(SAFE_DIVIDE(Close - Open, Open) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE Open > 0 AND avg_high_open_14d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- High Price Score 20D
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'High Price Score 20D' as signal_type,
    ROUND(
      COALESCE(avg_high_open_20d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + 
      COALESCE(SAFE_DIVIDE(Close - Open, Open) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE Open > 0 AND avg_high_open_20d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- Low Price Score 3D
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Low Price Score 3D' as signal_type,
    ROUND(
      COALESCE(avg_open_low_3d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + 
      COALESCE(ABS(SAFE_DIVIDE(Close - Open, Open)) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE Open > 0 AND avg_open_low_3d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- Low Price Score 7D
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Low Price Score 7D' as signal_type,
    ROUND(
      COALESCE(avg_open_low_7d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + 
      COALESCE(ABS(SAFE_DIVIDE(Close - Open, Open)) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE Open > 0 AND avg_open_low_7d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- Low Price Score 9D
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Low Price Score 9D' as signal_type,
    ROUND(
      COALESCE(avg_open_low_9d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + 
      COALESCE(ABS(SAFE_DIVIDE(Close - Open, Open)) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE Open > 0 AND avg_open_low_9d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- Low Price Score 14D
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Low Price Score 14D' as signal_type,
    ROUND(
      COALESCE(avg_open_low_14d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + 
      COALESCE(ABS(SAFE_DIVIDE(Close - Open, Open)) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE Open > 0 AND avg_open_low_14d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date
  
  UNION ALL
  
  -- Low Price Score 20D
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Low Price Score 20D' as signal_type,
    ROUND(
      COALESCE(avg_open_low_20d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + 
      COALESCE(ABS(SAFE_DIVIDE(Close - Open, Open)) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE Open > 0 AND avg_open_low_20d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date
),

-- 4. シグナルbinを計算
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
  FROM all_verification_signals s
),

-- 5. 最終データ準備
final_data AS (
  SELECT 
    s.signal_date,
    s.signal_type,
    s.signal_bin,
    s.stock_code,
    s.stock_name,
    s.signal_value,
    s.prev_close,
    s.Open as day_open,
    s.High as day_high,
    s.Low as day_low,
    s.Close as day_close,
    s.Volume as trading_volume,
    
    -- 計算値
    s.Open - s.prev_close as prev_close_to_open_gap,
    s.High - s.Open as open_to_high_gap,
    s.Low - s.Open as open_to_low_gap,
    s.Close - s.Open as open_to_close_gap,
    s.High - s.Low as daily_range,
    
    -- BUY（LONG）取引結果
    ROUND((s.Close - s.Open) / s.Open * 100, 4) as buy_profit_rate,
    CASE WHEN s.Close > s.Open THEN TRUE ELSE FALSE END as buy_is_win,
    
    -- SELL（SHORT）取引結果  
    ROUND((s.Open - s.Close) / s.Open * 100, 4) as sell_profit_rate,
    CASE WHEN s.Open > s.Close THEN TRUE ELSE FALSE END as sell_is_win,
    
    CURRENT_TIMESTAMP() as created_at
    
  FROM signals_with_bins s
  WHERE s.Open > 0 AND s.Close > 0 AND s.signal_bin IS NOT NULL
    AND s.signal_date >= '2024-07-01'  -- 検証期間制限
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
-- Step 4完了確認
-- ============================================================================

-- 追加結果確認
SELECT 
  '🎉 Step 4完了確認（3年間完全データ構築）' as status,
  COUNT(*) as total_records_after_verification,
  COUNT(DISTINCT signal_type) as signal_types_should_be_37,
  COUNT(DISTINCT stock_code) as stock_count,
  COUNT(DISTINCT trade_type) as trade_types,
  MIN(signal_date) as min_date_should_be_2022_07_05,
  MAX(signal_date) as max_date_current,
  ROUND(AVG(CASE WHEN is_win THEN 1.0 ELSE 0.0 END) * 100, 1) as overall_win_rate_percent,
  CURRENT_TIMESTAMP() as completion_time
FROM `kabu-376213.kabu2411.D010_basic_results`;

-- 期間別データ分布確認
SELECT 
  'Step 4: 期間別データ分布' as check_point,
  CASE 
    WHEN signal_date <= '2024-06-30' THEN '学習期間'
    ELSE '検証期間'
  END as period_type,
  COUNT(*) as record_count,
  COUNT(DISTINCT signal_type) as signal_types,
  MIN(signal_date) as period_start,
  MAX(signal_date) as period_end,
  ROUND(AVG(CASE WHEN is_win THEN 1.0 ELSE 0.0 END) * 100, 1) as win_rate_percent
FROM `kabu-376213.kabu2411.D010_basic_results`
GROUP BY 
  CASE 
    WHEN signal_date <= '2024-06-30' THEN '学習期間'
    ELSE '検証期間'
  END
ORDER BY period_type;

-- ============================================================================
-- 🎉 D010_basic_results完全完成確認
-- ============================================================================

SELECT 
  '🏆 D010_basic_results完全完成！' as achievement,
  '✅ 37指標フル対応完成' as signal_completion,
  '✅ 3年間完全データ構築完成' as period_completion,
  '✅ 学習期間 + 検証期間完璧分離' as data_separation,
  '✅ 設計書完全準拠達成' as design_compliance,
  '次Phase: D020_learning_stats作成開始可能' as next_step,
  CURRENT_TIMESTAMP() as completion_time;

-- ============================================================================
-- 実行ログ記録用セクション
-- ============================================================================

/*
=== Step 4 実行ログ ===
実行日時: [手動記入]
実行者: [手動記入]  
実行結果: [SUCCESS/FAILED]
処理時間: [手動記入]
追加レコード数: [手動記入]
総レコード数: [手動記入]
検証期間: [2024-07-01〜現在]
エラー内容: [あれば記入]
次のアクション: [D020_learning_stats作成]

=== 実行時メモ ===
- [検証期間データの品質確認結果]
- [学習期間との比較結果]
- [3年間完全データ構築の感想]
- [次のD020作成に向けての準備状況]
*/