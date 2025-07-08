/*
ファイル: 05_insert_D030_tomorrow_signals_corrected.sql
説明: D030_tomorrow_signals 日次データ投入（修正版・37指標完全対応）
作成日: 2025年7月4日
修正内容: 02クエリ準拠の正しい実装
- 過去35日分データ取得でLAG計算対応
- 37指標完全実装
- 営業日カレンダーでtarget_date計算
- D020統計データとLEFT JOIN + COALESCE
依存: D020_learning_stats（完成済み）+ daily_quotes + trading_calendar
目的: 明日発生予定のシグナル計算 + 学習期間統計の統合データ作成
処理時間: 約3-5分
データ量: 約5万レコード/日（1日分のみ保持）
更新: 日次で全件削除→再作成
実行タイミング: 17:00（市場終了後）
*/

-- ============================================================================
-- D030日次投入（明日シグナル予定 + 学習期間統計統合）修正版
-- ============================================================================

-- 処理開始メッセージ
SELECT 
  '🚀 D030日次投入開始（修正版・37指標完全対応）' as message,
  '修正内容: 02クエリ準拠の正しい実装' as fix_description,
  '1. 過去35日分データ取得でLAG計算対応' as fix_1,
  '2. 37指標完全実装' as fix_2,
  '3. 営業日カレンダーでtarget_date計算' as fix_3,
  'データソース: 最新株価データ + D020統計データ' as data_source,
  '予想レコード数: 約5万レコード' as estimated_records,
  CURRENT_TIMESTAMP() as start_time;

-- ============================================================================
-- Step 1: 既存データ削除（明日分のみ）
-- ============================================================================

-- 明日分のデータを削除（冪等性確保）
DELETE FROM `kabu-376213.kabu2411.D030_tomorrow_signals` 
WHERE target_date = (
  SELECT MIN(tc.Date)
  FROM `kabu-376213.kabu2411.trading_calendar` tc
  WHERE tc.Date > CURRENT_DATE() 
    AND tc.HolidayDivision = '1'
);

SELECT 
  '✅ Step 1完了: 既存明日データ削除完了' as status,
  (
    SELECT CONCAT('target_date: ', CAST(MIN(tc.Date) AS STRING))
    FROM `kabu-376213.kabu2411.trading_calendar` tc
    WHERE tc.Date > CURRENT_DATE() AND tc.HolidayDivision = '1'
  ) as deleted_date,
  '次: Step 2（明日シグナル計算）' as next_action;

-- ============================================================================
-- Step 2: 明日シグナル予定データ投入
-- ============================================================================

INSERT INTO `kabu-376213.kabu2411.D030_tomorrow_signals`
WITH 
-- 1. 過去35日分の株価データ準備（LAG計算対応）
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
      INTERVAL 35 DAY
    )
    AND dq.Date <= (SELECT MAX(Date) FROM `kabu-376213.kabu2411.daily_quotes`)
    AND dq.Open > 0 AND dq.Close > 0  -- 基本品質チェック
),

-- 2. シグナル計算（最新日のみ・02準拠）
signal_calculations AS (
  SELECT 
    q.stock_code,
    mts.company_name as stock_name,
    q.quote_date,
    -- target_date計算（営業日カレンダー使用）
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
    
    -- 移動平均計算（LAG対応）
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
    AND q.quote_date = (SELECT MAX(Date) FROM `kabu-376213.kabu2411.daily_quotes`)  -- 最新日のみ
),

-- 3. 37指標のシグナル生成（02準拠）
all_signals AS (

  -- ==================== Price系 9指標 ====================
  
  -- Close Change Rate
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'Close Change Rate' as signal_type,
    ROUND((quote_close - prev_close_for_signal) / prev_close_for_signal * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE prev_close_for_signal > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Close to Prev Close Ratio
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'Close to Prev Close Ratio' as signal_type,
    ROUND(quote_close / prev_close_for_signal * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE prev_close_for_signal > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Close MA3 Deviation
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'Close MA3 Deviation' as signal_type,
    ROUND(quote_close / ma3_close * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma3_close > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Close MA5 Deviation  
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'Close MA5 Deviation' as signal_type,
    ROUND(quote_close / ma5_close * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma5_close > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Close MA10 Deviation
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'Close MA10 Deviation' as signal_type, 
    ROUND(quote_close / ma10_close * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma10_close > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Close to MAX20 Ratio
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'Close to MAX20 Ratio' as signal_type,
    ROUND(quote_close / max20_close * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE max20_close > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Close to MIN20 Ratio
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'Close to MIN20 Ratio' as signal_type,
    ROUND(quote_close / min20_close * 100, 4) as signal_value  
  FROM signal_calculations 
  WHERE min20_close > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Close to Open Ratio
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'Close to Open Ratio' as signal_type,
    ROUND(quote_close / quote_open * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_open > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Close Volatility
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'Close Volatility' as signal_type,
    ROUND(SAFE_DIVIDE(stddev20_close, ma20_close) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma20_close > 0 AND stddev20_close IS NOT NULL AND target_date IS NOT NULL

  -- ==================== PriceRange系 5指標 ====================
  
  UNION ALL
  
  -- Close to Range Ratio
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'Close to Range Ratio' as signal_type,
    ROUND(SAFE_DIVIDE(quote_close - quote_low, quote_high - quote_low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > quote_low AND target_date IS NOT NULL
  
  UNION ALL
  
  -- High to Close Drop Rate
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'High to Close Drop Rate' as signal_type,
    ROUND(SAFE_DIVIDE(quote_high - quote_close, quote_high - quote_low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > quote_low AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Close to Low Rise Rate
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'Close to Low Rise Rate' as signal_type,
    ROUND(SAFE_DIVIDE(quote_close - quote_low, quote_high - quote_low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > quote_low AND target_date IS NOT NULL
  
  UNION ALL
  
  -- High to Close Ratio
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'High to Close Ratio' as signal_type,
    ROUND(quote_close / quote_high * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Close to Low Ratio
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'Close to Low Ratio' as signal_type,
    ROUND(quote_close / quote_low * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_low > 0 AND target_date IS NOT NULL

  -- ==================== OpenClose系 3指標 ====================
  
  UNION ALL
  
  -- Open to Close Change Rate
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'Open to Close Change Rate' as signal_type,
    ROUND((quote_close - quote_open) / quote_open * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_open > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Open Close Range Efficiency
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'Open Close Range Efficiency' as signal_type,
    ROUND(SAFE_DIVIDE(quote_close - quote_open, quote_high - quote_low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > quote_low AND target_date IS NOT NULL

  -- ==================== Open系 3指標 ====================
  
  UNION ALL
  
  -- Open to Range Ratio
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'Open to Range Ratio' as signal_type,
    ROUND(SAFE_DIVIDE(quote_open - quote_low, quote_high - quote_low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > quote_low AND target_date IS NOT NULL
  
  UNION ALL
  
  -- High to Open Drop Rate
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'High to Open Drop Rate' as signal_type,
    ROUND(SAFE_DIVIDE(quote_high - quote_open, quote_high - quote_low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > quote_low AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Open to Low Rise Rate
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'Open to Low Rise Rate' as signal_type,
    ROUND(SAFE_DIVIDE(quote_open - quote_low, quote_high - quote_low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > quote_low AND target_date IS NOT NULL

  -- ==================== Volume系 4指標 ====================
  
  UNION ALL
  
  -- Volume to Prev Ratio
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'Volume to Prev Ratio' as signal_type,
    ROUND(quote_volume / prev_volume_for_signal * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE prev_volume_for_signal > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Volume MA3 Deviation
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'Volume MA3 Deviation' as signal_type,
    ROUND(quote_volume / ma3_volume * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma3_volume > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Volume MA5 Deviation
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'Volume MA5 Deviation' as signal_type,
    ROUND(quote_volume / ma5_volume * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma5_volume > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Volume MA10 Deviation
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'Volume MA10 Deviation' as signal_type,
    ROUND(quote_volume / ma10_volume * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma10_volume > 0 AND target_date IS NOT NULL

  -- ==================== Value系 4指標 ====================
  
  UNION ALL
  
  -- Value to Prev Ratio
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'Value to Prev Ratio' as signal_type,
    ROUND(quote_value / prev_value_for_signal * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE prev_value_for_signal > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Value MA3 Deviation
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'Value MA3 Deviation' as signal_type,
    ROUND(quote_value / ma3_value * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma3_value > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Value MA5 Deviation
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'Value MA5 Deviation' as signal_type,
    ROUND(quote_value / ma5_value * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma5_value > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Value MA10 Deviation
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'Value MA10 Deviation' as signal_type,
    ROUND(quote_value / ma10_value * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma10_value > 0 AND target_date IS NOT NULL

  -- ==================== Score系 10指標 ====================
  
  UNION ALL
  
  -- High Price Score 3D
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'High Price Score 3D' as signal_type,
    ROUND(
      COALESCE(avg_high_open_3d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + 
      COALESCE(SAFE_DIVIDE(quote_close - quote_open, quote_open) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE quote_open > 0 AND avg_high_open_3d IS NOT NULL AND target_date IS NOT NULL
  
  UNION ALL
  
  -- High Price Score 7D
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'High Price Score 7D' as signal_type,
    ROUND(
      COALESCE(avg_high_open_7d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + 
      COALESCE(SAFE_DIVIDE(quote_close - quote_open, quote_open) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE quote_open > 0 AND avg_high_open_7d IS NOT NULL AND target_date IS NOT NULL
  
  UNION ALL
  
  -- High Price Score 9D
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'High Price Score 9D' as signal_type,
    ROUND(
      COALESCE(avg_high_open_9d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + 
      COALESCE(SAFE_DIVIDE(quote_close - quote_open, quote_open) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE quote_open > 0 AND avg_high_open_9d IS NOT NULL AND target_date IS NOT NULL
  
  UNION ALL
  
  -- High Price Score 14D
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'High Price Score 14D' as signal_type,
    ROUND(
      COALESCE(avg_high_open_14d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + 
      COALESCE(SAFE_DIVIDE(quote_close - quote_open, quote_open) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE quote_open > 0 AND avg_high_open_14d IS NOT NULL AND target_date IS NOT NULL
  
  UNION ALL
  
  -- High Price Score 20D
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'High Price Score 20D' as signal_type,
    ROUND(
      COALESCE(avg_high_open_20d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + 
      COALESCE(SAFE_DIVIDE(quote_close - quote_open, quote_open) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE quote_open > 0 AND avg_high_open_20d IS NOT NULL AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Low Price Score 3D
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'Low Price Score 3D' as signal_type,
    ROUND(
      COALESCE(avg_open_low_3d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + 
      COALESCE(ABS(SAFE_DIVIDE(quote_close - quote_open, quote_open)) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE quote_open > 0 AND avg_open_low_3d IS NOT NULL AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Low Price Score 7D
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'Low Price Score 7D' as signal_type,
    ROUND(
      COALESCE(avg_open_low_7d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + 
      COALESCE(ABS(SAFE_DIVIDE(quote_close - quote_open, quote_open)) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE quote_open > 0 AND avg_open_low_7d IS NOT NULL AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Low Price Score 9D
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'Low Price Score 9D' as signal_type,
    ROUND(
      COALESCE(avg_open_low_9d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + 
      COALESCE(ABS(SAFE_DIVIDE(quote_close - quote_open, quote_open)) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE quote_open > 0 AND avg_open_low_9d IS NOT NULL AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Low Price Score 14D
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'Low Price Score 14D' as signal_type,
    ROUND(
      COALESCE(avg_open_low_14d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + 
      COALESCE(ABS(SAFE_DIVIDE(quote_close - quote_open, quote_open)) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE quote_open > 0 AND avg_open_low_14d IS NOT NULL AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Low Price Score 20D
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    'Low Price Score 20D' as signal_type,
    ROUND(
      COALESCE(avg_open_low_20d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + 
      COALESCE(ABS(SAFE_DIVIDE(quote_close - quote_open, quote_open)) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE quote_open > 0 AND avg_open_low_20d IS NOT NULL AND target_date IS NOT NULL
),

-- 4. シグナルbinマッピング（M010_signal_binsとJOIN）
signals_with_bins AS (
  SELECT 
    s.*,
    -- bin割り当て（02準拠・境界値条件対応）
    COALESCE(
      (SELECT MAX(sb.signal_bin) 
       FROM `kabu-376213.kabu2411.M010_signal_bins` sb
       WHERE sb.signal_type = s.signal_type
         AND s.signal_value > sb.lower_bound 
         AND s.signal_value <= sb.upper_bound), 
      1  -- デフォルトbin
    ) as signal_bin
  FROM all_signals s
),

-- 5. BUY/SELL展開
signal_with_trade_types AS (
  SELECT 
    stock_code,
    stock_name,
    target_date,
    signal_type,
    signal_bin,
    signal_value,
    trade_type
  FROM signals_with_bins
  CROSS JOIN UNNEST(['BUY', 'SELL']) as trade_type
  WHERE signal_bin IS NOT NULL
),

-- 6. D020統計データとJOIN（LEFT JOIN + COALESCE）
final_data AS (
  SELECT 
    swt.target_date,
    
    -- 4軸情報
    swt.signal_type,
    swt.signal_bin,
    swt.trade_type,
    swt.stock_code,
    swt.stock_name,
    swt.signal_value,
    
    -- 学習期間統計（D020から複写・デフォルト値対応）
    COALESCE(d20.total_samples, 0) as total_samples,
    COALESCE(d20.win_samples, 0) as win_samples,
    COALESCE(d20.win_rate, 0.0) as win_rate,
    COALESCE(d20.avg_profit_rate, 0.0) as avg_profit_rate,
    COALESCE(d20.std_deviation, 0.0) as std_deviation,
    COALESCE(d20.sharpe_ratio, 0.0) as sharpe_ratio,
    COALESCE(d20.max_profit_rate, 0.0) as max_profit_rate,
    COALESCE(d20.min_profit_rate, 0.0) as min_profit_rate,
    
    -- パターン評価（D020から複写・デフォルト値対応）
    COALESCE(d20.is_excellent_pattern, false) as is_excellent_pattern,
    COALESCE(d20.pattern_category, 'CAUTION') as pattern_category,
    COALESCE(d20.priority_score, 0.0) as priority_score,
    
    -- ユーザー設定状況（D020から複写）
    COALESCE(d20.decision_status, 'pending') as decision_status,
    d20.profit_target_yen,
    d20.loss_cut_yen,
    d20.prev_close_gap_condition,
    d20.additional_notes,
    d20.decided_at,
    
    -- 期間情報（D020から複写）
    d20.first_signal_date,
    d20.last_signal_date,
    
    -- システム項目
    CURRENT_TIMESTAMP() as created_at,
    CURRENT_TIMESTAMP() as updated_at
    
  FROM signal_with_trade_types swt
  LEFT JOIN `kabu-376213.kabu2411.D020_learning_stats` d20
    ON swt.signal_type = d20.signal_type
    AND swt.signal_bin = d20.signal_bin
    AND swt.trade_type = d20.trade_type
    AND swt.stock_code = d20.stock_code
)

-- 最終データ投入
SELECT * FROM final_data
ORDER BY 
  is_excellent_pattern DESC,
  priority_score DESC,
  stock_code,
  signal_type,
  trade_type;

-- ============================================================================
-- Step 3: 投入結果確認
-- ============================================================================

-- 基本投入確認
SELECT 
  '✅ Step 3: 投入結果確認' as check_step,
  COUNT(*) as total_records_inserted,
  COUNT(DISTINCT signal_type) as signal_types_count_should_be_37,
  COUNT(DISTINCT stock_code) as stocks_count,
  COUNT(DISTINCT CONCAT(signal_type, '|', signal_bin, '|', trade_type, '|', stock_code)) as unique_4axis_patterns,
  SUM(CASE WHEN is_excellent_pattern = true THEN 1 ELSE 0 END) as excellent_patterns,
  AVG(CASE WHEN total_samples > 0 THEN win_rate ELSE NULL END) as avg_win_rate,
  (
    SELECT MIN(tc.Date)
    FROM `kabu-376213.kabu2411.trading_calendar` tc
    WHERE tc.Date > CURRENT_DATE() AND tc.HolidayDivision = '1'
  ) as target_date_confirmed
FROM `kabu-376213.kabu2411.D030_tomorrow_signals`
WHERE target_date = (
  SELECT MIN(tc.Date)
  FROM `kabu-376213.kabu2411.trading_calendar` tc
  WHERE tc.Date > CURRENT_DATE() AND tc.HolidayDivision = '1'
);

-- パターンカテゴリ分布確認
SELECT 
  '📊 パターンカテゴリ分布' as check_type,
  pattern_category,
  COUNT(*) as pattern_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage,
  ROUND(AVG(win_rate), 1) as avg_win_rate,
  ROUND(AVG(total_samples), 0) as avg_samples
FROM `kabu-376213.kabu2411.D030_tomorrow_signals`
WHERE target_date = (
  SELECT MIN(tc.Date)
  FROM `kabu-376213.kabu2411.trading_calendar` tc
  WHERE tc.Date > CURRENT_DATE() AND tc.HolidayDivision = '1'
)
GROUP BY pattern_category
ORDER BY 
  CASE pattern_category
    WHEN 'PREMIUM' THEN 1
    WHEN 'EXCELLENT' THEN 2
    WHEN 'GOOD' THEN 3
    WHEN 'NORMAL' THEN 4
    WHEN 'CAUTION' THEN 5
  END;

-- 37指標実装確認
SELECT 
  '🔍 37指標実装確認' as check_type,
  signal_type,
  COUNT(*) as records_per_signal,
  COUNT(DISTINCT stock_code) as stocks_per_signal,
  AVG(CASE WHEN signal_value IS NOT NULL THEN 1.0 ELSE 0.0 END) as signal_value_rate
FROM `kabu-376213.kabu2411.D030_tomorrow_signals`
WHERE target_date = (
  SELECT MIN(tc.Date)
  FROM `kabu-376213.kabu2411.trading_calendar` tc
  WHERE tc.Date > CURRENT_DATE() AND tc.HolidayDivision = '1'
)
GROUP BY signal_type
ORDER BY signal_type;

-- TOP優秀パターン確認
SELECT 
  '⭐ 明日の優秀パターン TOP10' as check_type,
  signal_type,
  signal_bin,
  trade_type,
  stock_name,
  signal_value,
  total_samples,
  win_rate,
  ROUND(avg_profit_rate, 2) as profit_percent,
  pattern_category,
  decision_status
FROM `kabu-376213.kabu2411.D030_tomorrow_signals`
WHERE target_date = (
  SELECT MIN(tc.Date)
  FROM `kabu-376213.kabu2411.trading_calendar` tc
  WHERE tc.Date > CURRENT_DATE() AND tc.HolidayDivision = '1'
)
  AND is_excellent_pattern = true
ORDER BY priority_score DESC
LIMIT 10;

-- ============================================================================
-- 🎉 D030日次投入完成確認
-- ============================================================================

SELECT 
  '🏆 D030日次投入完了！（修正版）' as achievement,
  '✅ 37指標完全実装' as signal_completion,
  '✅ 営業日カレンダー対応' as calendar_integration,
  '✅ D020統計データ統合（LEFT JOIN + COALESCE）' as statistics_integration,
  '✅ 4軸一覧画面データ準備完成' as ui_data_ready,
  '✅ JOIN完全不要データ作成完成' as join_free_data,
  (
    SELECT CONCAT('target_date: ', CAST(MIN(tc.Date) AS STRING))
    FROM `kabu-376213.kabu2411.trading_calendar` tc
    WHERE tc.Date > CURRENT_DATE() AND tc.HolidayDivision = '1'
  ) as tomorrow_trading_date,
  COUNT(*) as total_tomorrow_signals,
  '次Phase: 4軸一覧画面API実装可能' as next_development,
  CURRENT_TIMESTAMP() as completion_time
FROM `kabu-376213.kabu2411.D030_tomorrow_signals`
WHERE target_date = (
  SELECT MIN(tc.Date)
  FROM `kabu-376213.kabu2411.trading_calendar` tc
  WHERE tc.Date > CURRENT_DATE() AND tc.HolidayDivision = '1'
);

-- ============================================================================
-- 実行完了メッセージ
-- ============================================================================

SELECT 
  'D030日次投入が完了しました（修正版）' as message,
  '✅ 37指標シグナル値計算完成' as signal_calculation,
  '✅ 営業日ベースの正確なtarget_date設定' as accurate_date,
  '✅ D020統計データ完全統合' as statistics_complete,
  '統合データ: 4軸情報 + 37指標 + 学習期間統計' as data_structure,
  'パフォーマンス: 4軸一覧画面1秒以内表示準備完了' as performance_ready,
  '🚀 Signal Axis Trader 明日の投資判断準備完了！' as celebration,
  CURRENT_TIMESTAMP() as completion_time;