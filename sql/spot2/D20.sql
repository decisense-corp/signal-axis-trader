/*
ファイル: create_D20_tomorrow_signals_fixed.sql
説明: D20_tomorrow_signals テーブル作成と日次データ投入（修正版）
作成日: 2025年1月15日
修正内容: signal_binの偏り問題を修正
  - 全期間でシグナル計算してからフィルタリング
  - D10と同じロジックでsignal_binを適切に分散
目的: 明日の取引シグナルと全期間統計情報を統合
特徴:
  - 流動性情報（前営業日の出来高・売買代金・売買可能株数）を含む
  - 全期間の統計情報を毎回計算（学習期間の概念を撤廃）
  - シンプルな構造でパフォーマンス重視
実行時間: 約5-10分（全期間統計計算含む）
*/

-- ============================================================================
-- Part 1: テーブル作成とベース構造
-- ============================================================================

-- 処理開始メッセージ
SELECT 
  '🚀 D20_tomorrow_signals テーブル作成開始' as message,
  '特徴: 流動性情報 + 全期間統計' as features,
  '統計期間: 全期間（学習期間の概念なし）' as statistics_period,
  CURRENT_TIMESTAMP() as start_time;

-- 既存テーブル削除（存在する場合）
DROP TABLE IF EXISTS `kabu-376213.kabu2411.D20_tomorrow_signals`;

-- 新テーブル作成
CREATE TABLE `kabu-376213.kabu2411.D20_tomorrow_signals` (
  -- 基本情報
  target_date DATE NOT NULL,             -- 取引予定日（明日）
  signal_type STRING NOT NULL,           -- シグナル種別（37指標）
  signal_bin INT64 NOT NULL,             -- シグナル分位（1-20）
  trade_type STRING NOT NULL,            -- 取引種別（'BUY'/'SELL'）
  stock_code STRING NOT NULL,            -- 銘柄コード
  stock_name STRING,                     -- 銘柄名
  signal_value FLOAT64,                  -- シグナル値
  
  -- 流動性情報（新規追加）
  prev_close FLOAT64,                    -- 前日終値（シグナル計算元）
  prev_volume FLOAT64,                   -- 前営業日の出来高
  prev_trading_value FLOAT64,            -- 前営業日の売買代金
  tradable_shares INT64,                 -- 売買可能株数（前営業日出来高の1%）
  
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
  '次: 明日シグナルデータ投入' as next_step;

-- ============================================================================
-- Part 2: 日次データ投入（全件削除→再作成）- ベース構造
-- ============================================================================

-- 既存データ全削除
DELETE FROM `kabu-376213.kabu2411.D20_tomorrow_signals` WHERE TRUE;

-- データ投入開始（CTEベース構造）
INSERT INTO `kabu-376213.kabu2411.D20_tomorrow_signals`
WITH 
-- 1. 株価データ準備（最新日から35日前まで）
latest_quotes AS (
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
    AND dq.Open > 0 AND dq.Close > 0
),

-- 2. シグナル計算（全期間分）
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
    
    -- 移動平均等の計算（シグナル用）
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
    
    -- Score系用
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
    
  FROM latest_quotes q
  INNER JOIN `kabu-376213.kabu2411.master_trading_stocks` mts
    ON q.stock_code = mts.stock_code
  WHERE q.prev_close_for_signal IS NOT NULL
    -- ★修正: 最新日限定を削除（全期間で計算）
),

-- ============================================================================
-- Part 2: 37指標のシグナル生成
-- ============================================================================

-- 3. 37指標のシグナル生成（全期間分）
all_signals AS (
  -- ==================== Price系 9指標 ====================
  
  -- Close Change Rate
  SELECT 
    stock_code, stock_name, quote_date, target_date, 
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
    'Close Change Rate' as signal_type, 
    ROUND((quote_close - prev_close_for_signal) / prev_close_for_signal * 100, 4) as signal_value 
  FROM signal_calculations 
  WHERE prev_close_for_signal > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Close to Prev Close Ratio
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
    'Close to Prev Close Ratio' as signal_type, 
    ROUND(quote_close / prev_close_for_signal * 100, 4) as signal_value 
  FROM signal_calculations 
  WHERE prev_close_for_signal > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Close MA3 Deviation
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
    'Close MA3 Deviation' as signal_type, 
    ROUND(quote_close / ma3_close * 100, 4) as signal_value 
  FROM signal_calculations 
  WHERE ma3_close > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Close MA5 Deviation
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
    'Close MA5 Deviation' as signal_type, 
    ROUND(quote_close / ma5_close * 100, 4) as signal_value 
  FROM signal_calculations 
  WHERE ma5_close > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Close MA10 Deviation
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
    'Close MA10 Deviation' as signal_type, 
    ROUND(quote_close / ma10_close * 100, 4) as signal_value 
  FROM signal_calculations 
  WHERE ma10_close > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Close to MAX20 Ratio
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
    'Close to MAX20 Ratio' as signal_type, 
    ROUND(quote_close / max20_close * 100, 4) as signal_value 
  FROM signal_calculations 
  WHERE max20_close > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Close to MIN20 Ratio
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
    'Close to MIN20 Ratio' as signal_type, 
    ROUND(quote_close / min20_close * 100, 4) as signal_value 
  FROM signal_calculations 
  WHERE min20_close > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Close to Open Ratio
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
    'Close to Open Ratio' as signal_type, 
    ROUND(quote_close / quote_open * 100, 4) as signal_value 
  FROM signal_calculations 
  WHERE quote_open > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Close Volatility
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
    'Close Volatility' as signal_type, 
    ROUND(SAFE_DIVIDE(stddev20_close, ma20_close) * 100, 4) as signal_value 
  FROM signal_calculations 
  WHERE ma20_close > 0 AND stddev20_close IS NOT NULL AND target_date IS NOT NULL

  -- ==================== PriceRange系 5指標 ====================
  
  UNION ALL
  
  -- Close to Range Ratio
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
    'Close to Range Ratio' as signal_type,
    ROUND(SAFE_DIVIDE(quote_close - quote_low, quote_high - quote_low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > quote_low AND target_date IS NOT NULL
  
  UNION ALL
  
  -- High to Close Drop Rate
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
    'High to Close Drop Rate' as signal_type,
    ROUND(SAFE_DIVIDE(quote_high - quote_close, quote_high - quote_low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > quote_low AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Close to Low Rise Rate
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
    'Close to Low Rise Rate' as signal_type,
    ROUND(SAFE_DIVIDE(quote_close - quote_low, quote_high - quote_low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > quote_low AND target_date IS NOT NULL
  
  UNION ALL
  
  -- High to Close Ratio
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
    'High to Close Ratio' as signal_type,
    ROUND(quote_close / quote_high * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Close to Low Ratio
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
    'Close to Low Ratio' as signal_type,
    ROUND(quote_close / quote_low * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_low > 0 AND target_date IS NOT NULL

  -- ==================== OpenClose系 2指標 ====================
  
  UNION ALL
  
  -- Open to Close Change Rate
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
    'Open to Close Change Rate' as signal_type,
    ROUND((quote_close - quote_open) / quote_open * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_open > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Open Close Range Efficiency
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
    'Open Close Range Efficiency' as signal_type,
    ROUND(SAFE_DIVIDE(quote_close - quote_open, quote_high - quote_low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > quote_low AND target_date IS NOT NULL

  -- ==================== Open系 3指標 ====================
  
  UNION ALL
  
  -- Open to Range Ratio
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
    'Open to Range Ratio' as signal_type,
    ROUND(SAFE_DIVIDE(quote_open - quote_low, quote_high - quote_low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > quote_low AND target_date IS NOT NULL
  
  UNION ALL
  
  -- High to Open Drop Rate
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
    'High to Open Drop Rate' as signal_type,
    ROUND(SAFE_DIVIDE(quote_high - quote_open, quote_high - quote_low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > quote_low AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Open to Low Rise Rate
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
    'Open to Low Rise Rate' as signal_type,
    ROUND(SAFE_DIVIDE(quote_open - quote_low, quote_high - quote_low) * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE quote_high > quote_low AND target_date IS NOT NULL

  -- ==================== Volume系 4指標 ====================
  
  UNION ALL
  
  -- Volume to Prev Ratio
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
    'Volume to Prev Ratio' as signal_type,
    ROUND(quote_volume / prev_volume_for_signal * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE prev_volume_for_signal > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Volume MA3 Deviation
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
    'Volume MA3 Deviation' as signal_type,
    ROUND(quote_volume / ma3_volume * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma3_volume > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Volume MA5 Deviation
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
    'Volume MA5 Deviation' as signal_type,
    ROUND(quote_volume / ma5_volume * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma5_volume > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Volume MA10 Deviation
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
    'Volume MA10 Deviation' as signal_type,
    ROUND(quote_volume / ma10_volume * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma10_volume > 0 AND target_date IS NOT NULL

  -- ==================== Value系 4指標 ====================
  
  UNION ALL
  
  -- Value to Prev Ratio
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
    'Value to Prev Ratio' as signal_type,
    ROUND(quote_value / prev_value_for_signal * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE prev_value_for_signal > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Value MA3 Deviation
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
    'Value MA3 Deviation' as signal_type,
    ROUND(quote_value / ma3_value * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma3_value > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Value MA5 Deviation
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
    'Value MA5 Deviation' as signal_type,
    ROUND(quote_value / ma5_value * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma5_value > 0 AND target_date IS NOT NULL
  
  UNION ALL
  
  -- Value MA10 Deviation
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
    'Value MA10 Deviation' as signal_type,
    ROUND(quote_value / ma10_value * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE ma10_value > 0 AND target_date IS NOT NULL

  -- ==================== Score系 10指標 ====================
  
  UNION ALL
  
  -- High Price Score 3D
  SELECT 
    stock_code, stock_name, quote_date, target_date,
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
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
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
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
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
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
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
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
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
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
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
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
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
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
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
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
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
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
    quote_close as prev_close, quote_volume as prev_volume, quote_value as prev_trading_value,
    'Low Price Score 20D' as signal_type,
    ROUND(
      COALESCE(avg_open_low_20d * 50, 0) + 
      COALESCE(SAFE_DIVIDE(quote_high - quote_low, quote_open) * 30, 0) + 
      COALESCE(ABS(SAFE_DIVIDE(quote_close - quote_open, quote_open)) * 20, 0), 4
    ) as signal_value
  FROM signal_calculations 
  WHERE quote_open > 0 AND avg_open_low_20d IS NOT NULL AND target_date IS NOT NULL
),

-- ============================================================================
-- Part 3: 統計計算と最終処理
-- ============================================================================

-- 4. シグナルbinマッピング
signals_with_bins AS (
  SELECT 
    s.*,
    COALESCE(
      (SELECT MAX(sb.signal_bin) 
       FROM `kabu-376213.kabu2411.M10_signal_bins` sb
       WHERE sb.signal_type = s.signal_type
         AND s.signal_value > sb.lower_bound 
         AND s.signal_value <= sb.upper_bound), 
      1
    ) as signal_bin
  FROM all_signals s
),

-- ★新規追加: 最新日のみフィルタリング
latest_signals_with_bins AS (
  SELECT * FROM signals_with_bins
  WHERE target_date = (
    SELECT MIN(tc.Date)
    FROM `kabu-376213.kabu2411.trading_calendar` tc
    WHERE tc.Date > (SELECT MAX(Date) FROM `kabu-376213.kabu2411.daily_quotes`)
      AND tc.HolidayDivision = '1'
  )
),

-- 5. 全期間統計の計算
all_time_statistics AS (
  SELECT 
    signal_type,
    signal_bin,
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
  FROM `kabu-376213.kabu2411.D10_trading_signals`
  GROUP BY signal_type, signal_bin, trade_type, stock_code
),

-- 6. 最終結果の結合（★修正: latest_signals_with_binsを使用）
final_results AS (
  SELECT 
    swb.target_date,
    swb.signal_type,
    swb.signal_bin,
    trade_type,
    swb.stock_code,
    swb.stock_name,
    swb.signal_value,
    swb.prev_close,
    swb.prev_volume,
    swb.prev_trading_value,
    CAST(FLOOR(swb.prev_volume * 0.01 / 100) * 100 AS INT64) as tradable_shares,
    
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
    
  FROM latest_signals_with_bins swb  -- ★修正
  CROSS JOIN UNNEST(['BUY', 'SELL']) as trade_type
  LEFT JOIN all_time_statistics ats
    ON swb.signal_type = ats.signal_type
    AND swb.signal_bin = ats.signal_bin
    AND trade_type = ats.trade_type
    AND swb.stock_code = ats.stock_code
  WHERE swb.signal_bin IS NOT NULL
)

-- 最終SELECT
SELECT 
  target_date,
  signal_type,
  signal_bin,
  trade_type,
  stock_code,
  stock_name,
  signal_value,
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
  signal_type,
  trade_type;

-- ============================================================================
-- 投入結果確認クエリ
-- ============================================================================

-- 投入完了確認
SELECT 
  '✅ D20_tomorrow_signals データ投入完了' as status,
  COUNT(*) as total_records,
  COUNT(DISTINCT signal_type) as signal_types_count,
  COUNT(DISTINCT stock_code) as stocks_count,
  MIN(target_date) as target_date,
  CURRENT_TIMESTAMP() as end_time
FROM `kabu-376213.kabu2411.D20_tomorrow_signals`;

-- 流動性情報サンプル確認
SELECT 
  '🔍 流動性情報サンプル' as check_type,
  target_date,
  stock_code,
  stock_name,
  prev_close,
  prev_volume,
  prev_trading_value,
  tradable_shares,
  CONCAT(FORMAT('%.0f', prev_volume), ' 株') as prev_volume_formatted,
  CONCAT('¥', FORMAT('%.0f', prev_trading_value * 1000000)) as prev_trading_value_formatted,
  CONCAT(FORMAT('%.0f', tradable_shares), ' 株') as tradable_shares_formatted
FROM `kabu-376213.kabu2411.D20_tomorrow_signals`
WHERE stock_code IN ('7203', '8306', '9984')  -- トヨタ、三菱UFJ、ソフトバンクG
  AND trade_type = 'BUY'
  AND signal_type = 'Close Change Rate'
LIMIT 3;

-- 高パフォーマンスシグナル確認
SELECT 
  '⭐ 高パフォーマンスシグナル TOP10' as check_type,
  signal_type,
  signal_bin,
  trade_type,
  stock_name,
  signal_value,
  total_samples,
  ROUND(win_rate, 1) as win_rate_pct,
  ROUND(avg_profit_rate, 2) as avg_profit_pct,
  tradable_shares
FROM `kabu-376213.kabu2411.D20_tomorrow_signals`
WHERE total_samples >= 20  -- 十分なサンプル数
  AND win_rate >= 55       -- 高勝率
  AND avg_profit_rate >= 0.5  -- 高期待値
ORDER BY avg_profit_rate DESC
LIMIT 10;

-- 37指標実装確認
SELECT 
  '📊 37指標実装確認' as check_type,
  signal_type,
  COUNT(*) as records_count,
  COUNT(DISTINCT stock_code) as stocks_count,
  COUNT(DISTINCT signal_bin) as bins_count,
  AVG(total_samples) as avg_samples
FROM `kabu-376213.kabu2411.D20_tomorrow_signals`
GROUP BY signal_type
ORDER BY signal_type;

-- 統計サマリー
SELECT 
  '📈 統計サマリー' as check_type,
  COUNT(*) as total_signals,
  COUNT(CASE WHEN total_samples >= 20 THEN 1 END) as signals_with_enough_samples,
  COUNT(CASE WHEN win_rate >= 50 THEN 1 END) as positive_win_rate_signals,
  COUNT(CASE WHEN avg_profit_rate > 0 THEN 1 END) as positive_profit_signals,
  ROUND(AVG(win_rate), 2) as overall_avg_win_rate,
  ROUND(AVG(avg_profit_rate), 4) as overall_avg_profit_rate
FROM `kabu-376213.kabu2411.D20_tomorrow_signals`;

-- ============================================================================
-- 実行完了メッセージ
-- ============================================================================

SELECT 
  '🏆 D20_tomorrow_signals 作成・投入完了！' as message,
  '✅ テーブル作成完了' as step1,
  '✅ 37指標シグナル計算完了' as step2,
  '✅ 全期間統計計算完了' as step3,
  '✅ 流動性情報追加完了' as step4,
  '🎯 明日の取引判断準備完了' as result,
  CURRENT_TIMESTAMP() as completion_time;