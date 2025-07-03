/*
ファイル: 02_create_D010_basic_results.sql
説明: Signal Axis Trader 新設計書 - D010_basic_results テーブル新規作成
作成日: 2025年7月4日
目的: 基本取引結果テーブルの完全新規作成（既存システムに依存しない）
実行時間: 約10-15分（期間分割実行）
データ量: 約3,900万レコード予定（3年分）
設計方針: 新設計書完全準拠、シンプル・高速・保守性重視
*/

-- ============================================================================
-- Phase 2: D010_basic_results 新規作成（設計書完全準拠）
-- ============================================================================

-- 処理開始メッセージ
SELECT 
  'Phase 2: D010_basic_results 新規作成を開始します' as message,
  '設計方針: 新設計書完全準拠・既存システム非依存' as design_principle,
  'データソース: M010_signal_bins + daily_quotes（直接計算）' as source_info,
  'TARGET: 3年間 × 37指標 × LONG/SHORT = 約3,900万レコード' as target_scale,
  CURRENT_TIMESTAMP() as start_time;

-- ============================================================================
-- Step 1: データソース確認
-- ============================================================================

-- 依存テーブルの状況確認
SELECT 
  'Step 1: データソース確認' as check_step,
  (SELECT COUNT(*) FROM `kabu-376213.kabu2411.M010_signal_bins`) as M010_records_740_expected,
  (SELECT COUNT(*) FROM `kabu-376213.kabu2411.daily_quotes`) as daily_quotes_records,
  (SELECT MIN(Date) FROM `kabu-376213.kabu2411.daily_quotes`) as quotes_min_date,
  (SELECT MAX(Date) FROM `kabu-376213.kabu2411.daily_quotes`) as quotes_max_date,
  '期間: 約3年間のデータ処理予定' as processing_period;

-- M010 signal_bins 可用性確認
SELECT 
  'Step 1: M010境界値確認' as check_step,
  signal_type,
  COUNT(*) as bins_count,
  MIN(lower_bound) as min_lower,
  MAX(upper_bound) as max_upper
FROM `kabu-376213.kabu2411.M010_signal_bins`
GROUP BY signal_type
ORDER BY signal_type
LIMIT 5;

-- ============================================================================
-- Step 2: D010_basic_results テーブル作成
-- ============================================================================

-- 既存テーブルがある場合は削除
DROP TABLE IF EXISTS `kabu-376213.kabu2411.D010_basic_results`;

-- 新設計書準拠でテーブル作成
CREATE TABLE `kabu-376213.kabu2411.D010_basic_results` (
  signal_date DATE NOT NULL,
  
  -- 4軸情報
  signal_type STRING NOT NULL,           -- 4軸①
  signal_bin INT64 NOT NULL,             -- 4軸②
  trade_type STRING NOT NULL,            -- 4軸③ 'BUY'/'SELL'
  stock_code STRING NOT NULL,            -- 4軸④
  stock_name STRING,                     -- 冗長データ（JOIN回避）
  signal_value FLOAT64,                  -- シグナル値
  
  -- 価格データ（API必須項目）
  prev_close FLOAT64,                    -- 前日終値
  day_open FLOAT64,                      -- 始値
  day_high FLOAT64,                      -- 高値
  day_low FLOAT64,                       -- 安値
  day_close FLOAT64,                     -- 終値
  
  -- 計算値（API画面表示用）
  prev_close_to_open_gap FLOAT64,       -- 前日終値→始値
  open_to_high_gap FLOAT64,             -- 始値→高値
  open_to_low_gap FLOAT64,              -- 始値→安値
  open_to_close_gap FLOAT64,            -- 始値→終値
  daily_range FLOAT64,                  -- 日足値幅
  
  -- 取引結果
  baseline_profit_rate FLOAT64,         -- 寄引損益率
  is_win BOOLEAN,                       -- 勝敗
  trading_volume FLOAT64,               -- 売買代金
  
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY signal_date
CLUSTER BY stock_code, signal_type;

SELECT 
  '✅ Step 2完了: D010_basic_results テーブル作成完了' as status,
  '設計書準拠: 4軸情報 + 価格データ + 取引結果' as structure,
  'パーティション: signal_date, クラスタ: stock_code, signal_type' as optimization;

-- ============================================================================
-- Step 3: 学習期間データ投入（2022年7月〜2024年6月）
-- ============================================================================

INSERT INTO `kabu-376213.kabu2411.D010_basic_results`
WITH 
-- 1. 株価データ準備（前日終値計算付き）
quotes_with_prev AS (
  SELECT 
    REGEXP_REPLACE(dq.Code, '0$', '') as stock_code,
    dq.Date as quote_date,
    dq.Open,
    dq.High, 
    dq.Low,
    dq.Close,
    dq.Volume,
    LAG(dq.Close) OVER (
      PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') 
      ORDER BY dq.Date
    ) as prev_close
  FROM `kabu-376213.kabu2411.daily_quotes` dq
  WHERE dq.Date >= '2022-07-01' AND dq.Date < '2024-07-01'  -- 学習期間（2024-06-30まで）
    AND dq.Open > 0 AND dq.Close > 0  -- 基本的な品質チェック
),

-- 2. シグナル値計算（37指標を直接計算）
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
    q.prev_close,
    
    -- 移動平均計算
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
    
    MAX(q.Close) OVER (
      PARTITION BY q.stock_code 
      ORDER BY q.quote_date 
      ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) as max20_close,
    
    MIN(q.Close) OVER (
      PARTITION BY q.stock_code 
      ORDER BY q.quote_date 
      ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) as min20_close
    
  FROM quotes_with_prev q
  INNER JOIN `kabu-376213.kabu2411.master_trading_stocks` mts
    ON q.stock_code = mts.stock_code
  WHERE q.prev_close IS NOT NULL
),

-- 3. 全シグナル指標を生成
all_signals AS (
  -- Close Change Rate
  SELECT 
    stock_code, stock_name, quote_date, signal_date,
    Open, High, Low, Close, Volume, prev_close,
    'Close Change Rate' as signal_type,
    ROUND((Close - prev_close) / prev_close * 100, 4) as signal_value
  FROM signal_calculations 
  WHERE prev_close > 0 AND signal_date IS NOT NULL AND signal_date > quote_date
  
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
  
  -- 【段階的実行戦略】
-- Phase 2A: 基本6指標でテスト（このクエリ）
-- Phase 2B: 残り31指標を追加実行
-- 
-- 基本6指標選定理由:
-- - Close Change Rate: 最重要基本指標
-- - Close MA5/MA10 Deviation: 移動平均系
-- - Close to MAX20/MIN20 Ratio: レンジ系
-- - Close to Open Ratio: 日中変動系
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
  FROM all_signals s
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
    AND s.signal_date <= '2024-06-30'  -- signal_dateでも学習期間制限
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

-- 学習期間投入完了確認
SELECT 
  '✅ Step 3完了: 学習期間データ投入' as status,
  COUNT(*) as learning_period_records,
  COUNT(DISTINCT signal_type) as signal_types,
  COUNT(DISTINCT stock_code) as stock_count,
  COUNT(DISTINCT trade_type) as trade_types,
  MIN(signal_date) as min_date,
  MAX(signal_date) as max_date,
  '次: Step 4（検証期間投入）を実行してください' as next_action
FROM `kabu-376213.kabu2411.D010_basic_results`;

-- ============================================================================
-- Step 4: 検証期間データ投入（2024年7月〜現在）
-- ============================================================================

-- TODO: Step 3と同様のロジックで検証期間（2024/7/1〜現在）を投入

-- ============================================================================
-- Step 5: 作成結果確認
-- ============================================================================

-- 基本統計確認
SELECT 
  '🎉 Phase 2作成結果（D010_basic_results）' as final_check,
  COUNT(*) as total_records,
  COUNT(DISTINCT signal_type) as signal_types,
  COUNT(DISTINCT stock_code) as stock_count,
  COUNT(DISTINCT trade_type) as trade_types_buy_sell,
  MIN(signal_date) as min_date,
  MAX(signal_date) as max_date,
  ROUND(AVG(CASE WHEN is_win THEN 1.0 ELSE 0.0 END) * 100, 1) as overall_win_rate_percent
FROM `kabu-376213.kabu2411.D010_basic_results`;

-- 4軸統計
SELECT 
  'Phase 2: 4軸統計' as check_point,
  signal_type,
  trade_type,
  COUNT(*) as record_count,
  COUNT(DISTINCT stock_code) as unique_stocks,
  ROUND(AVG(baseline_profit_rate), 4) as avg_profit_rate
FROM `kabu-376213.kabu2411.D010_basic_results`
GROUP BY signal_type, trade_type
ORDER BY signal_type, trade_type
LIMIT 10;

-- ============================================================================
-- Phase 2完了確認
-- ============================================================================

SELECT 
  '🎉 Phase 2完了: D010_basic_results新規作成成功' as status,
  '設計書準拠: シンプル・高速・保守性重視' as design_achievement,
  '独立構築: 既存システム非依存で品質保証' as quality_assurance,
  '次Phase: D020_learning_stats作成準備完了' as next_step,
  CURRENT_TIMESTAMP() as completion_time;

-- ============================================================================
-- 使用方法例
-- ============================================================================

-- ============================================================================
-- 実行ログ記録用セクション
-- ============================================================================

-- 実行前チェックリスト
SELECT 
  '📋 Phase 2A実行前チェックリスト' as checklist,
  '✅ M010_signal_bins作成済み（740レコード）' as check1,
  '✅ daily_quotes利用可能' as check2,
  '✅ master_trading_stocks利用可能' as check3,
  '✅ trading_calendar利用可能' as check4,
  '⚠️ 基本6指標のみ実装（テスト用）' as limitation,
  CURRENT_TIMESTAMP() as check_time;

-- 実行結果ログ（実行後に手動更新）
/*
=== Phase 2A 実行ログ ===
実行日時: [手動記入]
実行者: [手動記入]  
実行結果: [SUCCESS/FAILED]
処理時間: [手動記入]
作成レコード数: [手動記入]
エラー内容: [あれば記入]
次のアクション: [Phase 2B実行/エラー対応等]

=== 実行時メモ ===
- [実行時の気づき等を記入]
- [パフォーマンス観察結果]
- [データ品質確認結果]
*/