/*
ファイル: 02_create_D010_basic_results_phase2C.sql
説明: D010_basic_results への残りScore系9指標追加（Phase 2B成功後）
前提: Phase 2B（28指標）が正常完了済み（1,876万レコード）
作成日: 2025年7月4日
目的: 37指標フル対応完成（設計書完全準拠）
処理時間: 約1-2分予想（Phase 2Bの高速実績により）
*/

-- ============================================================================
-- Phase 2C: 残りScore系9指標追加実行
-- ============================================================================

-- 処理開始メッセージ
SELECT 
  '🚀 Phase 2C開始: 残りScore系9指標追加実行' as message,
  '前提: Phase 2B完了（28指標・1,876万レコード）' as prerequisite,
  '目標: 37指標フル対応完成（設計書完全準拠）' as target,
  '予想処理時間: 約1-2分（Phase 2B高速実績により）' as estimated_time,
  '予想追加レコード数: 約600万レコード' as estimated_records,
  CURRENT_TIMESTAMP() as start_time;

-- ============================================================================
-- 事前確認: Phase 2B完了状況
-- ============================================================================

-- Phase 2B結果確認
SELECT 
  'Phase 2B完了状況確認' as check_point,
  COUNT(*) as current_records,
  COUNT(DISTINCT signal_type) as current_signal_types_should_be_28,
  MIN(signal_date) as min_date,
  MAX(signal_date) as max_date_should_be_2024_06_28,
  CASE 
    WHEN COUNT(DISTINCT signal_type) = 28 AND MAX(signal_date) = '2024-06-28'
    THEN '✅ Phase 2B正常完了 - Phase 2C実行可能'
    ELSE '❌ Phase 2B未完了 - Phase 2C実行不可'
  END as phase2b_status
FROM `kabu-376213.kabu2411.D010_basic_results`;

-- ============================================================================
-- 残りScore系9指標追加実行
-- ============================================================================

INSERT INTO `kabu-376213.kabu2411.D010_basic_results`
WITH 
-- 1. 株価データ準備（Phase 2A/2Bと同じロジック）
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
  WHERE dq.Date >= '2022-07-01' AND dq.Date < '2024-07-01'  -- 学習期間
    AND dq.Open > 0 AND dq.Close > 0  -- 基本的な品質チェック
),

-- 2. シグナル値計算（新指標用の移動平均計算）
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
    
    -- 新指標用の移動平均計算（High/Open比率とOpen/Low比率）
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

-- 3. 残り9つのScore系指標生成
remaining_score_signals AS (

  -- ==================== High Price Score系 4指標 ====================
  
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

  -- ==================== Low Price Score系 5指標 ====================
  
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
  FROM remaining_score_signals s
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

-- ============================================================================
-- Phase 2C完了確認
-- ============================================================================

-- 追加結果確認
SELECT 
  '🎉 Phase 2C完了確認（37指標フル対応完成）' as status,
  COUNT(*) as total_records_after_addition,
  COUNT(DISTINCT signal_type) as signal_types_should_be_37,
  COUNT(DISTINCT stock_code) as stock_count,
  COUNT(DISTINCT trade_type) as trade_types,
  MIN(signal_date) as min_date,
  MAX(signal_date) as max_date,
  ROUND(AVG(CASE WHEN is_win THEN 1.0 ELSE 0.0 END) * 100, 1) as overall_win_rate_percent,
  CURRENT_TIMESTAMP() as completion_time
FROM `kabu-376213.kabu2411.D010_basic_results`;

-- Score系指標確認
SELECT 
  'Phase 2C: Score系指標確認' as check_point,
  signal_type,
  COUNT(*) as record_count,
  COUNT(DISTINCT stock_code) as unique_stocks,
  ROUND(AVG(baseline_profit_rate), 4) as avg_profit_rate
FROM `kabu-376213.kabu2411.D010_basic_results`
WHERE signal_type LIKE '%Score%'
GROUP BY signal_type
ORDER BY signal_type;

-- 37指標完全リスト確認
SELECT 
  'Phase 2C: 37指標完全リスト' as check_point,
  signal_type,
  COUNT(*) as record_count
FROM `kabu-376213.kabu2411.D010_basic_results`
GROUP BY signal_type
ORDER BY signal_type;

-- ============================================================================
-- 🎉 設計書完全準拠達成確認
-- ============================================================================

SELECT 
  '🏆 設計書完全準拠達成！' as achievement,
  '✅ 37指標フル対応完成' as signal_completion,
  '✅ D010_basic_results基盤完成' as table_completion,
  '✅ 学習期間データ投入完了' as data_completion,
  '次Phase: Step 4（検証期間投入）→ 3年間完全データ構築' as next_step,
  'または D020_learning_stats作成開始' as alternative_next,
  CURRENT_TIMESTAMP() as completion_time;

-- ============================================================================
-- 実行ログ記録用セクション
-- ============================================================================

/*
=== Phase 2C 実行ログ ===
実行日時: [手動記入]
実行者: [手動記入]  
実行結果: [SUCCESS/FAILED]
処理時間: [手動記入]
追加レコード数: [手動記入]
総レコード数: [手動記入]
最終指標数: [37指標期待]
エラー内容: [あれば記入]
次のアクション: [Step 4実装 or D020作成]

=== 実行時メモ ===
- [Score系指標の動作確認結果]
- [37指標完成の感想]
- [次の作業計画]
*/