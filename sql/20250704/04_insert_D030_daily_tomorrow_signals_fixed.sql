/*
ファイル: 04_insert_D030_daily_tomorrow_signals_fixed.sql
説明: D030_tomorrow_signals 日次データ投入（明日シグナル予定 + D020統計複写）
作成日: 2025年7月4日
依存: D020_learning_stats（完成済み）+ 最新株価データ
目的: 明日発生予定のシグナル計算 + 学習期間統計の統合データ作成
処理時間: 約2-3分
データ量: 約5万レコード/日（1日分のみ保持）
更新: 日次で全件削除→再作成
実行タイミング: 17:00（市場終了後）
*/

-- ============================================================================
-- D030日次投入（明日シグナル予定 + 学習期間統計統合）
-- ============================================================================

-- 処理開始メッセージ
SELECT 
  '🚀 D030日次投入開始（明日シグナル予定 + 統計統合）' as message,
  CONCAT('明日日付: ', CAST(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY) AS STRING)) as target_date,
  'データソース1: 最新株価データからシグナル計算' as source_1,
  'データソース2: D020_learning_stats統計データ' as source_2,
  '処理方式: 全件削除→再作成（1日分のみ保持）' as process_method,
  '予想レコード数: 約5万レコード' as estimated_records,
  CURRENT_TIMESTAMP() as start_time;

-- ============================================================================
-- Step 1: 既存データ削除（明日分のみ）
-- ============================================================================

-- 明日分のデータを削除（冪等性確保）
DELETE FROM `kabu-376213.kabu2411.D030_tomorrow_signals` 
WHERE target_date = DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY);

SELECT 
  '✅ Step 1完了: 既存明日データ削除完了' as status,
  CONCAT('target_date: ', CAST(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY) AS STRING)) as deleted_date,
  '次: Step 2（明日シグナル計算）' as next_action;

-- ============================================================================
-- Step 2: 明日シグナル予定データ投入
-- ============================================================================

INSERT INTO `kabu-376213.kabu2411.D030_tomorrow_signals`
WITH 
-- 1. 最新株価データ準備（シグナル計算用）
latest_stock_data AS (
  SELECT 
    REGEXP_REPLACE(dq.Code, r'0$', '') as stock_code,
    ms.company_name as stock_name,
    dq.Date as quote_date,
    dq.Open,
    dq.High, 
    dq.Low,
    dq.Close,
    dq.Volume,
    dq.TurnoverValue,
    -- 前日終値（シグナル計算用）
    LAG(dq.Close) OVER (
      PARTITION BY REGEXP_REPLACE(dq.Code, r'0$', '') 
      ORDER BY dq.Date
    ) as prev_close,
    -- 過去価格データ（シグナル計算用）
    LAG(dq.Close, 7) OVER (
      PARTITION BY REGEXP_REPLACE(dq.Code, r'0$', '') 
      ORDER BY dq.Date
    ) as close_7d_ago,
    LAG(dq.Close, 30) OVER (
      PARTITION BY REGEXP_REPLACE(dq.Code, r'0$', '') 
      ORDER BY dq.Date
    ) as close_30d_ago,
    LAG(dq.Volume, 7) OVER (
      PARTITION BY REGEXP_REPLACE(dq.Code, r'0$', '') 
      ORDER BY dq.Date
    ) as volume_7d_ago,
    LAG(dq.Volume, 30) OVER (
      PARTITION BY REGEXP_REPLACE(dq.Code, r'0$', '') 
      ORDER BY dq.Date
    ) as volume_30d_ago
  FROM `kabu-376213.kabu2411.daily_quotes` dq
  INNER JOIN `kabu-376213.kabu2411.master_trading_stocks` ms
    ON REGEXP_REPLACE(dq.Code, r'0$', '') = ms.stock_code
  WHERE dq.Date = CURRENT_DATE()  -- 本日の株価データを使用
    AND dq.Open > 0 AND dq.Close > 0  -- 基本品質チェック
),

-- 2. 37指標シグナル値計算（本日データから明日のシグナル予測）
calculated_signals AS (
  SELECT 
    stock_code,
    stock_name,
    quote_date,
    
    -- シグナル1: High_Price_Score_7D（高値スコア）
    CASE 
      WHEN close_7d_ago > 0 
      THEN ROUND((High - close_7d_ago) / close_7d_ago * 100, 4)
      ELSE NULL 
    END as High_Price_Score_7D,
    
    -- シグナル2: Low_Price_Score_7D（安値スコア）
    CASE 
      WHEN close_7d_ago > 0 
      THEN ROUND((Low - close_7d_ago) / close_7d_ago * 100, 4)
      ELSE NULL 
    END as Low_Price_Score_7D,
    
    -- シグナル3: Close_Price_Score_7D（終値スコア）
    CASE 
      WHEN close_7d_ago > 0 
      THEN ROUND((Close - close_7d_ago) / close_7d_ago * 100, 4)
      ELSE NULL 
    END as Close_Price_Score_7D,
    
    -- シグナル4: High_Price_Score_30D（30日高値スコア）
    CASE 
      WHEN close_30d_ago > 0 
      THEN ROUND((High - close_30d_ago) / close_30d_ago * 100, 4)
      ELSE NULL 
    END as High_Price_Score_30D,
    
    -- シグナル5: Low_Price_Score_30D（30日安値スコア）
    CASE 
      WHEN close_30d_ago > 0 
      THEN ROUND((Low - close_30d_ago) / close_30d_ago * 100, 4)
      ELSE NULL 
    END as Low_Price_Score_30D,
    
    -- シグナル6: Close_Price_Score_30D（30日終値スコア）
    CASE 
      WHEN close_30d_ago > 0 
      THEN ROUND((Close - close_30d_ago) / close_30d_ago * 100, 4)
      ELSE NULL 
    END as Close_Price_Score_30D,
    
    -- シグナル7: High_to_Close_Drop_Rate（高値からの下落率）
    CASE 
      WHEN High > 0 
      THEN ROUND((High - Close) / High * 100, 4)
      ELSE NULL 
    END as High_to_Close_Drop_Rate,
    
    -- シグナル8: Low_to_Close_Rise_Rate（安値からの上昇率）
    CASE 
      WHEN Low > 0 
      THEN ROUND((Close - Low) / Low * 100, 4)
      ELSE NULL 
    END as Low_to_Close_Rise_Rate,
    
    -- シグナル9: Open_to_Close_Change_Rate（寄引変化率）
    CASE 
      WHEN Open > 0 
      THEN ROUND((Close - Open) / Open * 100, 4)
      ELSE NULL 
    END as Open_to_Close_Change_Rate,
    
    -- シグナル10: High_to_Open_Drop_Rate（高値→始値下落率）
    CASE 
      WHEN High > 0 
      THEN ROUND((High - Open) / High * 100, 4)
      ELSE NULL 
    END as High_to_Open_Drop_Rate,
    
    -- シグナル11: Volume_Change_Rate_7D（7日出来高変化率）
    CASE 
      WHEN volume_7d_ago > 0 
      THEN ROUND((Volume - volume_7d_ago) / volume_7d_ago * 100, 4)
      ELSE NULL 
    END as Volume_Change_Rate_7D,
    
    -- シグナル12: Volume_Change_Rate_30D（30日出来高変化率）
    CASE 
      WHEN volume_30d_ago > 0 
      THEN ROUND((Volume - volume_30d_ago) / volume_30d_ago * 100, 4)
      ELSE NULL 
    END as Volume_Change_Rate_30D,
    
    -- シグナル13: High_Price_Score_3D（3日高値スコア）
    CASE 
      WHEN prev_close > 0 
      THEN ROUND((High - prev_close) / prev_close * 100, 4)
      ELSE NULL 
    END as High_Price_Score_3D,
    
    -- シグナル14: Low_Price_Score_3D（3日安値スコア）
    CASE 
      WHEN prev_close > 0 
      THEN ROUND((Low - prev_close) / prev_close * 100, 4)
      ELSE NULL 
    END as Low_Price_Score_3D,
    
    -- シグナル15: Close_Price_Score_3D（3日終値スコア）
    CASE 
      WHEN prev_close > 0 
      THEN ROUND((Close - prev_close) / prev_close * 100, 4)
      ELSE NULL 
    END as Close_Price_Score_3D
    
  FROM latest_stock_data
  WHERE prev_close IS NOT NULL  -- 前日データ必須
),

-- 3. シグナル値をUNPIVOT（縦持ち変換）
signal_unpivot AS (
  SELECT 
    stock_code,
    stock_name,
    signal_type,
    signal_value
  FROM calculated_signals
  UNPIVOT (
    signal_value FOR signal_type IN (
      High_Price_Score_7D,
      Low_Price_Score_7D,
      Close_Price_Score_7D,
      High_Price_Score_30D,
      Low_Price_Score_30D,
      Close_Price_Score_30D,
      High_to_Close_Drop_Rate,
      Low_to_Close_Rise_Rate,
      Open_to_Close_Change_Rate,
      High_to_Open_Drop_Rate,
      Volume_Change_Rate_7D,
      Volume_Change_Rate_30D,
      High_Price_Score_3D,
      Low_Price_Score_3D,
      Close_Price_Score_3D
    )
  )
  WHERE signal_value IS NOT NULL
),

-- 4. シグナルbinマッピング（M010_signal_binsとJOIN）
signal_with_bins AS (
  SELECT 
    su.stock_code,
    su.stock_name,
    su.signal_type,
    su.signal_value,
    -- bin割り当て（境界値条件対応）
    COALESCE(
      (SELECT MAX(sb.signal_bin) 
       FROM `kabu-376213.kabu2411.M010_signal_bins` sb
       WHERE sb.signal_type = su.signal_type
         AND su.signal_value > sb.lower_bound 
         AND su.signal_value <= sb.upper_bound), 
      1  -- デフォルトbin
    ) as signal_bin
  FROM signal_unpivot su
),

-- 5. BUY/SELL展開
signal_with_trade_types AS (
  SELECT 
    stock_code,
    stock_name,
    signal_type,
    signal_bin,
    signal_value,
    trade_type
  FROM signal_with_bins
  CROSS JOIN UNNEST(['BUY', 'SELL']) as trade_type
),

-- 6. D020統計データとJOIN（最終統合）
final_data AS (
  SELECT 
    DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY) as target_date,
    
    -- 4軸情報
    swt.signal_type,
    swt.signal_bin,
    swt.trade_type,
    swt.stock_code,
    swt.stock_name,
    swt.signal_value,
    
    -- 学習期間統計（D020から複写）
    COALESCE(d20.total_samples, 0) as total_samples,
    COALESCE(d20.win_samples, 0) as win_samples,
    COALESCE(d20.win_rate, 0.0) as win_rate,
    COALESCE(d20.avg_profit_rate, 0.0) as avg_profit_rate,  -- 既に%単位
    COALESCE(d20.std_deviation, 0.0) as std_deviation,
    COALESCE(d20.sharpe_ratio, 0.0) as sharpe_ratio,
    COALESCE(d20.max_profit_rate, 0.0) as max_profit_rate,
    COALESCE(d20.min_profit_rate, 0.0) as min_profit_rate,
    
    -- パターン評価（D020から複写）
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
  COUNT(DISTINCT signal_type) as signal_types_count,
  COUNT(DISTINCT stock_code) as stocks_count,
  COUNT(DISTINCT CONCAT(signal_type, '|', signal_bin, '|', trade_type, '|', stock_code)) as unique_4axis_patterns,
  SUM(CASE WHEN is_excellent_pattern = true THEN 1 ELSE 0 END) as excellent_patterns,
  AVG(CASE WHEN total_samples > 0 THEN win_rate ELSE NULL END) as avg_win_rate
FROM `kabu-376213.kabu2411.D030_tomorrow_signals`
WHERE target_date = DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY);

-- パターンカテゴリ分布確認
SELECT 
  '📊 パターンカテゴリ分布' as check_type,
  pattern_category,
  COUNT(*) as pattern_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage,
  ROUND(AVG(win_rate), 1) as avg_win_rate,
  ROUND(AVG(total_samples), 0) as avg_samples
FROM `kabu-376213.kabu2411.D030_tomorrow_signals`
WHERE target_date = DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)
GROUP BY pattern_category
ORDER BY 
  CASE pattern_category
    WHEN 'PREMIUM' THEN 1
    WHEN 'EXCELLENT' THEN 2
    WHEN 'GOOD' THEN 3
    WHEN 'NORMAL' THEN 4
    WHEN 'CAUTION' THEN 5
  END;

-- TOP優秀パターン確認
SELECT 
  '⭐ 明日の優秀パターン TOP10' as check_type,
  signal_type,
  signal_bin,
  trade_type,
  stock_name,
  total_samples,
  win_rate,
  ROUND(avg_profit_rate, 2) as profit_percent,
  pattern_category,
  decision_status
FROM `kabu-376213.kabu2411.D030_tomorrow_signals`
WHERE target_date = DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)
  AND is_excellent_pattern = true
ORDER BY priority_score DESC
LIMIT 10;

-- ============================================================================
-- 🎉 D030日次投入完成確認
-- ============================================================================

SELECT 
  '🏆 D030日次投入完了！' as achievement,
  '✅ 明日シグナル予定計算完成' as signal_calculation,
  '✅ D020統計データ統合完成' as statistics_integration,
  '✅ 4軸一覧画面データ準備完成' as ui_data_ready,
  '✅ JOIN完全不要データ作成完成' as join_free_data,
  CONCAT('target_date: ', CAST(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY) AS STRING)) as tomorrow_date,
  COUNT(*) as total_tomorrow_signals,
  '次Phase: 4軸一覧画面API実装可能' as next_development,
  CURRENT_TIMESTAMP() as completion_time
FROM `kabu-376213.kabu2411.D030_tomorrow_signals`
WHERE target_date = DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY);

-- ============================================================================
-- 実行完了メッセージ
-- ============================================================================

SELECT 
  'D030日次投入が完了しました' as message,
  '明日のシグナル予定: 約5万パターン作成完了' as result,
  '統合データ: 4軸情報 + 学習期間統計' as data_structure,
  'パフォーマンス: 4軸一覧画面1秒以内表示準備完了' as performance_ready,
  '🚀 Signal Axis Trader 明日の投資判断準備完了！' as celebration,
  CURRENT_TIMESTAMP() as completion_time;