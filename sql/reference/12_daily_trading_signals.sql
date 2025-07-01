/*
ファイル: 12_daily_trading_signals_fixed.sql
説明: 期待利益率を最優先とした取引シグナル生成（日付問題修正版）
主な修正点: 
  - CURRENT_DATE('Asia/Tokyo')に依存せず、最新のシグナル日を処理
  - 17:00実行でも翌日のシグナルを確実に処理可能
実行時間: 約30秒
*/

-- ============================================================================
-- 1. 処理対象日の決定（修正版）
-- ============================================================================
-- 最新のシグナル日を取得（CURRENT_DATEに依存しない）
DECLARE target_signal_date DATE DEFAULT (
  SELECT MAX(signal_date)
  FROM `kabu-376213.kabu2411.d01_signals_raw`
);

-- 対応する参照日を取得
DECLARE target_reference_date DATE DEFAULT (
  SELECT MAX(reference_date)
  FROM `kabu-376213.kabu2411.d01_signals_raw`
  WHERE signal_date = target_signal_date
);

-- すでに処理済みかチェック
DECLARE already_processed INT64 DEFAULT (
  SELECT COUNT(*)
  FROM `kabu-376213.kabu2411.d04_daily_trading_signals`
  WHERE signal_date = target_signal_date
);

-- 処理対象の確認
SELECT 
  target_signal_date as signal_date,
  target_reference_date as reference_date,
  already_processed as existing_signals,
  (SELECT COUNT(DISTINCT stock_code) FROM `kabu-376213.kabu2411.d01_signals_raw` WHERE signal_date = target_signal_date) as target_stocks,
  (SELECT COUNT(*) FROM `kabu-376213.kabu2411.d01_signals_raw` WHERE signal_date = target_signal_date) as total_signals,
  CURRENT_TIMESTAMP() as execution_time;

-- 既に処理済みの場合は早期終了
IF already_processed > 0 THEN
  SELECT 
    CONCAT('シグナル日 ', CAST(target_signal_date AS STRING), ' は既に処理済みです（', CAST(already_processed AS STRING), '件）') as message;
  RETURN;  -- 処理を終了
END IF;

-- ============================================================================
-- 2. 既存データのクリア（当該日分のみ）
-- ============================================================================
DELETE FROM `kabu-376213.kabu2411.d04_daily_trading_signals`
WHERE signal_date = target_signal_date;

DELETE FROM `kabu-376213.kabu2411.h01_signal_predictions`
WHERE signal_date = target_signal_date;

-- ============================================================================
-- 3. 取引シグナルの生成（元のロジックと同じ）
-- ============================================================================
INSERT INTO `kabu-376213.kabu2411.d04_daily_trading_signals`
WITH current_signals AS (
  -- 当日のシグナルに区分番号を付与
  SELECT
    sr.signal_date,
    sr.reference_date,
    sr.stock_code,
    sr.stock_name,
    sr.signal_type,
    sr.signal_value,
    MAX(sb.signal_bin) as signal_bin,
    MAX(sb.percentile_rank) as signal_percentile
  FROM `kabu-376213.kabu2411.d01_signals_raw` sr
  INNER JOIN `kabu-376213.kabu2411.m02_signal_bins` sb
    ON sr.signal_type = sb.signal_type
    AND sr.signal_value <= sb.upper_bound
    AND sr.signal_value > sb.lower_bound
  WHERE sr.signal_date = target_signal_date
  GROUP BY
    sr.signal_date,
    sr.reference_date,
    sr.stock_code,
    sr.stock_name,
    sr.signal_type,
    sr.signal_value
),
-- 4軸グループの実績を結合（有効グループのみ）
signals_with_performance AS (
  SELECT
    cs.*,
    -- Buy側の実績
    pf_buy.total_count as buy_total_count,
    pf_buy.win_rate as buy_win_rate,
    pf_buy.avg_profit_rate as buy_avg_profit,
    pf_buy.sharpe_ratio as buy_sharpe_ratio,
    eg_buy.is_effective as buy_is_effective,
    eg_buy.composite_score as buy_composite_score,
    -- Sell側の実績
    pf_sell.total_count as sell_total_count,
    pf_sell.win_rate as sell_win_rate,
    pf_sell.avg_profit_rate as sell_avg_profit,
    pf_sell.sharpe_ratio as sell_sharpe_ratio,
    eg_sell.is_effective as sell_is_effective,
    eg_sell.composite_score as sell_composite_score,
    -- 前日の価格・出来高情報
    q.Close as prev_close,
    q.Volume as prev_volume,
    q.Close * q.Volume as prev_value
  FROM current_signals cs
  -- Buy側（有効グループのみJOIN）
  LEFT JOIN `kabu-376213.kabu2411.d03_effective_4axis_groups` eg_buy
    ON cs.signal_type = eg_buy.signal_type
    AND cs.signal_bin = eg_buy.signal_bin
    AND cs.stock_code = eg_buy.stock_code
    AND eg_buy.trade_type = 'Buy'
    AND eg_buy.is_effective = true
  LEFT JOIN `kabu-376213.kabu2411.d02_signal_performance_4axis` pf_buy
    ON eg_buy.signal_type = pf_buy.signal_type
    AND eg_buy.signal_bin = pf_buy.signal_bin
    AND eg_buy.stock_code = pf_buy.stock_code
    AND pf_buy.trade_type = 'Buy'
  -- Sell側（有効グループのみJOIN）
  LEFT JOIN `kabu-376213.kabu2411.d03_effective_4axis_groups` eg_sell
    ON cs.signal_type = eg_sell.signal_type
    AND cs.signal_bin = eg_sell.signal_bin
    AND cs.stock_code = eg_sell.stock_code
    AND eg_sell.trade_type = 'Sell'
    AND eg_sell.is_effective = true
  LEFT JOIN `kabu-376213.kabu2411.d02_signal_performance_4axis` pf_sell
    ON eg_sell.signal_type = pf_sell.signal_type
    AND eg_sell.signal_bin = pf_sell.signal_bin
    AND eg_sell.stock_code = pf_sell.stock_code
    AND pf_sell.trade_type = 'Sell'
  -- 前日の価格情報
  LEFT JOIN `kabu-376213.kabu2411.daily_quotes` q
    ON cs.stock_code = REGEXP_REPLACE(q.Code, '0$', '')
    AND q.Date = cs.reference_date  -- reference_dateを使用
  WHERE (eg_buy.is_effective = true OR eg_sell.is_effective = true)
),
-- フィルタリング（Buy/Sell別々に処理）
buy_candidates AS (
  SELECT *
  FROM signals_with_performance
  WHERE buy_is_effective = true 
    AND buy_total_count >= 30
    AND buy_avg_profit >= 1.0
    AND prev_close >= 500
    AND prev_value >= 100000000
    AND NOT REGEXP_CONTAINS(stock_code, '[A-Za-z]')
),
sell_candidates AS (
  SELECT *
  FROM signals_with_performance
  WHERE sell_is_effective = true 
    AND sell_total_count >= 30
    AND sell_avg_profit >= 1.0
    AND prev_close >= 500
    AND prev_value >= 100000000
    AND NOT REGEXP_CONTAINS(stock_code, '[A-Za-z]')
),
-- Buy/Sellを統合
all_candidates AS (
  SELECT 
    signal_date,
    stock_code,
    stock_name,
    signal_type,
    signal_bin,
    'Buy' as trade_type,
    signal_value,
    signal_percentile,
    buy_total_count as group_total_count,
    buy_win_rate as group_win_rate,
    buy_avg_profit as group_avg_profit,
    buy_sharpe_ratio as group_sharpe_ratio,
    buy_composite_score as confidence_score,
    prev_close,
    prev_volume,
    prev_value
  FROM buy_candidates
  
  UNION ALL
  
  SELECT 
    signal_date,
    stock_code,
    stock_name,
    signal_type,
    signal_bin,
    'Sell' as trade_type,
    signal_value,
    signal_percentile,
    sell_total_count as group_total_count,
    sell_win_rate as group_win_rate,
    sell_avg_profit as group_avg_profit,
    sell_sharpe_ratio as group_sharpe_ratio,
    sell_composite_score as confidence_score,
    prev_close,
    prev_volume,
    prev_value
  FROM sell_candidates
),
-- 各銘柄で最良のシグナルを選択（Buy/Sell別々に最良を選ぶ）
best_signal_per_stock_and_type AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY stock_code, trade_type 
      ORDER BY group_avg_profit DESC, group_win_rate DESC  -- 期待利益率を最優先
    ) as rn_within_type
  FROM all_candidates
),
-- 各銘柄で最終的に1つを選択（Buy/Sellがある場合は期待利益率で比較）
best_signal_per_stock AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY stock_code 
      ORDER BY group_avg_profit DESC, group_win_rate DESC  -- 期待利益率を最優先
    ) as rn_final
  FROM best_signal_per_stock_and_type
  WHERE rn_within_type = 1
),
-- 最終的なランキング
final_ranking AS (
  SELECT
    signal_date,
    stock_code,
    stock_name,
    signal_type,
    signal_bin,
    trade_type,
    signal_value,
    signal_percentile,
    group_total_count,
    group_win_rate,
    group_avg_profit,
    group_sharpe_ratio,
    confidence_score,
    GREATEST(100, FLOOR(prev_volume * 0.005 / 100) * 100) as recommended_quantity,
    group_avg_profit as expected_profit_rate,
    prev_close,
    prev_volume,
    ROW_NUMBER() OVER (ORDER BY group_avg_profit DESC, group_win_rate DESC) as priority_rank
  FROM best_signal_per_stock
  WHERE rn_final = 1
)
-- 上位500銘柄を保存
SELECT
  signal_date,
  stock_code,
  stock_name,
  signal_type,
  signal_bin,
  trade_type,
  signal_value,
  signal_percentile,
  group_total_count,
  group_win_rate,
  group_avg_profit,
  group_sharpe_ratio,
  priority_rank,
  confidence_score,
  CAST(recommended_quantity AS INT64) as recommended_quantity,
  expected_profit_rate,
  prev_close,
  CAST(prev_volume AS INT64) as prev_volume,
  CURRENT_TIMESTAMP() as created_at
FROM final_ranking
WHERE priority_rank <= 500
ORDER BY priority_rank;

-- ============================================================================
-- 4. 予測履歴の保存（検証用）
-- ============================================================================
INSERT INTO `kabu-376213.kabu2411.h01_signal_predictions`
SELECT
  signal_date,
  GENERATE_UUID() as prediction_id,
  stock_code,
  stock_name,
  signal_type,
  signal_bin,
  trade_type,
  signal_value,
  expected_profit_rate,
  confidence_score,
  priority_rank,
  group_total_count,
  group_win_rate,
  group_avg_profit,
  recommended_quantity,
  prev_close as entry_price,
  CURRENT_TIMESTAMP() as created_at
FROM
  `kabu-376213.kabu2411.d04_daily_trading_signals`
WHERE
  signal_date = target_signal_date
  AND priority_rank <= 100;  -- 上位100件の予測を保存

-- ============================================================================
-- 5. 処理結果のサマリー
-- ============================================================================
WITH summary AS (
  SELECT
    trade_type,
    COUNT(*) as signal_count,
    AVG(confidence_score) as avg_confidence,
    AVG(expected_profit_rate) as avg_expected_profit,
    AVG(group_win_rate) as avg_win_rate,
    AVG(group_sharpe_ratio) as avg_sharpe_ratio,
    COUNT(CASE WHEN priority_rank <= 10 THEN 1 END) as top10_count,
    COUNT(CASE WHEN priority_rank <= 50 THEN 1 END) as top50_count
  FROM
    `kabu-376213.kabu2411.d04_daily_trading_signals`
  WHERE
    signal_date = target_signal_date
  GROUP BY
    trade_type
)
SELECT
  '取引シグナル生成が完了しました' as message,
  target_signal_date as signal_date,
  SUM(signal_count) as total_signals,
  ROUND(AVG(avg_confidence), 1) as avg_confidence,
  ROUND(AVG(avg_expected_profit), 2) as avg_expected_profit,
  ROUND(AVG(avg_sharpe_ratio), 2) as avg_sharpe_ratio,
  ARRAY_AGG(
    STRUCT(
      trade_type,
      signal_count,
      ROUND(avg_confidence, 1) as confidence,
      ROUND(avg_expected_profit, 2) as expected_profit,
      ROUND(avg_win_rate, 1) as win_rate,
      ROUND(avg_sharpe_ratio, 2) as sharpe_ratio,
      top10_count,
      top50_count
    )
    ORDER BY trade_type
  ) as trade_type_summary
FROM
  summary
GROUP BY
  target_signal_date;

-- TOP10シグナルの詳細
SELECT
  priority_rank as rank,
  stock_code,
  stock_name,
  trade_type,
  signal_type,
  ROUND(expected_profit_rate, 2) as expected_profit,
  ROUND(confidence_score, 1) as confidence,
  ROUND(group_win_rate, 1) as win_rate,
  ROUND(group_sharpe_ratio, 2) as sharpe_ratio,
  group_total_count as samples,
  recommended_quantity as quantity
FROM
  `kabu-376213.kabu2411.d04_daily_trading_signals`
WHERE
  signal_date = target_signal_date
  AND priority_rank <= 10
ORDER BY
  priority_rank;