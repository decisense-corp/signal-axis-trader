-- ============================================================================
-- D71_two_axis_candidate_patterns_sell
-- 2軸パターン候補抽出（SELL版）
-- 
-- 期間設定（ここを変更すれば全体に反映）
-- ============================================================================
DECLARE learning_start_date DATE DEFAULT '2022-06-01';
DECLARE learning_end_date DATE DEFAULT '2025-05-31';

-- ============================================================================
-- テーブル作成
-- ============================================================================

-- 既存テーブル削除
DROP TABLE IF EXISTS `kabu-376213.kabu2411.D71_two_axis_candidate_patterns_sell`;

-- 新規作成
CREATE TABLE `kabu-376213.kabu2411.D71_two_axis_candidate_patterns_sell` AS
WITH 
-- 1. 2軸パターンの全組み合わせ生成
base_patterns AS (
  SELECT 
    a.signal_date,
    a.signal_type AS signal_type_1,
    a.signal_bin AS signal_bin_1,
    b.signal_type AS signal_type_2,
    b.signal_bin AS signal_bin_2,
    a.stock_code,
    a.baseline_profit_rate
  FROM `kabu-376213.kabu2411.D10_trading_signals` a
  INNER JOIN `kabu-376213.kabu2411.D10_trading_signals` b
    ON a.signal_date = b.signal_date
    AND a.stock_code = b.stock_code
    AND a.trade_type = b.trade_type
  WHERE 
    -- 学習期間
    a.signal_date BETWEEN learning_start_date AND learning_end_date
    AND a.trade_type = 'SELL'
    AND b.trade_type = 'SELL'
    -- 重複除外（同じ組み合わせを1回だけカウント）
    AND (a.signal_type < b.signal_type 
         OR (a.signal_type = b.signal_type AND a.signal_bin <= b.signal_bin))
),

-- 2. パターンごとの統計計算
pattern_stats AS (
  SELECT 
    signal_type_1,
    signal_bin_1,
    signal_type_2,
    signal_bin_2,
    COUNT(*) as sample_count,
    AVG(baseline_profit_rate) as avg_profit_rate,
    STDDEV(baseline_profit_rate) as stddev_profit_rate,
    -- 勝率
    SUM(CASE WHEN baseline_profit_rate > 0 THEN 1 ELSE 0 END) / COUNT(*) * 100 as win_rate,
    -- シャープレシオ
    CASE 
      WHEN STDDEV(baseline_profit_rate) > 0 
      THEN AVG(baseline_profit_rate) / STDDEV(baseline_profit_rate)
      ELSE 0 
    END as sharpe_ratio,
    -- 最大・最小利益率
    MAX(baseline_profit_rate) as max_profit_rate,
    MIN(baseline_profit_rate) as min_profit_rate
  FROM base_patterns
  GROUP BY 
    signal_type_1,
    signal_bin_1,
    signal_type_2,
    signal_bin_2
),

-- 3. ベースライン計算（全SELL取引の平均）
baseline_stats AS (
  SELECT 
    AVG(baseline_profit_rate) as baseline_avg,
    STDDEV(baseline_profit_rate) as baseline_stddev
  FROM `kabu-376213.kabu2411.D10_trading_signals`
  WHERE trade_type = 'SELL' 
    AND signal_date BETWEEN learning_start_date AND learning_end_date
)

-- 4. 最終結果の出力
SELECT 
  signal_type_1,
  signal_bin_1,
  signal_type_2,
  signal_bin_2,
  sample_count,
  ROUND(avg_profit_rate, 4) as avg_profit_rate,
  ROUND(stddev_profit_rate, 4) as stddev_profit_rate,
  ROUND(win_rate, 2) as win_rate,
  ROUND(sharpe_ratio, 3) as sharpe_ratio,
  ROUND(max_profit_rate, 4) as max_profit_rate,
  ROUND(min_profit_rate, 4) as min_profit_rate,
  -- ベースラインとの差分
  ROUND(avg_profit_rate - baseline_avg, 4) as profit_diff_from_baseline,
  -- パターンキー
  CONCAT(signal_type_1, '_', CAST(signal_bin_1 AS STRING), 'x', 
         signal_type_2, '_', CAST(signal_bin_2 AS STRING)) as pattern_key,
  -- メタ情報
  'SELL' as trade_type,
  learning_start_date as learning_start,
  learning_end_date as learning_end,
  CURRENT_TIMESTAMP() as created_at
FROM pattern_stats, baseline_stats
WHERE 
  -- フィルタ条件
  sample_count >= 300  -- 十分なサンプル数
  AND avg_profit_rate - baseline_avg >= 0.3  -- ベースライン+0.3%以上
ORDER BY avg_profit_rate DESC;

-- ============================================================================
-- 実行後の確認クエリ
-- ============================================================================

-- 結果サマリー
SELECT 
  CONCAT('期間: ', MIN(learning_start), ' ～ ', MAX(learning_end)) as period,
  COUNT(*) as total_candidate_patterns,
  ROUND(AVG(sample_count), 0) as avg_sample_count,
  ROUND(AVG(avg_profit_rate), 4) as avg_profit_rate,
  ROUND(MIN(avg_profit_rate), 4) as min_profit_rate,
  ROUND(MAX(avg_profit_rate), 4) as max_profit_rate,
  COUNT(CASE WHEN win_rate >= 50 THEN 1 END) as high_win_rate_patterns
FROM `kabu-376213.kabu2411.D71_two_axis_candidate_patterns_sell`;

-- 上位10パターン
SELECT 
  pattern_key,
  sample_count,
  avg_profit_rate,
  win_rate,
  sharpe_ratio,
  profit_diff_from_baseline
FROM `kabu-376213.kabu2411.D71_two_axis_candidate_patterns_sell`
ORDER BY avg_profit_rate DESC
LIMIT 10;

-- 指標の出現頻度
SELECT 
  signal_type,
  COUNT(*) as pattern_count,
  ROUND(AVG(avg_profit_rate), 4) as avg_profit_rate
FROM (
  SELECT signal_type_1 as signal_type, avg_profit_rate 
  FROM `kabu-376213.kabu2411.D71_two_axis_candidate_patterns_sell`
  UNION ALL
  SELECT signal_type_2 as signal_type, avg_profit_rate 
  FROM `kabu-376213.kabu2411.D71_two_axis_candidate_patterns_sell`
)
GROUP BY signal_type
ORDER BY pattern_count DESC
LIMIT 10;