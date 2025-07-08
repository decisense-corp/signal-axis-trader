-- ============================================================================
-- ファイル名: 07_calculate_all_scores_v2.sql
-- 作成日: 2025-01-06
-- 説明: 最適化された係数を使用して全期間・全銘柄のスコアを計算（対数スケール版）
--       アンダーフロー問題を回避するため、対数和をスコアとして使用
--       daily_8indicator_scoresテーブルに保存（過去3年分）
-- 変更点：
--       1. EXP(SUM(LN(...)))ではなく、SUM(LN(...))を直接スコアとして使用
--       2. スコアの解釈：高いほど良い（マイナスが小さいほど良い）
--       3. 下限値を0.01に引き上げ（プロシージャと同じ）
-- ============================================================================

-- ============================================================================
-- 実行前の確認
-- ============================================================================
SELECT 
  '📊 スコア計算前の確認' as status,
  COUNT(*) as total_coefficients,
  COUNT(CASE WHEN coef_h3p != 1.0 THEN 1 END) as h3p_optimized,
  COUNT(CASE WHEN coef_h1p != 1.0 THEN 1 END) as h1p_optimized,
  COUNT(CASE WHEN coef_l3p != 1.0 THEN 1 END) as l3p_optimized,
  COUNT(CASE WHEN coef_l1p != 1.0 THEN 1 END) as l1p_optimized,
  COUNT(CASE WHEN coef_cu3p != 1.0 THEN 1 END) as cu3p_optimized,
  COUNT(CASE WHEN coef_cu1p != 1.0 THEN 1 END) as cu1p_optimized,
  COUNT(CASE WHEN coef_cd3p != 1.0 THEN 1 END) as cd3p_optimized,
  COUNT(CASE WHEN coef_cd1p != 1.0 THEN 1 END) as cd1p_optimized
FROM `kabu-376213.kabu2411.signal_coefficients_8indicators`;

-- 既存のスコアデータをクリア（初回実行時）
TRUNCATE TABLE `kabu-376213.kabu2411.daily_8indicator_scores`;

-- ============================================================================
-- メインのスコア計算と保存（対数スケール版）
-- ============================================================================
INSERT INTO `kabu-376213.kabu2411.daily_8indicator_scores`
(signal_date, stock_code, stock_name,
 score_buy_h3p, score_buy_h1p, score_buy_l3p, score_buy_l1p,
 score_buy_cu3p, score_buy_cu1p, score_buy_cd3p, score_buy_cd1p,
 score_sell_h3p, score_sell_h1p, score_sell_l3p, score_sell_l1p,
 score_sell_cu3p, score_sell_cu1p, score_sell_cd3p, score_sell_cd1p,
 composite_score_buy, composite_score_sell,
 indicators_used_count, calculated_at)
WITH base_data AS (
  -- 日付×銘柄の基本データ
  SELECT DISTINCT
    signal_date,
    stock_code,
    ANY_VALUE(stock_name) as stock_name
  FROM `kabu-376213.kabu2411.D010_enhanced_analysis`
  WHERE signal_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
  GROUP BY signal_date, stock_code
),
score_components AS (
  -- 各銘柄・各日の37指標データと係数を結合
  SELECT 
    d.signal_date,
    d.stock_code,
    d.signal_type,
    d.signal_bin,
    d.trade_type,
    -- BUY側の係数
    cb.coef_h3p as buy_coef_h3p,
    cb.coef_h1p as buy_coef_h1p,
    cb.coef_l3p as buy_coef_l3p,
    cb.coef_l1p as buy_coef_l1p,
    cb.coef_cu3p as buy_coef_cu3p,
    cb.coef_cu1p as buy_coef_cu1p,
    cb.coef_cd3p as buy_coef_cd3p,
    cb.coef_cd1p as buy_coef_cd1p,
    -- SELL側の係数
    cs.coef_h3p as sell_coef_h3p,
    cs.coef_h1p as sell_coef_h1p,
    cs.coef_l3p as sell_coef_l3p,
    cs.coef_l1p as sell_coef_l1p,
    cs.coef_cu3p as sell_coef_cu3p,
    cs.coef_cu1p as sell_coef_cu1p,
    cs.coef_cd3p as sell_coef_cd3p,
    cs.coef_cd1p as sell_coef_cd1p
  FROM `kabu-376213.kabu2411.D010_enhanced_analysis` d
  LEFT JOIN `kabu-376213.kabu2411.signal_coefficients_8indicators` cb
    ON d.signal_type = cb.signal_type 
    AND d.signal_bin = cb.signal_bin
    AND cb.trade_type = 'BUY'
  LEFT JOIN `kabu-376213.kabu2411.signal_coefficients_8indicators` cs
    ON d.signal_type = cs.signal_type 
    AND d.signal_bin = cs.signal_bin
    AND cs.trade_type = 'SELL'
  WHERE d.signal_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
),
log_scores AS (
  -- 対数スケールでスコアを計算（アンダーフロー回避）
  SELECT 
    signal_date,
    stock_code,
    -- BUY側の8指標スコア（対数和）
    SUM(LN(GREATEST(buy_coef_h3p, 0.01))) as log_score_buy_h3p,
    SUM(LN(GREATEST(buy_coef_h1p, 0.01))) as log_score_buy_h1p,
    SUM(LN(GREATEST(buy_coef_l3p, 0.01))) as log_score_buy_l3p,
    SUM(LN(GREATEST(buy_coef_l1p, 0.01))) as log_score_buy_l1p,
    SUM(LN(GREATEST(buy_coef_cu3p, 0.01))) as log_score_buy_cu3p,
    SUM(LN(GREATEST(buy_coef_cu1p, 0.01))) as log_score_buy_cu1p,
    SUM(LN(GREATEST(buy_coef_cd3p, 0.01))) as log_score_buy_cd3p,
    SUM(LN(GREATEST(buy_coef_cd1p, 0.01))) as log_score_buy_cd1p,
    -- SELL側の8指標スコア（対数和）
    SUM(LN(GREATEST(sell_coef_h3p, 0.01))) as log_score_sell_h3p,
    SUM(LN(GREATEST(sell_coef_h1p, 0.01))) as log_score_sell_h1p,
    SUM(LN(GREATEST(sell_coef_l3p, 0.01))) as log_score_sell_l3p,
    SUM(LN(GREATEST(sell_coef_l1p, 0.01))) as log_score_sell_l1p,
    SUM(LN(GREATEST(sell_coef_cu3p, 0.01))) as log_score_sell_cu3p,
    SUM(LN(GREATEST(sell_coef_cu1p, 0.01))) as log_score_sell_cu1p,
    SUM(LN(GREATEST(sell_coef_cd3p, 0.01))) as log_score_sell_cd3p,
    SUM(LN(GREATEST(sell_coef_cd1p, 0.01))) as log_score_sell_cd1p,
    COUNT(DISTINCT signal_type) as indicators_used
  FROM score_components
  GROUP BY signal_date, stock_code
)
SELECT 
  bd.signal_date,
  bd.stock_code,
  bd.stock_name,
  -- 8指標のスコア（BUY）- 対数スケール
  ROUND(ls.log_score_buy_h3p, 6) as score_buy_h3p,
  ROUND(ls.log_score_buy_h1p, 6) as score_buy_h1p,
  ROUND(ls.log_score_buy_l3p, 6) as score_buy_l3p,
  ROUND(ls.log_score_buy_l1p, 6) as score_buy_l1p,
  ROUND(ls.log_score_buy_cu3p, 6) as score_buy_cu3p,
  ROUND(ls.log_score_buy_cu1p, 6) as score_buy_cu1p,
  ROUND(ls.log_score_buy_cd3p, 6) as score_buy_cd3p,
  ROUND(ls.log_score_buy_cd1p, 6) as score_buy_cd1p,
  -- 8指標のスコア（SELL）- 対数スケール
  ROUND(ls.log_score_sell_h3p, 6) as score_sell_h3p,
  ROUND(ls.log_score_sell_h1p, 6) as score_sell_h1p,
  ROUND(ls.log_score_sell_l3p, 6) as score_sell_l3p,
  ROUND(ls.log_score_sell_l1p, 6) as score_sell_l1p,
  ROUND(ls.log_score_sell_cu3p, 6) as score_sell_cu3p,
  ROUND(ls.log_score_sell_cu1p, 6) as score_sell_cu1p,
  ROUND(ls.log_score_sell_cd3p, 6) as score_sell_cd3p,
  ROUND(ls.log_score_sell_cd1p, 6) as score_sell_cd1p,
  -- 統合スコア（将来の拡張用 - 現時点ではNULL）
  NULL as composite_score_buy,
  NULL as composite_score_sell,
  -- メタデータ
  ls.indicators_used as indicators_used_count,
  CURRENT_TIMESTAMP() as calculated_at
FROM base_data bd
JOIN log_scores ls
  ON bd.signal_date = ls.signal_date
  AND bd.stock_code = ls.stock_code;

-- ============================================================================
-- 計算結果の確認
-- ============================================================================
WITH score_summary AS (
  SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT signal_date) as unique_dates,
    COUNT(DISTINCT stock_code) as unique_stocks,
    MIN(signal_date) as min_date,
    MAX(signal_date) as max_date,
    -- スコアの分布確認（対数スケール）
    ROUND(AVG(score_buy_h3p), 3) as avg_buy_h3p,
    ROUND(STDDEV(score_buy_h3p), 3) as std_buy_h3p,
    ROUND(MIN(score_buy_h3p), 3) as min_buy_h3p,
    ROUND(MAX(score_buy_h3p), 3) as max_buy_h3p
  FROM `kabu-376213.kabu2411.daily_8indicator_scores`
)
SELECT 
  '✅ スコア計算完了！（対数スケール版）' as status,
  CONCAT(FORMAT("%'d", total_records), ' レコード') as records_created,
  CONCAT(unique_dates, ' 日 × ', unique_stocks, ' 銘柄') as data_dimensions,
  CONCAT(min_date, ' 〜 ', max_date) as date_range,
  '8指標 × 2売買 = 16スコア/レコード（対数スケール）' as score_columns,
  CONCAT('H3P(BUY)平均: ', avg_buy_h3p, ' (σ=', std_buy_h3p, ')') as h3p_stats,
  '⚠️ スコアは対数値：高いほど良い（マイナスが小さいほど良い）' as score_interpretation,
  CURRENT_TIMESTAMP() as completed_at
FROM score_summary;

-- ============================================================================
-- 高スコア銘柄のサンプル（直近のBUY候補）- 対数スケール版
-- ============================================================================
SELECT 
  '🎯 本日の高スコア銘柄TOP10（BUY・H3P基準）- 対数スケール' as report_type,
  signal_date,
  stock_code,
  stock_name,
  ROUND(score_buy_h3p, 3) as h3p_log_score,
  ROUND(score_buy_h1p, 3) as h1p_log_score,
  ROUND(score_buy_cu3p, 3) as cu3p_log_score,
  ROUND(score_buy_cu1p, 3) as cu1p_log_score,
  -- 参考：元のスケールに戻した値（ただし極小値の可能性あり）
  ROUND(EXP(score_buy_h3p), 6) as h3p_original_scale
FROM `kabu-376213.kabu2411.daily_8indicator_scores`
WHERE signal_date = (
  SELECT MAX(signal_date) 
  FROM `kabu-376213.kabu2411.daily_8indicator_scores`
)
ORDER BY score_buy_h3p DESC  -- 対数スケールで高い順
LIMIT 10;

-- ============================================================================
-- スコアのユニーク性確認（対数スケール版）
-- ============================================================================
WITH daily_uniqueness AS (
  SELECT 
    signal_date,
    COUNT(DISTINCT stock_code) as total_stocks,
    COUNT(DISTINCT ROUND(score_buy_h3p, 6)) as unique_scores,
    ROUND(COUNT(DISTINCT ROUND(score_buy_h3p, 6)) * 100.0 / COUNT(DISTINCT stock_code), 2) as uniqueness_pct
  FROM `kabu-376213.kabu2411.daily_8indicator_scores`
  GROUP BY signal_date
)
SELECT 
  '📊 スコアのユニーク性分析（対数スケール版）' as analysis_type,
  MIN(uniqueness_pct) as min_uniqueness_pct,
  ROUND(AVG(uniqueness_pct), 2) as avg_uniqueness_pct,
  MAX(uniqueness_pct) as max_uniqueness_pct,
  CASE 
    WHEN AVG(uniqueness_pct) > 95 THEN '✅ 優秀：ほぼ全銘柄がユニークなスコア'
    WHEN AVG(uniqueness_pct) > 80 THEN '🔶 良好：大部分の銘柄がユニーク'
    ELSE '❌ 要改善：同一スコアが多い'
  END as evaluation
FROM daily_uniqueness;