-- ============================================================================
-- ファイル名: calculate_daily_scores_15indicators.sql
-- 説明: 指定日付のスコアを計算してdaily_8indicator_scoresテーブルに追加（15指標対応）
--       D010_basic_resultsをソースとして使用
-- 変更点: ボラティリティスコア（VOL3P, VOL5P）を追加
-- ============================================================================

-- ============================================================================
-- パラメータ設定（ここを変更するだけで対象日付を変更可能）
-- ============================================================================
DECLARE target_date DATE DEFAULT DATE('2025-01-08');  -- 計算対象日付

-- 処理開始メッセージ
SELECT 
  CONCAT('🚀 ', CAST(target_date AS STRING), ' のスコア計算開始（15指標対応）') as message,
  'ソーステーブル: D010_basic_results' as source_table,
  '係数テーブル: signal_coefficients_8indicators' as coefficient_table,
  '指標数: 既存8 + 新4 + 方向性 + ボラティリティ2 = 15指標' as indicators_info,
  CURRENT_TIMESTAMP() as start_time;

-- ============================================================================
-- Step 1: 対象日付の既存データを削除（冪等性確保）
-- ============================================================================
DELETE FROM `kabu-376213.kabu2411.daily_8indicator_scores`
WHERE signal_date = target_date;

SELECT 
  CONCAT('✅ ', CAST(target_date AS STRING), ' の既存データ削除完了') as status,
  '次: 15指標スコア計算処理' as next_step;

-- ============================================================================
-- Step 2: 指定日付のスコア計算と保存（15指標対応）
-- ============================================================================
INSERT INTO `kabu-376213.kabu2411.daily_8indicator_scores`
(signal_date, stock_code, stock_name,
 score_buy_h3p, score_buy_h1p, score_buy_l3p, score_buy_l1p,
 score_buy_cu3p, score_buy_cu1p, score_buy_cd3p, score_buy_cd1p,
 -- 新4指標BUY側
 score_buy_ud75p, score_buy_dd75p, score_buy_uc3p, score_buy_dc3p,
 -- 方向性スコアBUY側
 score_buy_direction,
 score_sell_h3p, score_sell_h1p, score_sell_l3p, score_sell_l1p,
 score_sell_cu3p, score_sell_cu1p, score_sell_cd3p, score_sell_cd1p,
 -- 新4指標SELL側
 score_sell_ud75p, score_sell_dd75p, score_sell_uc3p, score_sell_dc3p,
 -- 方向性スコアSELL側
 score_sell_direction,
 -- ボラティリティスコア（新規追加）
 score_volatility_3p, score_volatility_5p,
 composite_score_buy, composite_score_sell,
 indicators_used_count, calculated_at)
WITH base_data AS (
  -- 指定日付×銘柄の基本データ
  SELECT DISTINCT
    signal_date,
    stock_code,
    ANY_VALUE(stock_name) as stock_name
  FROM `kabu-376213.kabu2411.D010_basic_results`
  WHERE signal_date = target_date
  GROUP BY signal_date, stock_code
),
score_components AS (
  -- 各銘柄・指定日付の37指標データと係数を結合
  SELECT 
    d.signal_date,
    d.stock_code,
    d.signal_type,
    d.signal_bin,
    d.trade_type,
    -- BUY側の既存8指標係数
    cb.coef_h3p as buy_coef_h3p,
    cb.coef_h1p as buy_coef_h1p,
    cb.coef_l3p as buy_coef_l3p,
    cb.coef_l1p as buy_coef_l1p,
    cb.coef_cu3p as buy_coef_cu3p,
    cb.coef_cu1p as buy_coef_cu1p,
    cb.coef_cd3p as buy_coef_cd3p,
    cb.coef_cd1p as buy_coef_cd1p,
    -- BUY側の新4指標係数
    cb.coef_ud75p as buy_coef_ud75p,
    cb.coef_dd75p as buy_coef_dd75p,
    cb.coef_uc3p as buy_coef_uc3p,
    cb.coef_dc3p as buy_coef_dc3p,
    -- BUY側の方向性係数
    cb.coef_direction as buy_coef_direction,
    -- BUY側のボラティリティ係数（新規追加）
    cb.coef_vol3p as buy_coef_vol3p,
    cb.coef_vol5p as buy_coef_vol5p,
    -- SELL側の既存8指標係数
    cs.coef_h3p as sell_coef_h3p,
    cs.coef_h1p as sell_coef_h1p,
    cs.coef_l3p as sell_coef_l3p,
    cs.coef_l1p as sell_coef_l1p,
    cs.coef_cu3p as sell_coef_cu3p,
    cs.coef_cu1p as sell_coef_cu1p,
    cs.coef_cd3p as sell_coef_cd3p,
    cs.coef_cd1p as sell_coef_cd1p,
    -- SELL側の新4指標係数
    cs.coef_ud75p as sell_coef_ud75p,
    cs.coef_dd75p as sell_coef_dd75p,
    cs.coef_uc3p as sell_coef_uc3p,
    cs.coef_dc3p as sell_coef_dc3p,
    -- SELL側の方向性係数
    cs.coef_direction as sell_coef_direction,
    -- SELL側のボラティリティ係数（新規追加）
    cs.coef_vol3p as sell_coef_vol3p,
    cs.coef_vol5p as sell_coef_vol5p
  FROM `kabu-376213.kabu2411.D010_basic_results` d
  LEFT JOIN `kabu-376213.kabu2411.signal_coefficients_8indicators` cb
    ON d.signal_type = cb.signal_type 
    AND d.signal_bin = cb.signal_bin
    AND cb.trade_type = 'BUY'
  LEFT JOIN `kabu-376213.kabu2411.signal_coefficients_8indicators` cs
    ON d.signal_type = cs.signal_type 
    AND d.signal_bin = cs.signal_bin
    AND cs.trade_type = 'SELL'
  WHERE d.signal_date = target_date
),
log_scores AS (
  -- 対数スケールでスコアを計算（アンダーフロー回避）
  SELECT 
    signal_date,
    stock_code,
    -- BUY側の既存8指標スコア（対数和）
    SUM(LN(GREATEST(buy_coef_h3p, 0.01))) as log_score_buy_h3p,
    SUM(LN(GREATEST(buy_coef_h1p, 0.01))) as log_score_buy_h1p,
    SUM(LN(GREATEST(buy_coef_l3p, 0.01))) as log_score_buy_l3p,
    SUM(LN(GREATEST(buy_coef_l1p, 0.01))) as log_score_buy_l1p,
    SUM(LN(GREATEST(buy_coef_cu3p, 0.01))) as log_score_buy_cu3p,
    SUM(LN(GREATEST(buy_coef_cu1p, 0.01))) as log_score_buy_cu1p,
    SUM(LN(GREATEST(buy_coef_cd3p, 0.01))) as log_score_buy_cd3p,
    SUM(LN(GREATEST(buy_coef_cd1p, 0.01))) as log_score_buy_cd1p,
    -- BUY側の新4指標スコア（対数和）
    SUM(LN(GREATEST(buy_coef_ud75p, 0.01))) as log_score_buy_ud75p,
    SUM(LN(GREATEST(buy_coef_dd75p, 0.01))) as log_score_buy_dd75p,
    SUM(LN(GREATEST(buy_coef_uc3p, 0.01))) as log_score_buy_uc3p,
    SUM(LN(GREATEST(buy_coef_dc3p, 0.01))) as log_score_buy_dc3p,
    -- BUY側の方向性スコア（対数和）
    SUM(LN(GREATEST(buy_coef_direction, 0.01))) as log_score_buy_direction,
    -- SELL側の既存8指標スコア（対数和）
    SUM(LN(GREATEST(sell_coef_h3p, 0.01))) as log_score_sell_h3p,
    SUM(LN(GREATEST(sell_coef_h1p, 0.01))) as log_score_sell_h1p,
    SUM(LN(GREATEST(sell_coef_l3p, 0.01))) as log_score_sell_l3p,
    SUM(LN(GREATEST(sell_coef_l1p, 0.01))) as log_score_sell_l1p,
    SUM(LN(GREATEST(sell_coef_cu3p, 0.01))) as log_score_sell_cu3p,
    SUM(LN(GREATEST(sell_coef_cu1p, 0.01))) as log_score_sell_cu1p,
    SUM(LN(GREATEST(sell_coef_cd3p, 0.01))) as log_score_sell_cd3p,
    SUM(LN(GREATEST(sell_coef_cd1p, 0.01))) as log_score_sell_cd1p,
    -- SELL側の新4指標スコア（対数和）
    SUM(LN(GREATEST(sell_coef_ud75p, 0.01))) as log_score_sell_ud75p,
    SUM(LN(GREATEST(sell_coef_dd75p, 0.01))) as log_score_sell_dd75p,
    SUM(LN(GREATEST(sell_coef_uc3p, 0.01))) as log_score_sell_uc3p,
    SUM(LN(GREATEST(sell_coef_dc3p, 0.01))) as log_score_sell_dc3p,
    -- SELL側の方向性スコア（対数和）
    SUM(LN(GREATEST(sell_coef_direction, 0.01))) as log_score_sell_direction,
    -- ボラティリティスコア（BUY/SELL平均、新規追加）
    SUM(LN(GREATEST((buy_coef_vol3p + sell_coef_vol3p) / 2, 0.01))) as log_score_vol3p,
    SUM(LN(GREATEST((buy_coef_vol5p + sell_coef_vol5p) / 2, 0.01))) as log_score_vol5p,
    COUNT(DISTINCT signal_type) as indicators_used
  FROM score_components
  GROUP BY signal_date, stock_code
)
SELECT 
  bd.signal_date,
  bd.stock_code,
  bd.stock_name,
  -- 既存8指標のスコア（BUY）
  ROUND(ls.log_score_buy_h3p, 6) as score_buy_h3p,
  ROUND(ls.log_score_buy_h1p, 6) as score_buy_h1p,
  ROUND(ls.log_score_buy_l3p, 6) as score_buy_l3p,
  ROUND(ls.log_score_buy_l1p, 6) as score_buy_l1p,
  ROUND(ls.log_score_buy_cu3p, 6) as score_buy_cu3p,
  ROUND(ls.log_score_buy_cu1p, 6) as score_buy_cu1p,
  ROUND(ls.log_score_buy_cd3p, 6) as score_buy_cd3p,
  ROUND(ls.log_score_buy_cd1p, 6) as score_buy_cd1p,
  -- 新4指標のスコア（BUY）
  ROUND(ls.log_score_buy_ud75p, 6) as score_buy_ud75p,
  ROUND(ls.log_score_buy_dd75p, 6) as score_buy_dd75p,
  ROUND(ls.log_score_buy_uc3p, 6) as score_buy_uc3p,
  ROUND(ls.log_score_buy_dc3p, 6) as score_buy_dc3p,
  -- 方向性スコア（BUY）
  ROUND(ls.log_score_buy_direction, 6) as score_buy_direction,
  -- 既存8指標のスコア（SELL）
  ROUND(ls.log_score_sell_h3p, 6) as score_sell_h3p,
  ROUND(ls.log_score_sell_h1p, 6) as score_sell_h1p,
  ROUND(ls.log_score_sell_l3p, 6) as score_sell_l3p,
  ROUND(ls.log_score_sell_l1p, 6) as score_sell_l1p,
  ROUND(ls.log_score_sell_cu3p, 6) as score_sell_cu3p,
  ROUND(ls.log_score_sell_cu1p, 6) as score_sell_cu1p,
  ROUND(ls.log_score_sell_cd3p, 6) as score_sell_cd3p,
  ROUND(ls.log_score_sell_cd1p, 6) as score_sell_cd1p,
  -- 新4指標のスコア（SELL）
  ROUND(ls.log_score_sell_ud75p, 6) as score_sell_ud75p,
  ROUND(ls.log_score_sell_dd75p, 6) as score_sell_dd75p,
  ROUND(ls.log_score_sell_uc3p, 6) as score_sell_uc3p,
  ROUND(ls.log_score_sell_dc3p, 6) as score_sell_dc3p,
  -- 方向性スコア（SELL）
  ROUND(ls.log_score_sell_direction, 6) as score_sell_direction,
  -- ボラティリティスコア（新規追加）
  ROUND(ls.log_score_vol3p, 6) as score_volatility_3p,
  ROUND(ls.log_score_vol5p, 6) as score_volatility_5p,
  -- 統合スコア
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
-- Step 3: 処理結果の確認（15指標対応）
-- ============================================================================
WITH process_summary AS (
  SELECT 
    COUNT(*) as records_created,
    COUNT(DISTINCT stock_code) as unique_stocks,
    -- ボラティリティスコアの統計
    AVG(score_volatility_3p) as avg_vol3p,
    MIN(score_volatility_3p) as min_vol3p,
    MAX(score_volatility_3p) as max_vol3p,
    AVG(score_volatility_5p) as avg_vol5p,
    MIN(score_volatility_5p) as min_vol5p,
    MAX(score_volatility_5p) as max_vol5p,
    -- スコア計算確認
    COUNT(CASE WHEN score_volatility_3p IS NOT NULL THEN 1 END) as vol3p_calculated,
    COUNT(CASE WHEN score_volatility_5p IS NOT NULL THEN 1 END) as vol5p_calculated
  FROM `kabu-376213.kabu2411.daily_8indicator_scores`
  WHERE signal_date = target_date
)
SELECT 
  CONCAT('✅ ', CAST(target_date AS STRING), ' のスコア計算完了！（15指標対応）') as status,
  CONCAT(FORMAT("%'d", records_created), ' レコード作成') as records_info,
  CONCAT(unique_stocks, ' 銘柄') as stocks_processed,
  '既存8 + 新4 + 方向性 + ボラティリティ2 = 15指標完了' as indicators_summary,
  CONCAT('VOL3P平均: ', ROUND(avg_vol3p, 3), ' (', ROUND(min_vol3p, 3), '〜', ROUND(max_vol3p, 3), ')') as vol3p_stats,
  CONCAT('VOL5P平均: ', ROUND(avg_vol5p, 3), ' (', ROUND(min_vol5p, 3), '〜', ROUND(max_vol5p, 3), ')') as vol5p_stats,
  CONCAT('計算件数: VOL3P=', vol3p_calculated, ', VOL5P=', vol5p_calculated) as vol_count,
  CURRENT_TIMESTAMP() as completed_at
FROM process_summary;

-- ============================================================================
-- Step 4: 高ボラティリティ銘柄のサンプル表示
-- ============================================================================
SELECT 
  CONCAT('🎯 ', CAST(target_date AS STRING), ' の高ボラティリティ銘柄TOP10（3%）') as report_type,
  stock_code,
  stock_name,
  ROUND(score_volatility_3p, 3) as vol3p_score,
  ROUND(score_volatility_5p, 3) as vol5p_score,
  -- 参考：方向性と既存指標
  ROUND(score_buy_direction, 3) as buy_direction,
  ROUND(score_sell_direction, 3) as sell_direction,
  -- ボラティリティ関連指標の平均
  ROUND((score_buy_h3p + score_buy_l3p + score_buy_cu3p + score_buy_cd3p) / 4, 3) as avg_3p_indicators
FROM `kabu-376213.kabu2411.daily_8indicator_scores`
WHERE signal_date = target_date
ORDER BY score_volatility_3p DESC
LIMIT 10

UNION ALL

SELECT 
  CONCAT('🎯 ', CAST(target_date AS STRING), ' の高ボラティリティ銘柄TOP10（5%）') as report_type,
  stock_code,
  stock_name,
  ROUND(score_volatility_3p, 3) as vol3p_score,
  ROUND(score_volatility_5p, 3) as vol5p_score,
  ROUND(score_buy_direction, 3) as buy_direction,
  ROUND(score_sell_direction, 3) as sell_direction,
  ROUND((score_buy_h3p + score_buy_l3p + score_buy_cu3p + score_buy_cd3p) / 4, 3) as avg_3p_indicators
FROM `kabu-376213.kabu2411.daily_8indicator_scores`
WHERE signal_date = target_date
ORDER BY score_volatility_5p DESC
LIMIT 10;