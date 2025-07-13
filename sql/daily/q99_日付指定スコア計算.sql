-- ============================================================================
-- ファイル名: calculate_daily_scores_13indicators.sql
-- 説明: 指定日付のスコアを計算してdaily_8indicator_scoresテーブルに追加（13指標対応）
--       D010_basic_resultsをソースとして使用
-- 変更点: 方向性スコア（DIRECTION）を追加
-- ============================================================================

-- ============================================================================
-- パラメータ設定（ここを変更するだけで対象日付を変更可能）
-- ============================================================================
DECLARE target_date DATE DEFAULT DATE('2025-01-08');  -- 計算対象日付

-- 処理開始メッセージ
SELECT 
  CONCAT('🚀 ', CAST(target_date AS STRING), ' のスコア計算開始（13指標対応）') as message,
  'ソーステーブル: D010_basic_results' as source_table,
  '係数テーブル: signal_coefficients_8indicators' as coefficient_table,
  '指標数: 既存8指標 + 新4指標 + 方向性 = 13指標' as indicators_info,
  CURRENT_TIMESTAMP() as start_time;

-- ============================================================================
-- Step 1: 対象日付の既存データを削除（冪等性確保）
-- ============================================================================
DELETE FROM `kabu-376213.kabu2411.daily_8indicator_scores`
WHERE signal_date = target_date;

SELECT 
  CONCAT('✅ ', CAST(target_date AS STRING), ' の既存データ削除完了') as status,
  '次: 13指標スコア計算処理' as next_step;

-- ============================================================================
-- Step 2: 指定日付のスコア計算と保存（13指標対応）
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
 composite_score_buy, composite_score_sell,
 indicators_used_count, calculated_at)
WITH base_data AS (
  -- 指定日付×銘柄の基本データ
  SELECT DISTINCT
    signal_date,
    stock_code,
    ANY_VALUE(stock_name) as stock_name
  FROM `kabu-376213.kabu2411.D010_basic_results`
  WHERE signal_date = target_date  -- パラメータ参照
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
    cs.coef_direction as sell_coef_direction
  FROM `kabu-376213.kabu2411.D010_basic_results` d
  LEFT JOIN `kabu-376213.kabu2411.signal_coefficients_8indicators` cb
    ON d.signal_type = cb.signal_type 
    AND d.signal_bin = cb.signal_bin
    AND cb.trade_type = 'BUY'
  LEFT JOIN `kabu-376213.kabu2411.signal_coefficients_8indicators` cs
    ON d.signal_type = cs.signal_type 
    AND d.signal_bin = cs.signal_bin
    AND cs.trade_type = 'SELL'
  WHERE d.signal_date = target_date  -- パラメータ参照
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
    COUNT(DISTINCT signal_type) as indicators_used
  FROM score_components
  GROUP BY signal_date, stock_code
)
SELECT 
  bd.signal_date,
  bd.stock_code,
  bd.stock_name,
  -- 既存8指標のスコア（BUY）- 対数スケール
  ROUND(ls.log_score_buy_h3p, 6) as score_buy_h3p,
  ROUND(ls.log_score_buy_h1p, 6) as score_buy_h1p,
  ROUND(ls.log_score_buy_l3p, 6) as score_buy_l3p,
  ROUND(ls.log_score_buy_l1p, 6) as score_buy_l1p,
  ROUND(ls.log_score_buy_cu3p, 6) as score_buy_cu3p,
  ROUND(ls.log_score_buy_cu1p, 6) as score_buy_cu1p,
  ROUND(ls.log_score_buy_cd3p, 6) as score_buy_cd3p,
  ROUND(ls.log_score_buy_cd1p, 6) as score_buy_cd1p,
  -- 新4指標のスコア（BUY）- 対数スケール
  ROUND(ls.log_score_buy_ud75p, 6) as score_buy_ud75p,
  ROUND(ls.log_score_buy_dd75p, 6) as score_buy_dd75p,
  ROUND(ls.log_score_buy_uc3p, 6) as score_buy_uc3p,
  ROUND(ls.log_score_buy_dc3p, 6) as score_buy_dc3p,
  -- 方向性スコア（BUY）- 対数スケール
  ROUND(ls.log_score_buy_direction, 6) as score_buy_direction,
  -- 既存8指標のスコア（SELL）- 対数スケール
  ROUND(ls.log_score_sell_h3p, 6) as score_sell_h3p,
  ROUND(ls.log_score_sell_h1p, 6) as score_sell_h1p,
  ROUND(ls.log_score_sell_l3p, 6) as score_sell_l3p,
  ROUND(ls.log_score_sell_l1p, 6) as score_sell_l1p,
  ROUND(ls.log_score_sell_cu3p, 6) as score_sell_cu3p,
  ROUND(ls.log_score_sell_cu1p, 6) as score_sell_cu1p,
  ROUND(ls.log_score_sell_cd3p, 6) as score_sell_cd3p,
  ROUND(ls.log_score_sell_cd1p, 6) as score_sell_cd1p,
  -- 新4指標のスコア（SELL）- 対数スケール
  ROUND(ls.log_score_sell_ud75p, 6) as score_sell_ud75p,
  ROUND(ls.log_score_sell_dd75p, 6) as score_sell_dd75p,
  ROUND(ls.log_score_sell_uc3p, 6) as score_sell_uc3p,
  ROUND(ls.log_score_sell_dc3p, 6) as score_sell_dc3p,
  -- 方向性スコア（SELL）- 対数スケール
  ROUND(ls.log_score_sell_direction, 6) as score_sell_direction,
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
-- Step 3: 処理結果の確認（13指標対応）
-- ============================================================================
WITH process_summary AS (
  SELECT 
    COUNT(*) as records_created,
    COUNT(DISTINCT stock_code) as unique_stocks,
    -- 既存指標の統計
    AVG(score_buy_h3p) as avg_buy_h3p,
    MIN(score_buy_h3p) as min_buy_h3p,
    MAX(score_buy_h3p) as max_buy_h3p,
    -- 方向性スコアの統計
    AVG(score_buy_direction) as avg_buy_direction,
    MIN(score_buy_direction) as min_buy_direction,
    MAX(score_buy_direction) as max_buy_direction,
    AVG(score_sell_direction) as avg_sell_direction,
    -- スコア計算確認
    COUNT(CASE WHEN score_buy_direction IS NOT NULL THEN 1 END) as direction_buy_calculated,
    COUNT(CASE WHEN score_sell_direction IS NOT NULL THEN 1 END) as direction_sell_calculated
  FROM `kabu-376213.kabu2411.daily_8indicator_scores`
  WHERE signal_date = target_date
)
SELECT 
  CONCAT('✅ ', CAST(target_date AS STRING), ' のスコア計算完了！（13指標対応）') as status,
  CONCAT(FORMAT("%'d", records_created), ' レコード作成') as records_info,
  CONCAT(unique_stocks, ' 銘柄') as stocks_processed,
  '既存8指標 + 新4指標 + 方向性 = 13指標スコア計算完了' as indicators_summary,
  CONCAT('既存H3P(BUY)平均: ', ROUND(avg_buy_h3p, 3)) as h3p_avg,
  CONCAT('方向性(BUY)平均: ', ROUND(avg_buy_direction, 3)) as direction_buy_avg,
  CONCAT('方向性(SELL)平均: ', ROUND(avg_sell_direction, 3)) as direction_sell_avg,
  CONCAT('方向性計算件数: BUY=', direction_buy_calculated, ', SELL=', direction_sell_calculated) as direction_count,
  CURRENT_TIMESTAMP() as completed_at
FROM process_summary;

-- ============================================================================
-- Step 4: 方向性スコアの高い銘柄サンプル表示
-- ============================================================================
SELECT 
  CONCAT('🎯 ', CAST(target_date AS STRING), ' の高方向性銘柄TOP10（BUY）') as report_type,
  stock_code,
  stock_name,
  ROUND(score_buy_direction, 3) as direction_score,
  -- 参考：ボラティリティスコア
  ROUND(score_buy_h3p, 3) as h3p_score,
  ROUND(score_buy_h1p, 3) as h1p_score,
  ROUND(score_buy_cu3p, 3) as cu3p_score,
  ROUND(score_buy_cu1p, 3) as cu1p_score,
  -- 上昇系と下降系の差を見る
  ROUND((score_buy_h3p + score_buy_h1p + score_buy_cu3p + score_buy_cu1p) - 
        (score_buy_l3p + score_buy_l1p + score_buy_cd3p + score_buy_cd1p), 3) as direction_diff
FROM `kabu-376213.kabu2411.daily_8indicator_scores`
WHERE signal_date = target_date
ORDER BY score_buy_direction DESC  -- 方向性スコアでソート
LIMIT 10

UNION ALL

-- SELL側の高方向性銘柄
SELECT 
  CONCAT('🎯 ', CAST(target_date AS STRING), ' の高方向性銘柄TOP10（SELL）') as report_type,
  stock_code,
  stock_name,
  ROUND(score_sell_direction, 3) as direction_score,
  ROUND(score_sell_l3p, 3) as l3p_score,
  ROUND(score_sell_l1p, 3) as l1p_score,
  ROUND(score_sell_cd3p, 3) as cd3p_score,
  ROUND(score_sell_cd1p, 3) as cd1p_score,
  ROUND((score_sell_l3p + score_sell_l1p + score_sell_cd3p + score_sell_cd1p) - 
        (score_sell_h3p + score_sell_h1p + score_sell_cu3p + score_sell_cu1p), 3) as direction_diff
FROM `kabu-376213.kabu2411.daily_8indicator_scores`
WHERE signal_date = target_date
ORDER BY score_sell_direction DESC  -- 方向性スコアでソート
LIMIT 10;