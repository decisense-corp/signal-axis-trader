-- ============================================================================
-- ファイル名: calculate_daily_scores.sql
-- 説明: 指定日付のスコアを計算してdaily_8indicator_scoresテーブルに追加
--       D010_basic_resultsをソースとして使用
-- 作成日: 2025-01-09
-- ============================================================================

-- ============================================================================
-- パラメータ設定（ここを変更するだけで対象日付を変更可能）
-- ============================================================================
DECLARE target_date DATE DEFAULT DATE('2025-01-08');  -- 計算対象日付

-- 処理開始メッセージ
SELECT 
  CONCAT('🚀 ', CAST(target_date AS STRING), ' のスコア計算開始') as message,
  'ソーステーブル: D010_basic_results' as source_table,
  '係数テーブル: signal_coefficients_8indicators' as coefficient_table,
  CURRENT_TIMESTAMP() as start_time;

-- ============================================================================
-- Step 1: 対象日付の既存データを削除（冪等性確保）
-- ============================================================================
DELETE FROM `kabu-376213.kabu2411.daily_8indicator_scores`
WHERE signal_date = target_date;

SELECT 
  CONCAT('✅ ', CAST(target_date AS STRING), ' の既存データ削除完了') as status,
  '次: スコア計算処理' as next_step;

-- ============================================================================
-- Step 2: 指定日付のスコア計算と保存
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
-- Step 3: 処理結果の確認
-- ============================================================================
WITH process_summary AS (
  SELECT 
    COUNT(*) as records_created,
    COUNT(DISTINCT stock_code) as unique_stocks,
    AVG(score_buy_h3p) as avg_buy_h3p,
    MIN(score_buy_h3p) as min_buy_h3p,
    MAX(score_buy_h3p) as max_buy_h3p
  FROM `kabu-376213.kabu2411.daily_8indicator_scores`
  WHERE signal_date = target_date
)
SELECT 
  CONCAT('✅ ', CAST(target_date AS STRING), ' のスコア計算完了！') as status,
  CONCAT(FORMAT("%'d", records_created), ' レコード作成') as records_info,
  CONCAT(unique_stocks, ' 銘柄') as stocks_processed,
  CONCAT('H3P(BUY)平均: ', ROUND(avg_buy_h3p, 3)) as h3p_avg,
  CONCAT('H3P(BUY)範囲: ', ROUND(min_buy_h3p, 3), ' 〜 ', ROUND(max_buy_h3p, 3)) as h3p_range,
  CURRENT_TIMESTAMP() as completed_at
FROM process_summary;

-- ============================================================================
-- Step 4: 高スコア銘柄のサンプル表示（オプション）
-- ============================================================================
SELECT 
  CONCAT('🎯 ', CAST(target_date AS STRING), ' の高スコア銘柄TOP10（BUY・H3P基準）') as report_type,
  stock_code,
  stock_name,
  ROUND(score_buy_h3p, 3) as h3p_score,
  ROUND(score_buy_h1p, 3) as h1p_score,
  ROUND(score_buy_cu3p, 3) as cu3p_score,
  ROUND(score_buy_cu1p, 3) as cu1p_score
FROM `kabu-376213.kabu2411.daily_8indicator_scores`
WHERE signal_date = target_date
ORDER BY score_buy_h3p DESC
LIMIT 10;