/*
ファイル: create_M20_score_bins_fixed.sql
説明: M20_score_bins テーブル再作成（15指標版）
作成日: 2025年1月17日
修正内容: 
  - 28種類（H3P_BUY, H3P_SELLなど）から15種類（H3P, L3Pなど）に修正
  - BUY/SELL混合で20分位を計算
  - score_typeとtrade_typeを独立した軸として扱う
実行時間: 約5-10分
*/

-- ============================================================================
-- Part 1: 既存テーブル削除と再作成
-- ============================================================================

-- 処理開始メッセージ
SELECT 
  '🚀 M20_score_bins 再作成開始（15指標版）' as message,
  '修正内容: 28種類 → 15種類' as change,
  '期待レコード数: 15指標 × 20分位 = 300レコード' as expected_records,
  CURRENT_TIMESTAMP() as start_time;

-- 既存テーブル削除
DROP TABLE IF EXISTS `kabu-376213.kabu2411.M20_score_bins`;

-- 新テーブル作成
CREATE TABLE `kabu-376213.kabu2411.M20_score_bins` (
  score_type STRING NOT NULL,      -- 15種類のスコアタイプ（H3P, L3P等）
  score_bin INT64 NOT NULL,        -- 1-20
  lower_bound FLOAT64,             -- 下限値（含む）
  upper_bound FLOAT64,             -- 上限値（含まない）
  sample_count INT64,              -- サンプル数
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY score_type, score_bin;

-- ============================================================================
-- Part 2: 15指標の境界値計算と投入
-- ============================================================================

-- 全期間のスコア計算とM20_score_binsへの境界値投入（2025/5/31まで）
INSERT INTO `kabu-376213.kabu2411.M20_score_bins`
(score_type, score_bin, lower_bound, upper_bound, sample_count)
WITH 
-- 1. D10から学習期間データ取得してスコア計算
score_calculation AS (
  SELECT 
    d.signal_date,
    d.stock_code,
    d.signal_type,
    d.signal_bin,
    d.trade_type,
    
    -- 各指標の係数を取得
    c.coef_h3p, c.coef_h1p, c.coef_l3p, c.coef_l1p,
    c.coef_cu3p, c.coef_cu1p, c.coef_cd3p, c.coef_cd1p,
    c.coef_ud75p, c.coef_dd75p, c.coef_uc3p, c.coef_dc3p,
    c.coef_direction, c.coef_vol3p, c.coef_vol5p
    
  FROM `kabu-376213.kabu2411.D10_trading_signals` d
  JOIN `kabu-376213.kabu2411.D81_signal_coefficients_8indicators` c
    ON d.signal_type = c.signal_type 
    AND d.signal_bin = c.signal_bin
    AND d.trade_type = c.trade_type
  WHERE d.signal_date BETWEEN '2022-07-01' AND '2025-05-31'  -- 学習期間のみ
),

-- 2. 各スコアタイプの対数和を計算（BUY/SELL混合）
log_scores AS (
  SELECT 
    signal_date,
    stock_code,
    trade_type,
    -- 各スコアの対数和（37シグナルの係数の積）
    SUM(LN(GREATEST(coef_h3p, 0.01))) as log_score_h3p,
    SUM(LN(GREATEST(coef_h1p, 0.01))) as log_score_h1p,
    SUM(LN(GREATEST(coef_l3p, 0.01))) as log_score_l3p,
    SUM(LN(GREATEST(coef_l1p, 0.01))) as log_score_l1p,
    SUM(LN(GREATEST(coef_cu3p, 0.01))) as log_score_cu3p,
    SUM(LN(GREATEST(coef_cu1p, 0.01))) as log_score_cu1p,
    SUM(LN(GREATEST(coef_cd3p, 0.01))) as log_score_cd3p,
    SUM(LN(GREATEST(coef_cd1p, 0.01))) as log_score_cd1p,
    SUM(LN(GREATEST(coef_ud75p, 0.01))) as log_score_ud75p,
    SUM(LN(GREATEST(coef_dd75p, 0.01))) as log_score_dd75p,
    SUM(LN(GREATEST(coef_uc3p, 0.01))) as log_score_uc3p,
    SUM(LN(GREATEST(coef_dc3p, 0.01))) as log_score_dc3p,
    SUM(LN(GREATEST(coef_direction, 0.01))) as log_score_direction,
    SUM(LN(GREATEST(coef_vol3p, 0.01))) as log_score_vol3p,
    SUM(LN(GREATEST(coef_vol5p, 0.01))) as log_score_vol5p
  FROM score_calculation
  GROUP BY signal_date, stock_code, trade_type
),

-- 3. UNPIVOT形式に変換（15種類のみ、BUY/SELL混合）
unpivoted_scores AS (
  -- 既存8指標
  SELECT signal_date, stock_code, 'H3P' as score_type, log_score_h3p as score_value FROM log_scores
  UNION ALL
  SELECT signal_date, stock_code, 'H1P', log_score_h1p FROM log_scores
  UNION ALL
  SELECT signal_date, stock_code, 'L3P', log_score_l3p FROM log_scores
  UNION ALL
  SELECT signal_date, stock_code, 'L1P', log_score_l1p FROM log_scores
  UNION ALL
  SELECT signal_date, stock_code, 'CU3P', log_score_cu3p FROM log_scores
  UNION ALL
  SELECT signal_date, stock_code, 'CU1P', log_score_cu1p FROM log_scores
  UNION ALL
  SELECT signal_date, stock_code, 'CD3P', log_score_cd3p FROM log_scores
  UNION ALL
  SELECT signal_date, stock_code, 'CD1P', log_score_cd1p FROM log_scores
  
  -- 新4指標
  UNION ALL
  SELECT signal_date, stock_code, 'UD75P', log_score_ud75p FROM log_scores
  UNION ALL
  SELECT signal_date, stock_code, 'DD75P', log_score_dd75p FROM log_scores
  UNION ALL
  SELECT signal_date, stock_code, 'UC3P', log_score_uc3p FROM log_scores
  UNION ALL
  SELECT signal_date, stock_code, 'DC3P', log_score_dc3p FROM log_scores
  
  -- 方向性
  UNION ALL
  SELECT signal_date, stock_code, 'DIRECTION', log_score_direction FROM log_scores
  
  -- ボラティリティ（重複を避けるためDISTINCT）
  UNION ALL
  SELECT DISTINCT signal_date, stock_code, 'VOL3P', log_score_vol3p 
  FROM log_scores
  UNION ALL
  SELECT DISTINCT signal_date, stock_code, 'VOL5P', log_score_vol5p 
  FROM log_scores
),

-- 4. 20分位計算
score_with_percentiles AS (
  SELECT 
    score_type,
    score_value,
    NTILE(20) OVER (PARTITION BY score_type ORDER BY score_value) as score_bin
  FROM unpivoted_scores
),

-- 5. bin境界値の集計
bin_boundaries AS (
  SELECT 
    score_type,
    score_bin,
    MIN(score_value) as bin_min,
    MAX(score_value) as bin_max,
    COUNT(*) as sample_count
  FROM score_with_percentiles
  GROUP BY score_type, score_bin
)

-- 6. 最終的な境界値設定
SELECT 
  b1.score_type,
  b1.score_bin,
  CASE 
    WHEN b1.score_bin = 1 THEN b1.bin_min - 1
    ELSE COALESCE(b0.bin_max, b1.bin_min)
  END as lower_bound,
  b1.bin_max as upper_bound,
  b1.sample_count
FROM bin_boundaries b1
LEFT JOIN bin_boundaries b0
  ON b1.score_type = b0.score_type
  AND b1.score_bin = b0.score_bin + 1;

-- ============================================================================
-- Part 3: 結果確認
-- ============================================================================

-- 投入完了確認
SELECT 
  '✅ M20_score_bins 再作成完了' as status,
  COUNT(*) as total_records,
  COUNT(DISTINCT score_type) as score_types_count,
  '期待値: 15種類 × 20分位 = 300レコード' as expected,
  CURRENT_TIMESTAMP() as end_time
FROM `kabu-376213.kabu2411.M20_score_bins`;

-- スコアタイプ別確認
SELECT 
  '📊 スコアタイプ別レコード数' as check_type,
  score_type,
  COUNT(*) as bins_count,
  MIN(lower_bound) as min_value,
  MAX(upper_bound) as max_value,
  SUM(sample_count) as total_samples
FROM `kabu-376213.kabu2411.M20_score_bins`
GROUP BY score_type
ORDER BY score_type;

-- 15指標の確認
WITH expected_scores AS (
  SELECT score FROM UNNEST([
    'H3P', 'H1P', 'L3P', 'L1P',
    'CU3P', 'CU1P', 'CD3P', 'CD1P',
    'UD75P', 'DD75P', 'UC3P', 'DC3P',
    'DIRECTION', 'VOL3P', 'VOL5P'
  ]) as score
)
SELECT 
  '🎯 15指標の実装確認' as check_type,
  es.score as expected_score,
  CASE WHEN m.score_type IS NOT NULL THEN '✅' ELSE '❌' END as status,
  COUNT(DISTINCT m.score_bin) as bins_count
FROM expected_scores es
LEFT JOIN `kabu-376213.kabu2411.M20_score_bins` m
  ON es.score = m.score_type
GROUP BY es.score, m.score_type
ORDER BY es.score;