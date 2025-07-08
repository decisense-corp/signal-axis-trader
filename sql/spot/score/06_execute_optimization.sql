-- ============================================================================
-- ファイル名: 06_execute_optimization.sql
-- 作成日: 2025-01-05
-- 説明: 8指標すべての最適化を順次実行
--       各指標で37回の最適化ループを実行（合計296回の最適化）
--       実行時間目安：各指標5-10分、全体で40-80分程度
-- ============================================================================

-- ============================================================================
-- 実行前の状態確認
-- ============================================================================
SELECT 
  '📊 最適化実行前の状態' as status,
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

-- ============================================================================
-- 1. H3P (HIGH_3PCT) の最適化実行
-- ============================================================================
SELECT '🚀 1/8: H3P（高値3%タッチ）の最適化を開始します' as message;

-- BUY側の最適化
CALL `kabu-376213.kabu2411.optimize_single_metric`('H3P', 'BUY');

-- SELL側の最適化
CALL `kabu-376213.kabu2411.optimize_single_metric`('H3P', 'SELL');

-- 結果確認
SELECT 
  '✅ H3P最適化結果' as metric,
  trade_type,
  COUNT(DISTINCT optimized_signal_type) as optimized_signals,
  ROUND(AVG(coefficient_of_variation), 4) as avg_cv,
  ROUND(MIN(coefficient_of_variation), 4) as min_cv,
  ROUND(MAX(coefficient_of_variation), 4) as max_cv
FROM `kabu-376213.kabu2411.optimization_history`
WHERE target_metric = 'H3P'
GROUP BY trade_type;

-- ============================================================================
-- 2. H1P (HIGH_1PCT) の最適化実行
-- ============================================================================
SELECT '🚀 2/8: H1P（高値1%タッチ）の最適化を開始します' as message;

-- BUY側の最適化
CALL `kabu-376213.kabu2411.optimize_single_metric`('H1P', 'BUY');

-- SELL側の最適化
CALL `kabu-376213.kabu2411.optimize_single_metric`('H1P', 'SELL');

-- ============================================================================
-- 3. L3P (LOW_3PCT) の最適化実行
-- ============================================================================
SELECT '🚀 3/8: L3P（安値3%タッチ）の最適化を開始します' as message;

-- BUY側の最適化
CALL `kabu-376213.kabu2411.optimize_single_metric`('L3P', 'BUY');

-- SELL側の最適化
CALL `kabu-376213.kabu2411.optimize_single_metric`('L3P', 'SELL');

-- ============================================================================
-- 4. L1P (LOW_1PCT) の最適化実行
-- ============================================================================
SELECT '🚀 4/8: L1P（安値1%タッチ）の最適化を開始します' as message;

-- BUY側の最適化
CALL `kabu-376213.kabu2411.optimize_single_metric`('L1P', 'BUY');

-- SELL側の最適化
CALL `kabu-376213.kabu2411.optimize_single_metric`('L1P', 'SELL');

-- ============================================================================
-- 5. CU3P (CLOSE_UP_3PCT) の最適化実行
-- ============================================================================
SELECT '🚀 5/8: CU3P（引け3%上昇）の最適化を開始します' as message;

-- BUY側の最適化
CALL `kabu-376213.kabu2411.optimize_single_metric`('CU3P', 'BUY');

-- SELL側の最適化
CALL `kabu-376213.kabu2411.optimize_single_metric`('CU3P', 'SELL');

-- ============================================================================
-- 6. CU1P (CLOSE_UP_1PCT) の最適化実行
-- ============================================================================
SELECT '🚀 6/8: CU1P（引け1%上昇）の最適化を開始します' as message;

-- BUY側の最適化
CALL `kabu-376213.kabu2411.optimize_single_metric`('CU1P', 'BUY');

-- SELL側の最適化
CALL `kabu-376213.kabu2411.optimize_single_metric`('CU1P', 'SELL');

-- ============================================================================
-- 7. CD3P (CLOSE_DOWN_3PCT) の最適化実行
-- ============================================================================
SELECT '🚀 7/8: CD3P（引け3%下落）の最適化を開始します' as message;

-- BUY側の最適化
CALL `kabu-376213.kabu2411.optimize_single_metric`('CD3P', 'BUY');

-- SELL側の最適化
CALL `kabu-376213.kabu2411.optimize_single_metric`('CD3P', 'SELL');

-- ============================================================================
-- 8. CD1P (CLOSE_DOWN_1PCT) の最適化実行
-- ============================================================================
SELECT '🚀 8/8: CD1P（引け1%下落）の最適化を開始します' as message;

-- BUY側の最適化
CALL `kabu-376213.kabu2411.optimize_single_metric`('CD1P', 'BUY');

-- SELL側の最適化
CALL `kabu-376213.kabu2411.optimize_single_metric`('CD1P', 'SELL');

-- ============================================================================
-- 全体の最適化結果サマリー
-- ============================================================================
WITH optimization_summary AS (
  SELECT 
    target_metric,
    trade_type,
    COUNT(DISTINCT optimized_signal_type) as signals_optimized,
    ROUND(AVG(coefficient_of_variation), 4) as avg_cv,
    ROUND(SUM(processing_time_seconds), 1) as total_seconds,
    MIN(optimized_at) as start_time,
    MAX(optimized_at) as end_time
  FROM `kabu-376213.kabu2411.optimization_history`
  GROUP BY target_metric, trade_type
)
SELECT 
  '🎉 全最適化完了サマリー' as report_type,
  target_metric,
  trade_type,
  signals_optimized,
  avg_cv,
  CONCAT(ROUND(total_seconds / 60, 1), ' 分') as processing_time,
  TIMESTAMP_DIFF(end_time, start_time, MINUTE) as elapsed_minutes
FROM optimization_summary
ORDER BY target_metric, trade_type;

-- ============================================================================
-- 最適化後の係数分布確認
-- ============================================================================
SELECT 
  '📊 最適化後の係数分布' as report_type,
  'H3P' as metric,
  ROUND(MIN(coef_h3p), 3) as min_coef,
  ROUND(PERCENTILE_CONT(coef_h3p, 0.25) OVER(), 3) as q1,
  ROUND(PERCENTILE_CONT(coef_h3p, 0.50) OVER(), 3) as median,
  ROUND(AVG(coef_h3p), 3) as mean,
  ROUND(PERCENTILE_CONT(coef_h3p, 0.75) OVER(), 3) as q3,
  ROUND(MAX(coef_h3p), 3) as max_coef
FROM `kabu-376213.kabu2411.signal_coefficients_8indicators`
WHERE trade_type = 'BUY'
LIMIT 1

UNION ALL

SELECT 
  '📊 最適化後の係数分布',
  'H1P',
  ROUND(MIN(coef_h1p), 3),
  ROUND(PERCENTILE_CONT(coef_h1p, 0.25) OVER(), 3),
  ROUND(PERCENTILE_CONT(coef_h1p, 0.50) OVER(), 3),
  ROUND(AVG(coef_h1p), 3),
  ROUND(PERCENTILE_CONT(coef_h1p, 0.75) OVER(), 3),
  ROUND(MAX(coef_h1p), 3)
FROM `kabu-376213.kabu2411.signal_coefficients_8indicators`
WHERE trade_type = 'BUY'
LIMIT 1

-- 他の指標も同様に表示

UNION ALL

SELECT 
  '📊 最適化後の係数分布',
  'L3P',
  ROUND(MIN(coef_l3p), 3),
  ROUND(PERCENTILE_CONT(coef_l3p, 0.25) OVER(), 3),
  ROUND(PERCENTILE_CONT(coef_l3p, 0.50) OVER(), 3),
  ROUND(AVG(coef_l3p), 3),
  ROUND(PERCENTILE_CONT(coef_l3p, 0.75) OVER(), 3),
  ROUND(MAX(coef_l3p), 3)
FROM `kabu-376213.kabu2411.signal_coefficients_8indicators`
WHERE trade_type = 'BUY'
LIMIT 1

UNION ALL

SELECT 
  '📊 最適化後の係数分布',
  'L1P',
  ROUND(MIN(coef_l1p), 3),
  ROUND(PERCENTILE_CONT(coef_l1p, 0.25) OVER(), 3),
  ROUND(PERCENTILE_CONT(coef_l1p, 0.50) OVER(), 3),
  ROUND(AVG(coef_l1p), 3),
  ROUND(PERCENTILE_CONT(coef_l1p, 0.75) OVER(), 3),
  ROUND(MAX(coef_l1p), 3)
FROM `kabu-376213.kabu2411.signal_coefficients_8indicators`
WHERE trade_type = 'BUY'
LIMIT 1

UNION ALL

SELECT 
  '📊 最適化後の係数分布',
  'CU3P',
  ROUND(MIN(coef_cu3p), 3),
  ROUND(PERCENTILE_CONT(coef_cu3p, 0.25) OVER(), 3),
  ROUND(PERCENTILE_CONT(coef_cu3p, 0.50) OVER(), 3),
  ROUND(AVG(coef_cu3p), 3),
  ROUND(PERCENTILE_CONT(coef_cu3p, 0.75) OVER(), 3),
  ROUND(MAX(coef_cu3p), 3)
FROM `kabu-376213.kabu2411.signal_coefficients_8indicators`
WHERE trade_type = 'BUY'
LIMIT 1

UNION ALL

SELECT 
  '📊 最適化後の係数分布',
  'CU1P',
  ROUND(MIN(coef_cu1p), 3),
  ROUND(PERCENTILE_CONT(coef_cu1p, 0.25) OVER(), 3),
  ROUND(PERCENTILE_CONT(coef_cu1p, 0.50) OVER(), 3),
  ROUND(AVG(coef_cu1p), 3),
  ROUND(PERCENTILE_CONT(coef_cu1p, 0.75) OVER(), 3),
  ROUND(MAX(coef_cu1p), 3)
FROM `kabu-376213.kabu2411.signal_coefficients_8indicators`
WHERE trade_type = 'BUY'
LIMIT 1

UNION ALL

SELECT 
  '📊 最適化後の係数分布',
  'CD3P',
  ROUND(MIN(coef_cd3p), 3),
  ROUND(PERCENTILE_CONT(coef_cd3p, 0.25) OVER(), 3),
  ROUND(PERCENTILE_CONT(coef_cd3p, 0.50) OVER(), 3),
  ROUND(AVG(coef_cd3p), 3),
  ROUND(PERCENTILE_CONT(coef_cd3p, 0.75) OVER(), 3),
  ROUND(MAX(coef_cd3p), 3)
FROM `kabu-376213.kabu2411.signal_coefficients_8indicators`
WHERE trade_type = 'BUY'
LIMIT 1

UNION ALL

SELECT 
  '📊 最適化後の係数分布',
  'CD1P',
  ROUND(MIN(coef_cd1p), 3),
  ROUND(PERCENTILE_CONT(coef_cd1p, 0.25) OVER(), 3),
  ROUND(PERCENTILE_CONT(coef_cd1p, 0.50) OVER(), 3),
  ROUND(AVG(coef_cd1p), 3),
  ROUND(PERCENTILE_CONT(coef_cd1p, 0.75) OVER(), 3),
  ROUND(MAX(coef_cd1p), 3)
FROM `kabu-376213.kabu2411.signal_coefficients_8indicators`
WHERE trade_type = 'BUY'
LIMIT 1

ORDER BY metric;

-- ============================================================================
-- 次のステップの案内
-- ============================================================================
SELECT 
  '✅ 全8指標の最適化が完了しました！' as status,
  '次のステップ: 07_calculate_all_scores.sql を実行してください' as next_step,
  '全期間・全銘柄のスコア計算（1-2分）' as next_processing_time,
  CURRENT_TIMESTAMP() as completed_at;