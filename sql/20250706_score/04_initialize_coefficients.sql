-- ============================================================================
-- ファイル名: 04_initialize_coefficients.sql
-- 作成日: 2025-01-05
-- 説明: signal_coefficients_8indicatorsテーブルに初期データを投入
--       37指標 × 20bin × 2売買種別の全組み合わせを係数1.0で初期化
-- ============================================================================

-- ============================================================================
-- 事前確認：既存データの確認
-- ============================================================================
SELECT 
  '📊 初期化前の状態確認' as check_type,
  COUNT(*) as existing_records,
  COUNT(DISTINCT signal_type) as unique_signals,
  COUNT(DISTINCT signal_bin) as unique_bins,
  COUNT(DISTINCT trade_type) as unique_trade_types
FROM `kabu-376213.kabu2411.signal_coefficients_8indicators`;

-- ============================================================================
-- 既存データのクリア（必要に応じて実行）
-- ============================================================================
-- 初期化を繰り返す場合はコメントアウトを外す
-- TRUNCATE TABLE `kabu-376213.kabu2411.signal_coefficients_8indicators`;

-- ============================================================================
-- 係数テーブルの初期化（全係数を1.0に設定）
-- ============================================================================
INSERT INTO `kabu-376213.kabu2411.signal_coefficients_8indicators`
(signal_type, signal_bin, trade_type, 
 coef_h3p, coef_h1p, coef_l3p, coef_l1p, 
 coef_cu3p, coef_cu1p, coef_cd3p, coef_cd1p, 
 sample_count, base_probability, lift_ratio)
WITH learning_data AS (
  -- 学習期間（〜2025/5/31）のデータを集計
  SELECT 
    signal_type,
    signal_bin,
    trade_type,
    COUNT(*) as sample_count,
    
    -- 各指標のベースライン確率を計算
    AVG(CASE WHEN open_to_high_percent >= 3.0 THEN 1.0 ELSE 0.0 END) as base_h3p,
    AVG(CASE WHEN open_to_high_percent >= 1.0 THEN 1.0 ELSE 0.0 END) as base_h1p,
    AVG(CASE WHEN open_to_low_percent <= -3.0 THEN 1.0 ELSE 0.0 END) as base_l3p,
    AVG(CASE WHEN open_to_low_percent <= -1.0 THEN 1.0 ELSE 0.0 END) as base_l1p,
    AVG(CASE WHEN open_to_close_percent >= 3.0 THEN 1.0 ELSE 0.0 END) as base_cu3p,
    AVG(CASE WHEN open_to_close_percent >= 1.0 THEN 1.0 ELSE 0.0 END) as base_cu1p,
    AVG(CASE WHEN open_to_close_percent <= -3.0 THEN 1.0 ELSE 0.0 END) as base_cd3p,
    AVG(CASE WHEN open_to_close_percent <= -1.0 THEN 1.0 ELSE 0.0 END) as base_cd1p
    
  FROM `kabu-376213.kabu2411.D010_enhanced_analysis`
  WHERE signal_date <= '2025-05-31'  -- 学習期間
  GROUP BY signal_type, signal_bin, trade_type
),
market_baseline AS (
  -- マーケット全体のベースライン確率
  SELECT 
    AVG(CASE WHEN open_to_high_percent >= 3.0 THEN 1.0 ELSE 0.0 END) as market_h3p,
    AVG(CASE WHEN open_to_high_percent >= 1.0 THEN 1.0 ELSE 0.0 END) as market_h1p,
    AVG(CASE WHEN open_to_low_percent <= -3.0 THEN 1.0 ELSE 0.0 END) as market_l3p,
    AVG(CASE WHEN open_to_low_percent <= -1.0 THEN 1.0 ELSE 0.0 END) as market_l1p,
    AVG(CASE WHEN open_to_close_percent >= 3.0 THEN 1.0 ELSE 0.0 END) as market_cu3p,
    AVG(CASE WHEN open_to_close_percent >= 1.0 THEN 1.0 ELSE 0.0 END) as market_cu1p,
    AVG(CASE WHEN open_to_close_percent <= -3.0 THEN 1.0 ELSE 0.0 END) as market_cd3p,
    AVG(CASE WHEN open_to_close_percent <= -1.0 THEN 1.0 ELSE 0.0 END) as market_cd1p
  FROM `kabu-376213.kabu2411.D010_enhanced_analysis`
  WHERE signal_date <= '2025-05-31'
)
SELECT 
  ld.signal_type,
  ld.signal_bin,
  ld.trade_type,
  
  -- 全ての係数を1.0で初期化
  1.0 as coef_h3p,
  1.0 as coef_h1p,
  1.0 as coef_l3p,
  1.0 as coef_l1p,
  1.0 as coef_cu3p,
  1.0 as coef_cu1p,
  1.0 as coef_cd3p,
  1.0 as coef_cd1p,
  
  -- 統計情報
  ld.sample_count,
  
  -- 代表としてH3Pのベースライン確率を格納（後で各指標ごとに更新可能）
  ld.base_h3p as base_probability,
  
  -- リフト率（この時点では参考値）
  CASE 
    WHEN mb.market_h3p > 0 THEN ld.base_h3p / mb.market_h3p
    ELSE NULL 
  END as lift_ratio
  
FROM learning_data ld
CROSS JOIN market_baseline mb
WHERE ld.sample_count >= 10;  -- 最低10サンプル以上のパターンのみ

-- ============================================================================
-- 初期化結果の確認
-- ============================================================================
WITH initialization_summary AS (
  SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT signal_type) as unique_signals,
    COUNT(DISTINCT signal_bin) as unique_bins,
    COUNT(DISTINCT trade_type) as unique_trade_types,
    AVG(sample_count) as avg_sample_count,
    MIN(sample_count) as min_sample_count,
    MAX(sample_count) as max_sample_count
  FROM `kabu-376213.kabu2411.signal_coefficients_8indicators`
)
SELECT 
  '✅ 係数テーブル初期化完了' as status,
  CONCAT(total_records, ' レコード作成') as records_created,
  CONCAT(unique_signals, ' × ', unique_bins, ' × ', unique_trade_types, ' の組み合わせ') as combinations,
  CONCAT('平均サンプル数: ', ROUND(avg_sample_count, 0)) as avg_samples,
  CONCAT('最小サンプル数: ', min_sample_count) as min_samples,
  CONCAT('最大サンプル数: ', max_sample_count) as max_samples,
  '全係数 = 1.0 で初期化済み' as coefficient_status,
  CURRENT_TIMESTAMP() as initialized_at
FROM initialization_summary;

-- ============================================================================
-- 指標別のカバレッジ確認
-- ============================================================================
SELECT 
  '📊 指標別カバレッジ' as report_type,
  signal_type,
  COUNT(DISTINCT signal_bin) as bins_covered,
  COUNT(DISTINCT trade_type) as trade_types_covered,
  SUM(sample_count) as total_samples,
  ROUND(AVG(base_probability) * 100, 2) as avg_base_probability_pct
FROM `kabu-376213.kabu2411.signal_coefficients_8indicators`
GROUP BY signal_type
ORDER BY total_samples DESC
LIMIT 10;