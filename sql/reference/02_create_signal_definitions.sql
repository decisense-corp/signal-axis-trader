/*
ファイル: 02_create_signal_definitions.sql
説明: 37種類のシグナルタイプ定義とシグナル区分（bins）の管理テーブルを作成
作成日: 2025-01-01
更新日: 2025-06-02 - WHERE FALSE構文エラーを修正
*/

-- ============================================================================
-- 1. シグナルタイプ定義マスター
-- ============================================================================
CREATE TABLE IF NOT EXISTS `kabu-376213.kabu2411.m01_signal_types` (
  signal_type STRING NOT NULL,
  signal_category STRING NOT NULL,
  description STRING,
  calculation_method STRING,
  -- パフォーマンス期待値（初期値、運用で更新）
  expected_performance STRING,
  priority_rank INT64,
  -- フラグ
  is_active BOOL DEFAULT true,
  is_score_type BOOL DEFAULT false,
  -- メタデータ
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY signal_category, signal_type
OPTIONS(
  description="37種類のシグナルタイプの定義。カテゴリ分類と計算方法を管理。"
);

-- 既存データを削除（初期構築時のみ）
DELETE FROM `kabu-376213.kabu2411.m01_signal_types` WHERE TRUE;

-- シグナルタイプデータの挿入
INSERT INTO `kabu-376213.kabu2411.m01_signal_types` 
(signal_type, signal_category, description, calculation_method, expected_performance, priority_rank, is_score_type)
VALUES
  -- 価格系（Price）8種類
  ('Close to Prev Close Ratio', 'Price', '終値の前日比率', 'close / prev_close * 100', 'MEDIUM', 10, false),
  ('Close Change Rate', 'Price', '終値の変化率', '(close - prev_close) / prev_close * 100', 'MEDIUM', 11, false),
  ('Close MA3 Deviation', 'Price', '終値の3日移動平均乖離率', 'close / ma3_close * 100', 'MEDIUM', 12, false),
  ('Close MA5 Deviation', 'Price', '終値の5日移動平均乖離率', 'close / ma5_close * 100', 'MEDIUM', 13, false),
  ('Close MA10 Deviation', 'Price', '終値の10日移動平均乖離率', 'close / ma10_close * 100', 'MEDIUM', 14, false),
  ('Close to MAX20 Ratio', 'Price', '終値の20日最大値比率', 'close / max20_close * 100', 'MEDIUM', 15, false),
  ('Close to MIN20 Ratio', 'Price', '終値の20日最小値比率', 'close / min20_close * 100', 'MEDIUM', 16, false),
  ('Close Volatility', 'Price', '終値のボラティリティ', 'stddev20_close / ma20_close * 100', 'LOW', 17, false),
  
  -- 価格レンジ系（PriceRange）5種類
  ('Close to Range Ratio', 'PriceRange', '終値の値幅比率', '(close - low) / (high - low) * 100', 'MEDIUM', 20, false),
  ('High to Close Drop Rate', 'PriceRange', '高値から終値までの下落率', '(high - close) / (high - low) * 100', 'MEDIUM', 21, false),
  ('Close to Low Rise Rate', 'PriceRange', '安値から終値までの上昇率', '(close - low) / (high - low) * 100', 'MEDIUM', 22, false),
  ('High to Close Ratio', 'PriceRange', '終値の高値比率', 'close / high * 100', 'MEDIUM', 23, false),
  ('Close to Low Ratio', 'PriceRange', '終値の安値比率', 'close / low * 100', 'MEDIUM', 24, false),
  
  -- 始値終値系（OpenClose）3種類
  ('Close to Open Ratio', 'OpenClose', '終値の始値比率', 'close / open * 100', 'MEDIUM', 30, false),
  ('Open to Close Change Rate', 'OpenClose', '始値から終値への変化率', '(close - open) / open * 100', 'MEDIUM', 31, false),
  ('Open Close Range Efficiency', 'OpenClose', '始値終値の値幅効率', '(close - open) / (high - low) * 100', 'LOW', 32, false),
  
  -- 始値系（Open）3種類
  ('Open to Range Ratio', 'Open', '始値の値幅比率', '(open - low) / (high - low) * 100', 'LOW', 40, false),
  ('High to Open Drop Rate', 'Open', '高値から始値までの下落率', '(high - open) / (high - low) * 100', 'LOW', 41, false),
  ('Open to Low Rise Rate', 'Open', '安値から始値までの上昇率', '(open - low) / (high - low) * 100', 'LOW', 42, false),
  
  -- 出来高系（Volume）4種類
  ('Volume to Prev Ratio', 'Volume', '出来高の前日比率', 'volume / prev_volume * 100', 'MEDIUM', 50, false),
  ('Volume MA3 Deviation', 'Volume', '出来高の3日移動平均乖離率', 'volume / ma3_volume * 100', 'MEDIUM', 51, false),
  ('Volume MA5 Deviation', 'Volume', '出来高の5日移動平均乖離率', 'volume / ma5_volume * 100', 'MEDIUM', 52, false),
  ('Volume MA10 Deviation', 'Volume', '出来高の10日移動平均乖離率', 'volume / ma10_volume * 100', 'MEDIUM', 53, false),
  
  -- 売買代金系（Value）4種類
  ('Value to Prev Ratio', 'Value', '売買代金の前日比率', 'value / prev_value * 100', 'MEDIUM', 60, false),
  ('Value MA3 Deviation', 'Value', '売買代金の3日移動平均乖離率', 'value / ma3_value * 100', 'MEDIUM', 61, false),
  ('Value MA5 Deviation', 'Value', '売買代金の5日移動平均乖離率', 'value / ma5_value * 100', 'MEDIUM', 62, false),
  ('Value MA10 Deviation', 'Value', '売買代金の10日移動平均乖離率', 'value / ma10_value * 100', 'MEDIUM', 63, false),
  
  -- スコア系（Score）10種類 ※高パフォーマンス期待
  ('High Price Score 3D', 'Score', '3日間高値予測スコア', 'custom_high_score_3d', 'HIGH', 1, true),
  ('High Price Score 7D', 'Score', '7日間高値予測スコア', 'custom_high_score_7d', 'HIGH', 2, true),
  ('High Price Score 9D', 'Score', '9日間高値予測スコア', 'custom_high_score_9d', 'HIGH', 3, true),
  ('High Price Score 14D', 'Score', '14日間高値予測スコア', 'custom_high_score_14d', 'HIGH', 4, true),
  ('High Price Score 20D', 'Score', '20日間高値予測スコア', 'custom_high_score_20d', 'HIGH', 5, true),
  ('Low Price Score 3D', 'Score', '3日間安値予測スコア', 'custom_low_score_3d', 'HIGH', 6, true),
  ('Low Price Score 7D', 'Score', '7日間安値予測スコア', 'custom_low_score_7d', 'HIGH', 7, true),
  ('Low Price Score 9D', 'Score', '9日間安値予測スコア', 'custom_low_score_9d', 'HIGH', 8, true),
  ('Low Price Score 14D', 'Score', '14日間安値予測スコア', 'custom_low_score_14d', 'HIGH', 9, true),
  ('Low Price Score 20D', 'Score', '20日間安値予測スコア', 'custom_low_score_20d', 'HIGH', 10, true);

-- ============================================================================
-- 2. シグナル区分（bins）定義テーブル
-- ============================================================================
CREATE TABLE IF NOT EXISTS `kabu-376213.kabu2411.m02_signal_bins`
(
  signal_type STRING,
  signal_bin INT64,
  lower_bound FLOAT64,
  upper_bound FLOAT64,
  percentile_rank FLOAT64,
  -- 統計情報（区分作成時の情報）
  sample_count INT64,
  mean_value FLOAT64,
  median_value FLOAT64,
  std_value FLOAT64,
  -- メタデータ
  calculation_date DATE,
  created_at TIMESTAMP
)
CLUSTER BY signal_type, signal_bin
OPTIONS(
  description="各シグナルタイプの値を20区分に分割する境界値。週次で更新。"
);

-- ============================================================================
-- 3. シグナルカテゴリ統計ビュー
-- ============================================================================
CREATE OR REPLACE VIEW `kabu-376213.kabu2411.v_signal_category_stats` AS
SELECT
  signal_category,
  COUNT(*) as signal_count,
  COUNTIF(is_active) as active_count,
  COUNTIF(is_score_type) as score_type_count,
  STRING_AGG(
    CASE WHEN is_active THEN signal_type END, 
    ', ' 
    ORDER BY priority_rank
  ) as active_signals,
  MIN(priority_rank) as min_priority,
  MAX(priority_rank) as max_priority
FROM
  `kabu-376213.kabu2411.m01_signal_types`
GROUP BY
  signal_category
ORDER BY
  MIN(priority_rank);

-- ============================================================================
-- 4. 初期化確認
-- ============================================================================
SELECT 
  'シグナル定義テーブルの作成が完了しました' AS message,
  COUNT(*) as total_signal_types,
  COUNTIF(is_score_type) as score_signal_types,
  COUNT(DISTINCT signal_category) as categories
FROM 
  `kabu-376213.kabu2411.m01_signal_types`;