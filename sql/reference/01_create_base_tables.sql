/*
ファイル: 01_create_base_tables.sql
説明: 株式取引シグナルシステムの基本テーブルを作成
作成日: 2025-01-01
更新日: 2025-06-02 - WHERE FALSE構文エラーを修正
*/

-- ============================================================================
-- 1. データセット作成（存在しない場合）
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS `kabu-376213.kabu2411`
OPTIONS(
  description="株式取引シグナルシステム",
  location="asia-northeast2"
);

-- ============================================================================
-- 2. 既存システムテーブル（p01_update_quotes_historical.pyで使用）
-- ============================================================================

-- 日次株価データテーブル
CREATE TABLE IF NOT EXISTS `kabu-376213.kabu2411.daily_quotes`
(
  Date DATE NOT NULL,
  Code STRING NOT NULL,
  Open FLOAT64,
  High FLOAT64,
  Low FLOAT64,
  Close FLOAT64,
  Volume INT64,
  AdjustmentFactor FLOAT64,
  AdjustmentOpen FLOAT64,
  AdjustmentHigh FLOAT64,
  AdjustmentLow FLOAT64,
  AdjustmentClose FLOAT64,
  AdjustmentVolume FLOAT64
)
PARTITION BY Date
CLUSTER BY Code
OPTIONS(
  description="日次株価データ（J-Quantsから取得）",
  partition_expiration_days=NULL  -- 無期限保持
);

-- 上場銘柄情報テーブル
CREATE TABLE IF NOT EXISTS `kabu-376213.kabu2411.listed_info`
(
  Date DATE,
  Code INT64,
  CompanyName STRING,
  Sector17Code STRING,
  Sector17CodeName STRING,
  Sector33Code STRING,
  Sector33CodeName STRING,
  ScaleCategory STRING,
  MarketCode STRING,
  MarketCodeName STRING
)
CLUSTER BY Code
OPTIONS(
  description="上場銘柄情報（J-Quantsから取得）"
);

-- 取引カレンダーテーブル
CREATE TABLE IF NOT EXISTS `kabu-376213.kabu2411.trading_calendar`
(
  Date DATE NOT NULL,
  HolidayDivision STRING
)
CLUSTER BY Date
OPTIONS(
  description="取引カレンダー（1:営業日、それ以外:休場日）"
);

-- 最終更新日管理テーブル
CREATE TABLE IF NOT EXISTS `kabu-376213.kabu2411.last_success_date`
(
  last_success_date DATE NOT NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS(
  description="株価データの最終更新日を管理"
);

-- コードマッピングテーブル
CREATE TABLE IF NOT EXISTS `kabu-376213.kabu2411.code_mapping`
(
  original_code STRING NOT NULL,
  standard_code STRING NOT NULL,
  company_name STRING,
  market_code STRING,
  sector_code STRING,
  last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY original_code, standard_code
OPTIONS(
  description="銘柄コードのマッピング（末尾0の処理用）"
);

-- ============================================================================
-- 3. 生シグナルデータテーブル
-- ============================================================================
CREATE TABLE IF NOT EXISTS `kabu-376213.kabu2411.d01_signals_raw`
(
  signal_date DATE,
  reference_date DATE,
  stock_code STRING,
  stock_name STRING,
  signal_type STRING,
  signal_value FLOAT64,
  signal_category STRING,
  created_at TIMESTAMP
)
PARTITION BY signal_date
CLUSTER BY stock_code, signal_type
OPTIONS(
  description="生シグナル値を保存するテーブル。日次で追加されていく。",
  partition_expiration_days=1095  -- 3年間保持
);

-- ============================================================================
-- 4. 4軸グループパフォーマンステーブル
-- ============================================================================
CREATE TABLE IF NOT EXISTS `kabu-376213.kabu2411.d02_signal_performance_4axis`
(
  signal_type STRING,
  signal_bin INT64,
  trade_type STRING,
  stock_code STRING,
  stock_name STRING,
  -- 実績統計
  total_count INT64,
  win_count INT64,
  win_rate FLOAT64,
  avg_profit_rate FLOAT64,
  median_profit_rate FLOAT64,
  std_profit_rate FLOAT64,
  sharpe_ratio FLOAT64,
  max_profit_rate FLOAT64,
  min_profit_rate FLOAT64,
  -- 期間別統計
  last_30d_count INT64,
  last_30d_win_rate FLOAT64,
  last_30d_avg_profit FLOAT64,
  last_90d_count INT64,
  last_90d_win_rate FLOAT64,
  last_90d_avg_profit FLOAT64,
  -- メタデータ
  first_signal_date DATE,
  last_signal_date DATE,
  last_updated DATE,
  updated_at TIMESTAMP
)
CLUSTER BY signal_type, signal_bin, trade_type, stock_code
OPTIONS(
  description="4軸（SignalType×Signal値×TradeType×StockCode）ごとの累積パフォーマンス統計"
);

-- ============================================================================
-- 5. 有効4軸グループテーブル
-- ============================================================================
CREATE TABLE IF NOT EXISTS `kabu-376213.kabu2411.d03_effective_4axis_groups`
(
  signal_type STRING,
  signal_bin INT64,
  trade_type STRING,
  stock_code STRING,
  -- 有効性判定
  is_effective BOOL,
  effectiveness_reason STRING,
  -- 統計サマリ
  total_count INT64,
  win_rate FLOAT64,
  avg_profit_rate FLOAT64,
  sharpe_ratio FLOAT64,
  -- 信頼性スコア
  reliability_score FLOAT64,
  stability_score FLOAT64,
  recency_score FLOAT64,
  composite_score FLOAT64,
  -- メタデータ
  evaluation_date DATE,
  updated_at TIMESTAMP
)
CLUSTER BY signal_type, signal_bin, trade_type
OPTIONS(
  description="取引に使用する有効な4軸グループ。週次で更新される。"
);

-- ============================================================================
-- 6. 日次取引シグナルテーブル
-- ============================================================================
CREATE TABLE IF NOT EXISTS `kabu-376213.kabu2411.d04_daily_trading_signals`
(
  signal_date DATE,
  stock_code STRING,
  stock_name STRING,
  signal_type STRING,
  signal_bin INT64,
  trade_type STRING,
  -- シグナル情報
  signal_value FLOAT64,
  signal_percentile FLOAT64,
  -- 4軸グループの実績
  group_total_count INT64,
  group_win_rate FLOAT64,
  group_avg_profit FLOAT64,
  group_sharpe_ratio FLOAT64,
  -- 取引推奨情報
  priority_rank INT64,
  confidence_score FLOAT64,
  recommended_quantity INT64,
  expected_profit_rate FLOAT64,
  -- 価格情報
  prev_close FLOAT64,
  prev_volume INT64,
  -- メタデータ
  created_at TIMESTAMP
)
PARTITION BY signal_date
CLUSTER BY stock_code, priority_rank
OPTIONS(
  description="日次で生成される取引シグナル。実際の取引判断に使用。",
  partition_expiration_days=365  -- 1年間保持
);

-- ============================================================================
-- 7. シグナル予測履歴テーブル
-- ============================================================================
CREATE TABLE IF NOT EXISTS `kabu-376213.kabu2411.h01_signal_predictions`
(
  signal_date DATE,
  prediction_id STRING,
  stock_code STRING,
  stock_name STRING,
  signal_type STRING,
  signal_bin INT64,
  trade_type STRING,
  -- 予測情報
  signal_value FLOAT64,
  expected_profit_rate FLOAT64,
  confidence_score FLOAT64,
  priority_rank INT64,
  -- 4軸グループの統計（予測時点）
  group_total_count INT64,
  group_win_rate FLOAT64,
  group_avg_profit FLOAT64,
  -- 取引設定
  recommended_quantity INT64,
  entry_price FLOAT64,
  -- メタデータ
  created_at TIMESTAMP
)
PARTITION BY signal_date
CLUSTER BY stock_code, signal_type
OPTIONS(
  description="すべての予測を記録する履歴テーブル。精度検証に使用。"
);

-- ============================================================================
-- 8. 取引結果履歴テーブル
-- ============================================================================
CREATE TABLE IF NOT EXISTS `kabu-376213.kabu2411.h02_trading_results`
(
  signal_date DATE,
  prediction_id STRING,
  stock_code STRING,
  stock_name STRING,
  signal_type STRING,
  signal_bin INT64,
  trade_type STRING,
  -- 実績情報
  open_price FLOAT64,
  high_price FLOAT64,
  low_price FLOAT64,
  close_price FLOAT64,
  volume INT64,
  -- 結果
  actual_profit_rate FLOAT64,
  result_status STRING,  -- 'WIN', 'LOSS'
  profit_amount FLOAT64,
  -- 予測との比較
  expected_profit_rate FLOAT64,
  prediction_error FLOAT64,
  -- メタデータ
  updated_at TIMESTAMP
)
PARTITION BY signal_date
CLUSTER BY stock_code, signal_type
OPTIONS(
  description="実際の取引結果を記録する履歴テーブル。予測精度の評価に使用。"
);

-- ============================================================================
-- 9. シグナルタイプ定義マスター（m01_signal_typesテーブル）
-- ============================================================================
CREATE TABLE IF NOT EXISTS `kabu-376213.kabu2411.m01_signal_types` (
  signal_type STRING NOT NULL,
  signal_category STRING NOT NULL,
  description STRING,
  calculation_method STRING,
  expected_performance STRING,
  priority_rank INT64,
  is_active BOOL DEFAULT true,
  is_score_type BOOL DEFAULT false,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY signal_category, signal_type
OPTIONS(
  description="36種類のシグナルタイプの定義。カテゴリ分類と計算方法を管理。"
);

-- ============================================================================
-- 10. シグナル区分（bins）定義テーブル
-- ============================================================================
CREATE TABLE IF NOT EXISTS `kabu-376213.kabu2411.m02_signal_bins`
(
  signal_type STRING,
  signal_bin INT64,
  lower_bound FLOAT64,
  upper_bound FLOAT64,
  percentile_rank FLOAT64,
  sample_count INT64,
  mean_value FLOAT64,
  median_value FLOAT64,
  std_value FLOAT64,
  calculation_date DATE,
  created_at TIMESTAMP
)
CLUSTER BY signal_type, signal_bin
OPTIONS(
  description="各シグナルタイプの値を20区分に分割する境界値。週次で更新。"
);

-- ============================================================================
-- 11. 初期化完了メッセージ
-- ============================================================================
SELECT 
  '基本テーブルの作成が完了しました' AS message,
  CURRENT_TIMESTAMP() AS completed_at;