-- ============================================================================
-- ファイル名: 03_create_optimization_history_table.sql
-- 作成日: 2025-01-05
-- 説明: 逐次最適化の処理履歴を管理するテーブル
--       37回の最適化ラウンドで処理済み指標を記録し、重複処理を防ぐ
-- ============================================================================

-- 既存テーブルがある場合は削除（初回構築時のみ実行）
DROP TABLE IF EXISTS `kabu-376213.kabu2411.optimization_history`;

-- 最適化履歴管理テーブル作成
CREATE TABLE `kabu-376213.kabu2411.optimization_history` (
  -- 最適化ラウンド情報
  optimization_round INT64 NOT NULL,      -- 最適化ラウンド（1-37）
  target_metric STRING NOT NULL,          -- 対象指標（H3P, H1P, L3P, L1P, CU3P, CU1P, CD3P, CD1P）
  trade_type STRING NOT NULL,             -- 売買種別（BUY/SELL）
  
  -- 最適化された指標情報
  optimized_signal_type STRING NOT NULL,  -- 最適化された指標名（例: RSI, MACD等）
  coefficient_of_variation FLOAT64,       -- 変動係数（CV値）= 説明力の指標
  
  -- 最適化結果の統計
  bins_updated INT64,                     -- 更新されたbin数（通常20）
  avg_coefficient FLOAT64,                -- 更新後の平均係数（正規化により1.0になるはず）
  min_coefficient FLOAT64,                -- 更新後の最小係数
  max_coefficient FLOAT64,                -- 更新後の最大係数
  
  -- 補正前後の比較情報
  raw_avg_touch_rate FLOAT64,             -- 補正前の平均タッチ率
  corrected_avg_touch_rate FLOAT64,       -- 補正後の平均タッチ率
  improvement_ratio FLOAT64,              -- 改善率（補正後/補正前）
  
  -- 処理情報
  sample_count INT64,                     -- 最適化に使用したサンプル数
  processing_time_seconds FLOAT64,        -- 処理時間（秒）
  optimized_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),  -- 最適化実行日時
  
  -- 制約
  PRIMARY KEY (optimization_round, target_metric, trade_type) NOT ENFORCED
)
CLUSTER BY target_metric, trade_type
OPTIONS(
  description='逐次最適化の処理履歴。37ラウンド×8指標×2売買種別の最適化過程を記録。'
);

-- ============================================================================
-- インデックス作成（クエリ最適化用）
-- ============================================================================
-- BigQueryでは不要（クラスタリングで対応）

-- ============================================================================
-- ビュー作成：未処理指標の確認用
-- ============================================================================
CREATE OR REPLACE VIEW `kabu-376213.kabu2411.v_unprocessed_indicators` AS
WITH all_combinations AS (
  -- 全ての処理対象組み合わせ
  SELECT DISTINCT
    signal_type,
    target_metric,
    trade_type
  FROM 
    UNNEST(['H3P', 'H1P', 'L3P', 'L1P', 'CU3P', 'CU1P', 'CD3P', 'CD1P']) AS target_metric,
    UNNEST(['BUY', 'SELL']) AS trade_type,
    (SELECT DISTINCT signal_type FROM `kabu-376213.kabu2411.D010_enhanced_analysis`) AS signals
),
processed AS (
  -- 処理済みの組み合わせ
  SELECT DISTINCT
    optimized_signal_type AS signal_type,
    target_metric,
    trade_type
  FROM `kabu-376213.kabu2411.optimization_history`
)
SELECT 
  ac.target_metric,
  ac.trade_type,
  ac.signal_type,
  CASE WHEN p.signal_type IS NULL THEN '未処理' ELSE '処理済' END AS status
FROM all_combinations ac
LEFT JOIN processed p
  ON ac.signal_type = p.signal_type 
  AND ac.target_metric = p.target_metric
  AND ac.trade_type = p.trade_type
ORDER BY 
  ac.target_metric,
  ac.trade_type,
  CASE WHEN p.signal_type IS NULL THEN 0 ELSE 1 END,  -- 未処理を先に表示
  ac.signal_type;

-- ============================================================================
-- テーブル作成確認
-- ============================================================================
SELECT 
  '✅ optimization_history テーブル作成完了' as status,
  '最大レコード数: 37ラウンド × 8指標 × 2売買種別 = 592レコード' as max_records,
  '✅ v_unprocessed_indicators ビュー作成完了' as view_status,
  '未処理指標の確認が容易になりました' as benefit,
  CURRENT_TIMESTAMP() as created_at;