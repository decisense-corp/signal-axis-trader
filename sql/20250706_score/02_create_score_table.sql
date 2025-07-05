-- ============================================================================
-- ファイル名: 02_create_score_table.sql
-- 作成日: 2025-01-05
-- 説明: 日次8指標スコアテーブルの作成
--       各銘柄・各日の8指標スコアを保存（BUY/SELL別）
--       最適化された係数を使用してスコアを計算・保存
-- ============================================================================

-- 既存テーブルがある場合は削除（初回構築時のみ実行）
DROP TABLE IF EXISTS `kabu-376213.kabu2411.daily_8indicator_scores`;

-- 日次8指標スコアテーブル作成
CREATE TABLE `kabu-376213.kabu2411.daily_8indicator_scores` (
  -- 主キー項目
  signal_date DATE NOT NULL,        -- 取引日（シグナル発生日）
  stock_code STRING NOT NULL,       -- 銘柄コード
  stock_name STRING,                -- 銘柄名
  
  -- BUY側の8指標個別スコア
  score_buy_h3p FLOAT64,           -- BUY: 高値3%タッチスコア（37指標の積）
  score_buy_h1p FLOAT64,           -- BUY: 高値1%タッチスコア（37指標の積）
  score_buy_l3p FLOAT64,           -- BUY: 安値3%タッチスコア（37指標の積）
  score_buy_l1p FLOAT64,           -- BUY: 安値1%タッチスコア（37指標の積）
  score_buy_cu3p FLOAT64,          -- BUY: 引け3%上昇スコア（37指標の積）
  score_buy_cu1p FLOAT64,          -- BUY: 引け1%上昇スコア（37指標の積）
  score_buy_cd3p FLOAT64,          -- BUY: 引け3%下落スコア（37指標の積）
  score_buy_cd1p FLOAT64,          -- BUY: 引け1%下落スコア（37指標の積）
  
  -- SELL側の8指標個別スコア
  score_sell_h3p FLOAT64,          -- SELL: 高値3%タッチスコア（37指標の積）
  score_sell_h1p FLOAT64,          -- SELL: 高値1%タッチスコア（37指標の積）
  score_sell_l3p FLOAT64,          -- SELL: 安値3%タッチスコア（37指標の積）
  score_sell_l1p FLOAT64,          -- SELL: 安値1%タッチスコア（37指標の積）
  score_sell_cu3p FLOAT64,         -- SELL: 引け3%上昇スコア（37指標の積）
  score_sell_cu1p FLOAT64,         -- SELL: 引け1%上昇スコア（37指標の積）
  score_sell_cd3p FLOAT64,         -- SELL: 引け3%下落スコア（37指標の積）
  score_sell_cd1p FLOAT64,         -- SELL: 引け1%下落スコア（37指標の積）
  
  -- 統合スコア（将来の拡張用）
  composite_score_buy FLOAT64,      -- BUY: 8指標の統合スコア
  composite_score_sell FLOAT64,     -- SELL: 8指標の統合スコア
  
  -- メタデータ
  indicators_used_count INT64 DEFAULT 37,  -- 使用した指標数（通常37）
  calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()  -- スコア計算日時
)
-- パフォーマンス最適化
PARTITION BY signal_date
CLUSTER BY stock_code
OPTIONS(
  description='日次8指標スコアテーブル。各銘柄・各日の8指標スコアをBUY/SELL別に保存。',
  partition_expiration_days=1095  -- 3年間保持
);

-- ============================================================================
-- テーブル作成確認
-- ============================================================================
SELECT 
  '✅ daily_8indicator_scores テーブル作成完了' as status,
  '日次パーティション設定済み（3年保持）' as partition_info,
  '銘柄コードでクラスタリング' as cluster_info,
  'BUY/SELL × 8指標 = 16スコア/レコード' as score_columns,
  '日次更新で約200万レコード/年（688銘柄×250営業日×8指標）' as estimated_size,
  CURRENT_TIMESTAMP() as created_at;