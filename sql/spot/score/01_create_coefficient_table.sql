-- ============================================================================
-- ファイル名: 01_create_coefficient_table.sql
-- 作成日: 2025-01-05
-- 説明: 8指標係数マスターテーブルの作成
--       37シグナル × 20bin × 2売買種別（BUY/SELL）の係数を管理
-- ============================================================================

-- 既存テーブルがある場合は削除（初回構築時のみ実行）
DROP TABLE IF EXISTS `kabu-376213.kabu2411.signal_coefficients_8indicators`;

-- 8指標係数マスターテーブル作成
CREATE TABLE `kabu-376213.kabu2411.signal_coefficients_8indicators` (
  -- 主キー項目
  signal_type STRING NOT NULL,        -- 37種類のシグナルタイプ（RSI, MACD等）
  signal_bin INT64 NOT NULL,          -- 分位（1-20）、1が最も強いシグナル
  trade_type STRING NOT NULL,         -- 売買種別（BUY/SELL）
  
  -- 8指標の係数（短縮版命名）
  coef_h3p FLOAT64 DEFAULT 1.0,      -- HIGH_3PCT係数: 寄付→高値3%以上
  coef_h1p FLOAT64 DEFAULT 1.0,      -- HIGH_1PCT係数: 寄付→高値1%以上
  coef_l3p FLOAT64 DEFAULT 1.0,      -- LOW_3PCT係数: 寄付→安値3%以上下落
  coef_l1p FLOAT64 DEFAULT 1.0,      -- LOW_1PCT係数: 寄付→安値1%以上下落
  coef_cu3p FLOAT64 DEFAULT 1.0,     -- CLOSE_UP_3PCT係数: 寄引3%以上上昇
  coef_cu1p FLOAT64 DEFAULT 1.0,     -- CLOSE_UP_1PCT係数: 寄引1%以上上昇
  coef_cd3p FLOAT64 DEFAULT 1.0,     -- CLOSE_DOWN_3PCT係数: 寄引3%以上下落
  coef_cd1p FLOAT64 DEFAULT 1.0,     -- CLOSE_DOWN_1PCT係数: 寄引1%以上下落
  
  -- 統計情報
  sample_count INT64,                 -- 学習に使用したサンプル数
  base_probability FLOAT64,           -- マーケット全体での発生確率（ベースライン）
  lift_ratio FLOAT64,                -- マーケット対比の上昇率（1.0が標準、2.0で2倍）
  
  -- メタデータ
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
-- パフォーマンス最適化のためのクラスタリング
CLUSTER BY signal_type, trade_type
OPTIONS(
  description='8指標（H3P/H1P/L3P/L1P/CU3P/CU1P/CD3P/CD1P）の係数を管理するマスターテーブル。逐次最適化により各係数を更新。'
);

-- ============================================================================
-- テーブル作成確認
-- ============================================================================
SELECT 
  '✅ signal_coefficients_8indicators テーブル作成完了' as status,
  '8指標 × 37シグナル × 20bin × 2売買種別 = 最大11,840レコード' as capacity,
  'H3P/H1P/L3P/L1P/CU3P/CU1P/CD3P/CD1P の短縮命名規則' as naming_convention,
  'デフォルト係数: 1.0（全指標均等）' as initial_value,
  CURRENT_TIMESTAMP() as created_at;