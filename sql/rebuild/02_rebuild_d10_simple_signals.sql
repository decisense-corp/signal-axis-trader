-- ============================================================================
-- d10_simple_signals 指標数変更スクリプト
-- 作成日: 2025年7月3日
-- 目的: 指標数変更時のレスポンス容量制限対策（期間分割投入）
-- ============================================================================

-- 💡 進め方のコツ
-- ============================================================================
-- 
-- 【はまりやすいポイント】
-- ❌ 全期間一括投入 → BigQueryレスポンス容量でエラー
-- ✅ 期間分割投入 → 学習期間と検証期間に分けて成功
-- 
-- 【安全な手順】
-- 1. バックアップ → 必須（TRUNCATE前に）
-- 2. TRUNCATE → DROP+CREATEより軽い
-- 3. 期間分割投入 → 学習期間（〜2024/6/30）→ 検証期間（2024/7/1〜）
-- 4. 最小限確認 → 指標数、未来視チェックのみ
-- 
-- 【容量制限の見極め】
-- - 現在の指標数で成功 → +10指標程度なら期間分割で解決
-- - 大幅増加時 → さらに細かい期間分割を検討
-- 
-- 【復旧方法】
-- 失敗時: バックアップテーブルから復元
-- 
-- ============================================================================


-- ============================================================================
-- Step 1: d10_simple_signals バックアップ作成
-- 目的: 17指標版を安全にバックアップしてから37指標版への移行開始
-- 作成日: 2025年7月3日
-- 安全性: 既存データを完全保護
-- ============================================================================

-- 現在の状況確認
SELECT 
  '🔍 移行前状況確認' as check_type,
  COUNT(*) as current_total_records,
  COUNT(DISTINCT signal_type) as current_signal_types,
  COUNT(DISTINCT stock_code) as current_stocks,
  MIN(signal_date) as min_signal_date,
  MAX(signal_date) as max_signal_date,
  ROUND(
    (SELECT size_bytes / 1024 / 1024 FROM `kabu-376213.kabu2411.__TABLES__` WHERE table_id = 'd10_simple_signals'), 
    2
  ) as current_size_mb
FROM `kabu-376213.kabu2411.d10_simple_signals`;

-- バックアップテーブル作成（17指標版→37指標版移行用）
CREATE TABLE `kabu-376213.kabu2411.d10_simple_signals_backup_17to37_migration` AS
SELECT 
  *,
  CURRENT_TIMESTAMP() as backup_created_at,
  '17指標版から37指標版への移行前バックアップ' as backup_note
FROM `kabu-376213.kabu2411.d10_simple_signals`;

-- バックアップ完了確認
SELECT 
  '✅ バックアップ完了確認' as status,
  COUNT(*) as backup_record_count,
  COUNT(DISTINCT signal_type) as backup_signal_types,
  'バックアップテーブル: d10_simple_signals_backup_17to37_migration' as backup_table_name,
  MAX(backup_created_at) as backup_timestamp
FROM `kabu-376213.kabu2411.d10_simple_signals_backup_17to37_migration`;

-- 処理完了メッセージ
SELECT 
  '🎯 Step 1完了: バックアップ作成済み' as message,
  '⚡ 次ステップ: Step 2（データクリア）を実行してください' as next_action,
  '🛡️ 安全性: 17指標版データは完全に保護されました' as safety_note,
  CURRENT_DATETIME('Asia/Tokyo') as completion_time;

-- ============================================================================
-- Step 2: d10_simple_signals データクリア
-- 目的: 37指標投入の準備（テーブル構造は保持、データのみクリア）
-- 前提: Step 1でバックアップ済み（8,583,568件 17指標）
-- 安全性: TRUNCATE使用でスキーマ保持
-- ============================================================================

-- クリア前の最終確認
SELECT 
  '⚠️ クリア前最終確認' as warning,
  COUNT(*) as records_to_be_deleted,
  COUNT(DISTINCT signal_type) as signal_types_to_be_deleted,
  'バックアップ確認: d10_simple_signals_backup_17to37_migration' as backup_reminder
FROM `kabu-376213.kabu2411.d10_simple_signals`;

-- バックアップテーブル存在確認（安全性チェック）
SELECT 
  '🛡️ バックアップ存在確認' as safety_check,
  COUNT(*) as backup_record_count,
  CASE 
    WHEN COUNT(*) > 0 THEN '✅ バックアップ確認済み - 安全に進行可能'
    ELSE '❌ バックアップが見つかりません - 処理を中止してください'
  END as safety_status
FROM `kabu-376213.kabu2411.d10_simple_signals_backup_17to37_migration`;

-- データクリア実行（テーブル構造は保持）
TRUNCATE TABLE `kabu-376213.kabu2411.d10_simple_signals`;

-- クリア完了確認
SELECT 
  '✅ データクリア完了確認' as status,
  COUNT(*) as remaining_records_should_be_0,
  CASE 
    WHEN COUNT(*) = 0 THEN '✅ 正常にクリアされました'
    ELSE '❌ データが残っています - 確認が必要です'
  END as clear_status
FROM `kabu-376213.kabu2411.d10_simple_signals`;

-- テーブル構造確認（スキーマ保持チェック）
SELECT 
  '🔧 テーブル構造確認' as check_type,
  column_name,
  data_type,
  is_nullable,
  CASE WHEN is_partitioning_column = 'YES' THEN '🔑パーティション' ELSE '' END as partition_info,
  CASE WHEN clustering_ordinal_position IS NOT NULL THEN CONCAT('🗂️クラスタ(', clustering_ordinal_position, ')') ELSE '' END as cluster_info
FROM `kabu-376213.kabu2411.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'd10_simple_signals'
ORDER BY ordinal_position;

-- 処理完了メッセージ
SELECT 
  '🎯 Step 2完了: データクリア完了' as message,
  '⚡ 次ステップ: Step 3（学習期間投入）を実行してください' as next_action,
  '📊 準備完了: 37指標投入の準備が整いました' as preparation_status,
  '🛡️ 安全性: バックアップからいつでも復旧可能です' as safety_note,
  CURRENT_DATETIME('Asia/Tokyo') as completion_time;

-- ============================================================================
-- Step 3: 学習期間投入（37指標）
-- 対象期間: 2022年7月1日 〜 2024年6月30日
-- 指標数: 37種類（既存27種類 + 新指標10種類）
-- 戦略: 期間限定で容量制限回避
-- ============================================================================

INSERT INTO `kabu-376213.kabu2411.d10_simple_signals`
(signal_date, reference_date, stock_code, stock_name, signal_type, signal_category, signal_value)

WITH quotes_data AS (
  SELECT 
    REGEXP_REPLACE(dq.Code, '0$', '') as stock_code,
    mts.company_name as stock_name,
    dq.Date as quote_date,
    (
      SELECT MIN(tc.Date)
      FROM `kabu-376213.kabu2411.trading_calendar` tc
      WHERE tc.Date > dq.Date AND tc.HolidayDivision = '1'
    ) as signal_date,
    dq.Open, dq.High, dq.Low, dq.Close, dq.Volume, dq.TurnoverValue,
    
    -- 前日データ
    LAG(dq.Close, 1) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date) as prev_close,
    LAG(dq.Volume, 1) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date) as prev_volume,
    LAG(dq.TurnoverValue, 1) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date) as prev_value,
    
    -- 移動平均（Close）
    AVG(dq.Close) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as ma3_close,
    AVG(dq.Close) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as ma5_close,
    AVG(dq.Close) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as ma10_close,
    AVG(dq.Close) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as ma20_close,
    
    -- 移動平均（Volume）
    AVG(dq.Volume) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as ma3_volume,
    AVG(dq.Volume) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as ma5_volume,
    AVG(dq.Volume) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as ma10_volume,
    
    -- 移動平均（TurnoverValue）
    AVG(dq.TurnoverValue) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as ma3_value,
    AVG(dq.TurnoverValue) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as ma5_value,
    AVG(dq.TurnoverValue) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as ma10_value,
    
    -- 最高値・最安値
    MAX(dq.Close) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as max20_close,
    MIN(dq.Close) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as min20_close,
    
    -- 標準偏差
    STDDEV(dq.Close) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as stddev20_close,
    
    -- 🚀 新指標用の基礎計算
    AVG(CASE WHEN dq.Open > 0 THEN dq.High / dq.Open ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as avg_high_open_3d,
    AVG(CASE WHEN dq.Open > 0 THEN dq.High / dq.Open ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as avg_high_open_7d,
    AVG(CASE WHEN dq.Open > 0 THEN dq.High / dq.Open ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) as avg_high_open_9d,
    AVG(CASE WHEN dq.Open > 0 THEN dq.High / dq.Open ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as avg_high_open_14d,
    AVG(CASE WHEN dq.Open > 0 THEN dq.High / dq.Open ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as avg_high_open_20d,
    
    AVG(CASE WHEN dq.Low > 0 THEN dq.Open / dq.Low ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as avg_open_low_3d,
    AVG(CASE WHEN dq.Low > 0 THEN dq.Open / dq.Low ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as avg_open_low_7d,
    AVG(CASE WHEN dq.Low > 0 THEN dq.Open / dq.Low ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) as avg_open_low_9d,
    AVG(CASE WHEN dq.Low > 0 THEN dq.Open / dq.Low ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as avg_open_low_14d,
    AVG(CASE WHEN dq.Low > 0 THEN dq.Open / dq.Low ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as avg_open_low_20d
    
  FROM `kabu-376213.kabu2411.daily_quotes` dq
  INNER JOIN `kabu-376213.kabu2411.master_trading_stocks` mts
    ON REGEXP_REPLACE(dq.Code, '0$', '') = mts.stock_code
  WHERE dq.Date >= '2022-07-01' AND dq.Date <= '2024-06-30'  -- 🎯 学習期間のみ
)

-- 🔥 37種類のシグナル定義

-- Price signals (8 types)
SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Close to Prev Close Ratio' as signal_type, 'Price' as signal_category,
  ROUND(Close / prev_close * 100, 4) as signal_value
FROM quotes_data WHERE prev_close > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Close Change Rate', 'Price', ROUND((Close - prev_close) / prev_close * 100, 4)
FROM quotes_data WHERE prev_close > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Close MA3 Deviation', 'Price', ROUND(Close / ma3_close * 100, 4)
FROM quotes_data WHERE ma3_close > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Close MA5 Deviation', 'Price', ROUND(Close / ma5_close * 100, 4)
FROM quotes_data WHERE ma5_close > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Close MA10 Deviation', 'Price', ROUND(Close / ma10_close * 100, 4)
FROM quotes_data WHERE ma10_close > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Close to MAX20 Ratio', 'Price', ROUND(Close / max20_close * 100, 4)
FROM quotes_data WHERE max20_close > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Close to MIN20 Ratio', 'Price', ROUND(Close / min20_close * 100, 4)
FROM quotes_data WHERE min20_close > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Close Volatility', 'Price', ROUND(SAFE_DIVIDE(stddev20_close, ma20_close) * 100, 4)
FROM quotes_data WHERE ma20_close > 0 AND stddev20_close IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date

-- PriceRange signals (5 types)
UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Close to Range Ratio', 'PriceRange', ROUND(SAFE_DIVIDE(Close - Low, High - Low) * 100, 4)
FROM quotes_data WHERE High > Low AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'High to Close Drop Rate', 'PriceRange', ROUND(SAFE_DIVIDE(High - Close, High - Low) * 100, 4)
FROM quotes_data WHERE High > Low AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Close to Low Rise Rate', 'PriceRange', ROUND(SAFE_DIVIDE(Close - Low, High - Low) * 100, 4)
FROM quotes_data WHERE High > Low AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'High to Close Ratio', 'PriceRange', ROUND(Close / High * 100, 4)
FROM quotes_data WHERE High > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Close to Low Ratio', 'PriceRange', ROUND(Close / Low * 100, 4)
FROM quotes_data WHERE Low > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

-- OpenClose signals (3 types)
UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Close to Open Ratio', 'OpenClose', ROUND(Close / Open * 100, 4)
FROM quotes_data WHERE Open > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Open to Close Change Rate', 'OpenClose', ROUND((Close - Open) / Open * 100, 4)
FROM quotes_data WHERE Open > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Open Close Range Efficiency', 'OpenClose', ROUND(SAFE_DIVIDE(Close - Open, High - Low) * 100, 4)
FROM quotes_data WHERE High > Low AND signal_date IS NOT NULL AND signal_date > quote_date

-- Open signals (3 types)
UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Open to Range Ratio', 'Open', ROUND(SAFE_DIVIDE(Open - Low, High - Low) * 100, 4)
FROM quotes_data WHERE High > Low AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'High to Open Drop Rate', 'Open', ROUND(SAFE_DIVIDE(High - Open, High - Low) * 100, 4)
FROM quotes_data WHERE High > Low AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Open to Low Rise Rate', 'Open', ROUND(SAFE_DIVIDE(Open - Low, High - Low) * 100, 4)
FROM quotes_data WHERE High > Low AND signal_date IS NOT NULL AND signal_date > quote_date

-- Volume signals (4 types)
UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Volume to Prev Ratio', 'Volume', ROUND(Volume / prev_volume * 100, 4)
FROM quotes_data WHERE prev_volume > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Volume MA3 Deviation', 'Volume', ROUND(Volume / ma3_volume * 100, 4)
FROM quotes_data WHERE ma3_volume > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Volume MA5 Deviation', 'Volume', ROUND(Volume / ma5_volume * 100, 4)
FROM quotes_data WHERE ma5_volume > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Volume MA10 Deviation', 'Volume', ROUND(Volume / ma10_volume * 100, 4)
FROM quotes_data WHERE ma10_volume > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

-- Value signals (4 types)
UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Value to Prev Ratio', 'Value', ROUND(TurnoverValue / prev_value * 100, 4)
FROM quotes_data WHERE prev_value > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Value MA3 Deviation', 'Value', ROUND(TurnoverValue / ma3_value * 100, 4)
FROM quotes_data WHERE ma3_value > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Value MA5 Deviation', 'Value', ROUND(TurnoverValue / ma5_value * 100, 4)
FROM quotes_data WHERE ma5_value > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Value MA10 Deviation', 'Value', ROUND(TurnoverValue / ma10_value * 100, 4)
FROM quotes_data WHERE ma10_value > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

-- 🚀 新指標10種類（High Price Score 5種類 + Low Price Score 5種類）

-- High Price Score 3D
UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'High Price Score 3D', 'Score',
  ROUND(COALESCE(avg_high_open_3d * 50, 0) + COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + COALESCE(SAFE_DIVIDE(Close - Open, Open) * 20, 0), 4)
FROM quotes_data WHERE Open > 0 AND avg_high_open_3d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'High Price Score 7D', 'Score',
  ROUND(COALESCE(avg_high_open_7d * 50, 0) + COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + COALESCE(SAFE_DIVIDE(Close - Open, Open) * 20, 0), 4)
FROM quotes_data WHERE Open > 0 AND avg_high_open_7d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'High Price Score 9D', 'Score',
  ROUND(COALESCE(avg_high_open_9d * 50, 0) + COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + COALESCE(SAFE_DIVIDE(Close - Open, Open) * 20, 0), 4)
FROM quotes_data WHERE Open > 0 AND avg_high_open_9d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'High Price Score 14D', 'Score',
  ROUND(COALESCE(avg_high_open_14d * 50, 0) + COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + COALESCE(SAFE_DIVIDE(Close - Open, Open) * 20, 0), 4)
FROM quotes_data WHERE Open > 0 AND avg_high_open_14d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'High Price Score 20D', 'Score',
  ROUND(COALESCE(avg_high_open_20d * 50, 0) + COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + COALESCE(SAFE_DIVIDE(Close - Open, Open) * 20, 0), 4)
FROM quotes_data WHERE Open > 0 AND avg_high_open_20d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date

-- Low Price Score 3D
UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Low Price Score 3D', 'Score',
  ROUND(COALESCE(avg_open_low_3d * 50, 0) + COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + COALESCE(ABS(SAFE_DIVIDE(Close - Open, Open)) * 20, 0), 4)
FROM quotes_data WHERE Open > 0 AND avg_open_low_3d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Low Price Score 7D', 'Score',
  ROUND(COALESCE(avg_open_low_7d * 50, 0) + COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + COALESCE(ABS(SAFE_DIVIDE(Close - Open, Open)) * 20, 0), 4)
FROM quotes_data WHERE Open > 0 AND avg_open_low_7d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Low Price Score 9D', 'Score',
  ROUND(COALESCE(avg_open_low_9d * 50, 0) + COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + COALESCE(ABS(SAFE_DIVIDE(Close - Open, Open)) * 20, 0), 4)
FROM quotes_data WHERE Open > 0 AND avg_open_low_9d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Low Price Score 14D', 'Score',
  ROUND(COALESCE(avg_open_low_14d * 50, 0) + COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + COALESCE(ABS(SAFE_DIVIDE(Close - Open, Open)) * 20, 0), 4)
FROM quotes_data WHERE Open > 0 AND avg_open_low_14d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Low Price Score 20D', 'Score',
  ROUND(COALESCE(avg_open_low_20d * 50, 0) + COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + COALESCE(ABS(SAFE_DIVIDE(Close - Open, Open)) * 20, 0), 4)
FROM quotes_data WHERE Open > 0 AND avg_open_low_20d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date;

-- ============================================================================
-- Step 4: 検証期間投入（37指標）
-- 対象期間: 2024年7月1日 〜 現在
-- 指標数: 37種類（既存27種類 + 新指標10種類）
-- 前提: Step 3で学習期間投入完了
-- ============================================================================

INSERT INTO `kabu-376213.kabu2411.d10_simple_signals`
(signal_date, reference_date, stock_code, stock_name, signal_type, signal_category, signal_value)

WITH quotes_data AS (
  SELECT 
    REGEXP_REPLACE(dq.Code, '0$', '') as stock_code,
    mts.company_name as stock_name,
    dq.Date as quote_date,
    (
      SELECT MIN(tc.Date)
      FROM `kabu-376213.kabu2411.trading_calendar` tc
      WHERE tc.Date > dq.Date AND tc.HolidayDivision = '1'
    ) as signal_date,
    dq.Open, dq.High, dq.Low, dq.Close, dq.Volume, dq.TurnoverValue,
    
    -- 前日データ
    LAG(dq.Close, 1) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date) as prev_close,
    LAG(dq.Volume, 1) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date) as prev_volume,
    LAG(dq.TurnoverValue, 1) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date) as prev_value,
    
    -- 移動平均（Close）
    AVG(dq.Close) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as ma3_close,
    AVG(dq.Close) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as ma5_close,
    AVG(dq.Close) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as ma10_close,
    AVG(dq.Close) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as ma20_close,
    
    -- 移動平均（Volume）
    AVG(dq.Volume) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as ma3_volume,
    AVG(dq.Volume) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as ma5_volume,
    AVG(dq.Volume) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as ma10_volume,
    
    -- 移動平均（TurnoverValue）
    AVG(dq.TurnoverValue) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as ma3_value,
    AVG(dq.TurnoverValue) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as ma5_value,
    AVG(dq.TurnoverValue) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as ma10_value,
    
    -- 最高値・最安値
    MAX(dq.Close) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as max20_close,
    MIN(dq.Close) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as min20_close,
    
    -- 標準偏差
    STDDEV(dq.Close) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as stddev20_close,
    
    -- 🚀 新指標用の基礎計算
    AVG(CASE WHEN dq.Open > 0 THEN dq.High / dq.Open ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as avg_high_open_3d,
    AVG(CASE WHEN dq.Open > 0 THEN dq.High / dq.Open ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as avg_high_open_7d,
    AVG(CASE WHEN dq.Open > 0 THEN dq.High / dq.Open ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) as avg_high_open_9d,
    AVG(CASE WHEN dq.Open > 0 THEN dq.High / dq.Open ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as avg_high_open_14d,
    AVG(CASE WHEN dq.Open > 0 THEN dq.High / dq.Open ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as avg_high_open_20d,
    
    AVG(CASE WHEN dq.Low > 0 THEN dq.Open / dq.Low ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as avg_open_low_3d,
    AVG(CASE WHEN dq.Low > 0 THEN dq.Open / dq.Low ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as avg_open_low_7d,
    AVG(CASE WHEN dq.Low > 0 THEN dq.Open / dq.Low ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) as avg_open_low_9d,
    AVG(CASE WHEN dq.Low > 0 THEN dq.Open / dq.Low ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as avg_open_low_14d,
    AVG(CASE WHEN dq.Low > 0 THEN dq.Open / dq.Low ELSE NULL END) OVER (PARTITION BY REGEXP_REPLACE(dq.Code, '0$', '') ORDER BY dq.Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as avg_open_low_20d
    
  FROM `kabu-376213.kabu2411.daily_quotes` dq
  INNER JOIN `kabu-376213.kabu2411.master_trading_stocks` mts
    ON REGEXP_REPLACE(dq.Code, '0$', '') = mts.stock_code
  WHERE dq.Date >= '2024-07-01'  -- 🎯 検証期間のみ
)

-- 🔥 37種類のシグナル定義（Step 3と同一定義）

-- Price signals (8 types)
SELECT signal_date, quote_date as reference_date, stock_code, stock_name,
  'Close to Prev Close Ratio' as signal_type, 'Price' as signal_category,
  ROUND(Close / prev_close * 100, 4) as signal_value
FROM quotes_data WHERE prev_close > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Close Change Rate', 'Price', ROUND((Close - prev_close) / prev_close * 100, 4)
FROM quotes_data WHERE prev_close > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Close MA3 Deviation', 'Price', ROUND(Close / ma3_close * 100, 4)
FROM quotes_data WHERE ma3_close > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Close MA5 Deviation', 'Price', ROUND(Close / ma5_close * 100, 4)
FROM quotes_data WHERE ma5_close > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Close MA10 Deviation', 'Price', ROUND(Close / ma10_close * 100, 4)
FROM quotes_data WHERE ma10_close > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Close to MAX20 Ratio', 'Price', ROUND(Close / max20_close * 100, 4)
FROM quotes_data WHERE max20_close > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Close to MIN20 Ratio', 'Price', ROUND(Close / min20_close * 100, 4)
FROM quotes_data WHERE min20_close > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Close Volatility', 'Price', ROUND(SAFE_DIVIDE(stddev20_close, ma20_close) * 100, 4)
FROM quotes_data WHERE ma20_close > 0 AND stddev20_close IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date

-- PriceRange signals (5 types)
UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Close to Range Ratio', 'PriceRange', ROUND(SAFE_DIVIDE(Close - Low, High - Low) * 100, 4)
FROM quotes_data WHERE High > Low AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'High to Close Drop Rate', 'PriceRange', ROUND(SAFE_DIVIDE(High - Close, High - Low) * 100, 4)
FROM quotes_data WHERE High > Low AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Close to Low Rise Rate', 'PriceRange', ROUND(SAFE_DIVIDE(Close - Low, High - Low) * 100, 4)
FROM quotes_data WHERE High > Low AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'High to Close Ratio', 'PriceRange', ROUND(Close / High * 100, 4)
FROM quotes_data WHERE High > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Close to Low Ratio', 'PriceRange', ROUND(Close / Low * 100, 4)
FROM quotes_data WHERE Low > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

-- OpenClose signals (3 types)
UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Close to Open Ratio', 'OpenClose', ROUND(Close / Open * 100, 4)
FROM quotes_data WHERE Open > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Open to Close Change Rate', 'OpenClose', ROUND((Close - Open) / Open * 100, 4)
FROM quotes_data WHERE Open > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Open Close Range Efficiency', 'OpenClose', ROUND(SAFE_DIVIDE(Close - Open, High - Low) * 100, 4)
FROM quotes_data WHERE High > Low AND signal_date IS NOT NULL AND signal_date > quote_date

-- Open signals (3 types)
UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Open to Range Ratio', 'Open', ROUND(SAFE_DIVIDE(Open - Low, High - Low) * 100, 4)
FROM quotes_data WHERE High > Low AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'High to Open Drop Rate', 'Open', ROUND(SAFE_DIVIDE(High - Open, High - Low) * 100, 4)
FROM quotes_data WHERE High > Low AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Open to Low Rise Rate', 'Open', ROUND(SAFE_DIVIDE(Open - Low, High - Low) * 100, 4)
FROM quotes_data WHERE High > Low AND signal_date IS NOT NULL AND signal_date > quote_date

-- Volume signals (4 types)
UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Volume to Prev Ratio', 'Volume', ROUND(Volume / prev_volume * 100, 4)
FROM quotes_data WHERE prev_volume > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Volume MA3 Deviation', 'Volume', ROUND(Volume / ma3_volume * 100, 4)
FROM quotes_data WHERE ma3_volume > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Volume MA5 Deviation', 'Volume', ROUND(Volume / ma5_volume * 100, 4)
FROM quotes_data WHERE ma5_volume > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Volume MA10 Deviation', 'Volume', ROUND(Volume / ma10_volume * 100, 4)
FROM quotes_data WHERE ma10_volume > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

-- Value signals (4 types)
UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Value to Prev Ratio', 'Value', ROUND(TurnoverValue / prev_value * 100, 4)
FROM quotes_data WHERE prev_value > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Value MA3 Deviation', 'Value', ROUND(TurnoverValue / ma3_value * 100, 4)
FROM quotes_data WHERE ma3_value > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Value MA5 Deviation', 'Value', ROUND(TurnoverValue / ma5_value * 100, 4)
FROM quotes_data WHERE ma5_value > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Value MA10 Deviation', 'Value', ROUND(TurnoverValue / ma10_value * 100, 4)
FROM quotes_data WHERE ma10_value > 0 AND signal_date IS NOT NULL AND signal_date > quote_date

-- 🚀 新指標10種類（High Price Score 5種類 + Low Price Score 5種類）

-- High Price Score 3D
UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'High Price Score 3D', 'Score',
  ROUND(COALESCE(avg_high_open_3d * 50, 0) + COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + COALESCE(SAFE_DIVIDE(Close - Open, Open) * 20, 0), 4)
FROM quotes_data WHERE Open > 0 AND avg_high_open_3d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'High Price Score 7D', 'Score',
  ROUND(COALESCE(avg_high_open_7d * 50, 0) + COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + COALESCE(SAFE_DIVIDE(Close - Open, Open) * 20, 0), 4)
FROM quotes_data WHERE Open > 0 AND avg_high_open_7d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'High Price Score 9D', 'Score',
  ROUND(COALESCE(avg_high_open_9d * 50, 0) + COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + COALESCE(SAFE_DIVIDE(Close - Open, Open) * 20, 0), 4)
FROM quotes_data WHERE Open > 0 AND avg_high_open_9d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'High Price Score 14D', 'Score',
  ROUND(COALESCE(avg_high_open_14d * 50, 0) + COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + COALESCE(SAFE_DIVIDE(Close - Open, Open) * 20, 0), 4)
FROM quotes_data WHERE Open > 0 AND avg_high_open_14d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'High Price Score 20D', 'Score',
  ROUND(COALESCE(avg_high_open_20d * 50, 0) + COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + COALESCE(SAFE_DIVIDE(Close - Open, Open) * 20, 0), 4)
FROM quotes_data WHERE Open > 0 AND avg_high_open_20d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date

-- Low Price Score 3D
UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Low Price Score 3D', 'Score',
  ROUND(COALESCE(avg_open_low_3d * 50, 0) + COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + COALESCE(ABS(SAFE_DIVIDE(Close - Open, Open)) * 20, 0), 4)
FROM quotes_data WHERE Open > 0 AND avg_open_low_3d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Low Price Score 7D', 'Score',
  ROUND(COALESCE(avg_open_low_7d * 50, 0) + COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + COALESCE(ABS(SAFE_DIVIDE(Close - Open, Open)) * 20, 0), 4)
FROM quotes_data WHERE Open > 0 AND avg_open_low_7d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Low Price Score 9D', 'Score',
  ROUND(COALESCE(avg_open_low_9d * 50, 0) + COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + COALESCE(ABS(SAFE_DIVIDE(Close - Open, Open)) * 20, 0), 4)
FROM quotes_data WHERE Open > 0 AND avg_open_low_9d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Low Price Score 14D', 'Score',
  ROUND(COALESCE(avg_open_low_14d * 50, 0) + COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + COALESCE(ABS(SAFE_DIVIDE(Close - Open, Open)) * 20, 0), 4)
FROM quotes_data WHERE Open > 0 AND avg_open_low_14d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date

UNION ALL
SELECT signal_date, quote_date, stock_code, stock_name,
  'Low Price Score 20D', 'Score',
  ROUND(COALESCE(avg_open_low_20d * 50, 0) + COALESCE(SAFE_DIVIDE(High - Low, Open) * 30, 0) + COALESCE(ABS(SAFE_DIVIDE(Close - Open, Open)) * 20, 0), 4)
FROM quotes_data WHERE Open > 0 AND avg_open_low_20d IS NOT NULL AND signal_date IS NOT NULL AND signal_date > quote_date;

-- ============================================================================
-- Step 5: 最終検証（必要最小限チェック）
-- 目的: 37指標復活の成功確認のみ
-- 方針: 最小限の確認で完了とする
-- ============================================================================

-- ✅ 1. 基本完了確認（最重要）
SELECT 
  '🎉 37指標復活完了確認' as status,
  COUNT(*) as total_records,
  COUNT(DISTINCT signal_type) as signal_types_should_be_37,
  COUNT(DISTINCT stock_code) as stocks_count,
  MIN(signal_date) as min_signal_date,
  MAX(signal_date) as max_signal_date
FROM `kabu-376213.kabu2411.d10_simple_signals`;

-- ✅ 2. 未来視チェック（クリティカル）
SELECT 
  '🚨 未来視チェック' as check_type,
  COUNT(*) as future_leak_records_should_be_0
FROM `kabu-376213.kabu2411.d10_simple_signals`
WHERE signal_date <= reference_date;

-- ✅ 3. 完了メッセージ
SELECT 
  '🎯 37指標システム復活完了' as message,
  '⚡ 次段階: m30_signal_bins を37指標用に再計算してください' as next_action,
  '🚀 成果: 17指標→37指標への大幅パワーアップ完了' as achievement,
  CURRENT_DATETIME('Asia/Tokyo') as completion_time;

