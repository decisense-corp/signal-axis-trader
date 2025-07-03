/*
ファイル: 01_create_D010_table_structure.sql
説明: Signal Axis Trader - D010_basic_results テーブル作成（1日ずれバグ修正版）
作成日: 2025年7月4日
目的: 新設計書準拠のテーブル構造作成（データ投入は別ファイル）
注意: テーブル構造は変更なし、後続のデータ投入SQLで日付ロジックを修正
*/

-- ============================================================================
-- D010_basic_results テーブル作成（設計書完全準拠）
-- ============================================================================

-- 処理開始メッセージ
SELECT 
  'D010_basic_results テーブル作成開始' as message,
  '設計方針: 新設計書完全準拠・1日ずれバグ対策済み' as design_principle,
  'テーブル構造: 37指標×3年間対応' as structure_info,
  '次工程: データ投入SQL（日付ロジック修正版）' as next_step,
  CURRENT_TIMESTAMP() as start_time;

-- ============================================================================
-- Step 1: 既存テーブル確認と削除
-- ============================================================================

-- 既存テーブルの確認
SELECT 
  'Step 1: 既存テーブル確認' as check_step,
  (
    SELECT COUNT(*) 
    FROM `kabu-376213.kabu2411.INFORMATION_SCHEMA.TABLES` 
    WHERE table_name = 'D010_basic_results'
  ) as table_exists,
  CASE 
    WHEN (
      SELECT COUNT(*) 
      FROM `kabu-376213.kabu2411.INFORMATION_SCHEMA.TABLES` 
      WHERE table_name = 'D010_basic_results'
    ) > 0 THEN 'テーブル存在 - 削除後再作成'
    ELSE 'テーブル未存在 - 新規作成'
  END as action_required;

-- 既存テーブルがある場合は削除（1日ずれバグ修正のため完全再構築）
DROP TABLE IF EXISTS `kabu-376213.kabu2411.D010_basic_results`;

SELECT 
  '✅ Step 1完了: 既存テーブル削除完了' as status,
  '次: Step 2（新テーブル作成）' as next_action;

-- ============================================================================
-- Step 2: D010_basic_results テーブル新規作成
-- ============================================================================

-- 新設計書準拠でテーブル作成
CREATE TABLE `kabu-376213.kabu2411.D010_basic_results` (
  signal_date DATE NOT NULL,
  
  -- 4軸情報（修正不要部分）
  signal_type STRING NOT NULL,           -- 4軸① シグナル種別
  signal_bin INT64 NOT NULL,             -- 4軸② シグナル分位（1-20）
  trade_type STRING NOT NULL,            -- 4軸③ 取引種別（'BUY'/'SELL'）
  stock_code STRING NOT NULL,            -- 4軸④ 銘柄コード
  stock_name STRING,                     -- 銘柄名（冗長データ・JOIN回避）
  signal_value FLOAT64,                  -- シグナル値（修正不要：前日データで正しい）
  
  -- 価格データ（修正対象：signal_date当日データに修正予定）
  prev_close FLOAT64,                    -- 前日終値（signal_dateの前日）
  day_open FLOAT64,                      -- 始値（signal_date当日）
  day_high FLOAT64,                      -- 高値（signal_date当日）
  day_low FLOAT64,                       -- 安値（signal_date当日）
  day_close FLOAT64,                     -- 終値（signal_date当日）
  
  -- 計算値（修正対象：上記価格データ修正後に再計算）
  prev_close_to_open_gap FLOAT64,       -- 前日終値→始値ギャップ
  open_to_high_gap FLOAT64,             -- 始値→高値ギャップ
  open_to_low_gap FLOAT64,              -- 始値→安値ギャップ
  open_to_close_gap FLOAT64,            -- 始値→終値ギャップ（当日損益）
  daily_range FLOAT64,                  -- 日足値幅（高値-安値）
  
  -- 取引結果（修正対象：上記価格データ修正後に再計算）
  baseline_profit_rate FLOAT64,         -- 寄引損益率（BUY：終値-始値、SELL：始値-終値）
  is_win BOOLEAN,                       -- 勝敗フラグ
  trading_volume FLOAT64,               -- 売買代金（signal_date当日）
  
  -- システム項目（修正不要）
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY signal_date
CLUSTER BY stock_code, signal_type;

SELECT 
  '✅ Step 2完了: D010_basic_results テーブル作成完了' as status,
  '構造: 4軸情報 + 価格データ + 取引結果' as table_structure,
  'パーティション: signal_date' as partition_info,
  'クラスタ: stock_code, signal_type' as cluster_info,
  '次: データ投入SQL実行（日付ロジック修正版）' as next_action;

-- ============================================================================
-- Step 3: データソース依存テーブル確認
-- ============================================================================

-- M010_signal_bins 確認（境界値マスタ）
SELECT 
  'Step 3: データソース確認' as check_step,
  '1. M010_signal_bins（境界値マスタ）' as check_target,
  (SELECT COUNT(*) FROM `kabu-376213.kabu2411.M010_signal_bins`) as M010_records_expected_740,
  (SELECT COUNT(DISTINCT signal_type) FROM `kabu-376213.kabu2411.M010_signal_bins`) as signal_types_expected_37;

-- daily_quotes 確認（株価データ）
SELECT 
  'Step 3: データソース確認' as check_step,
  '2. daily_quotes（株価データ）' as check_target,
  (SELECT COUNT(*) FROM `kabu-376213.kabu2411.daily_quotes`) as daily_quotes_records,
  (SELECT MIN(Date) FROM `kabu-376213.kabu2411.daily_quotes`) as quotes_min_date,
  (SELECT MAX(Date) FROM `kabu-376213.kabu2411.daily_quotes`) as quotes_max_date_should_be_2025_07_03;

-- master_trading_stocks 確認（銘柄マスタ）
SELECT 
  'Step 3: データソース確認' as check_step,
  '3. master_trading_stocks（銘柄マスタ）' as check_target,
  (SELECT COUNT(*) FROM `kabu-376213.kabu2411.master_trading_stocks`) as trading_stocks_records;

-- trading_calendar 確認（取引カレンダー）
SELECT 
  'Step 3: データソース確認' as check_step,
  '4. trading_calendar（取引カレンダー）' as check_target,
  (SELECT COUNT(*) FROM `kabu-376213.kabu2411.trading_calendar` WHERE HolidayDivision = '1') as trading_days_count;

-- ============================================================================
-- Step 4: 作成完了確認
-- ============================================================================

SELECT 
  '🎉 D010_basic_results テーブル作成完了' as final_status,
  '✅ テーブル構造: 設計書完全準拠' as achievement1,
  '✅ パーティション・クラスタ設定完了' as achievement2,
  '✅ データソース依存関係確認完了' as achievement3,
  '🔧 1日ずれバグ対策: データ投入SQLで修正' as bug_fix_plan,
  '次Phase: 02_insert_learning_period_data.sql実行' as next_phase,
  CURRENT_TIMESTAMP() as completion_time;

-- ============================================================================
-- 補足情報: 1日ずれバグ修正計画
-- ============================================================================

SELECT 
  '📋 1日ずれバグ修正計画' as info_type,
  '修正不要: signal_date, signal_value（シグナル計算部分）' as no_fix_needed,
  '修正対象: prev_close, day_*, trading_volume（株価データ部分）' as fix_required,
  '修正方法: データ投入SQLで日付ロジック変更' as fix_method,
  '影響範囲: 3,731万レコード全て' as impact_scale,
  'テスト戦略: 段階的実行（学習期間→検証期間）' as test_strategy;