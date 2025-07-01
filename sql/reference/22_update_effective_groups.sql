/*
ファイル: 22_update_effective_groups.sql
説明: 有効な4軸グループを評価・更新（週次実行）
作成日: 2025-01-01
修正日: 2025-06-08 - カラム参照エラーを修正
実行時間: 約2分
*/

-- ============================================================================
-- 1. 処理開始メッセージ
-- ============================================================================
SELECT 
  '有効4軸グループの更新を開始します' as message,
  CURRENT_DATE('Asia/Tokyo') as evaluation_date,
  (SELECT COUNT(*) FROM `kabu-376213.kabu2411.d02_signal_performance_4axis`) as total_4axis_groups,
  (SELECT COUNT(*) FROM `kabu-376213.kabu2411.d03_effective_4axis_groups` WHERE is_effective = true) as current_effective_groups;

-- ============================================================================
-- 2. 既存データのクリア
-- ============================================================================
TRUNCATE TABLE `kabu-376213.kabu2411.d03_effective_4axis_groups`;

-- ============================================================================
-- 3. 有効グループの評価と格納（初期クエリの構造を参考に）
-- ============================================================================
INSERT INTO `kabu-376213.kabu2411.d03_effective_4axis_groups`
WITH group_evaluation AS (
  SELECT
    signal_type,
    signal_bin,
    trade_type,
    stock_code,
    stock_name,
    -- 基本統計
    total_count,
    win_rate,
    avg_profit_rate,
    std_profit_rate,
    sharpe_ratio,
    -- 期間別統計
    last_30d_count,
    last_30d_win_rate,
    last_30d_avg_profit,
    last_90d_count,
    last_90d_win_rate,
    last_90d_avg_profit,
    -- 有効性判定（複数の基準を考慮）
    CASE
      -- 基本条件: サンプル数30以上、勝率50%以上、平均利益率0.1%以上
      WHEN total_count >= 30 
       AND win_rate >= 50 
       AND avg_profit_rate >= 0.1
       AND sharpe_ratio >= 0.1  -- リスク調整後リターンも考慮
      THEN 
        -- 追加条件: 直近実績も確認
        CASE
          -- 直近30日の実績が悪化していない
          WHEN last_30d_count >= 10 
           AND last_30d_avg_profit < avg_profit_rate * 0.5  -- 50%以上悪化
          THEN false
          -- 90日間取引がない（非アクティブ）
          WHEN last_90d_count = 0
          THEN false
          -- その他は有効
          ELSE true
        END
      ELSE false
    END as is_effective,
    -- 有効性の理由
    CASE
      WHEN total_count < 30 THEN CONCAT('サンプル不足（', CAST(total_count AS STRING), '件）')
      WHEN win_rate < 50 THEN CONCAT('勝率不足（', CAST(ROUND(win_rate, 1) AS STRING), '%）')
      WHEN avg_profit_rate < 0.1 THEN CONCAT('利益率不足（', CAST(ROUND(avg_profit_rate, 2) AS STRING), '%）')
      WHEN sharpe_ratio < 0.1 THEN CONCAT('シャープレシオ不足（', CAST(ROUND(sharpe_ratio, 2) AS STRING), '）')
      WHEN last_30d_count >= 10 AND last_30d_avg_profit < avg_profit_rate * 0.5 
      THEN CONCAT('直近パフォーマンス悪化（30日: ', CAST(ROUND(last_30d_avg_profit, 2) AS STRING), '%）')
      WHEN last_90d_count = 0 THEN '90日間非アクティブ'
      ELSE '基準クリア'
    END as effectiveness_reason,
    -- スコア計算の要素
    -- 1. 信頼性スコア（サンプル数ベース、最大100）
    LEAST(total_count / 100, 1.0) * 100 as reliability_score,
    -- 2. 安定性スコア（シャープレシオベース、最大100）
    LEAST(GREATEST(sharpe_ratio, 0) / 1.0, 1.0) * 100 as stability_score,
    -- 3. 直近スコア（30日実績ベース、最大100）
    CASE
      WHEN last_30d_count >= 10 THEN
        LEAST(GREATEST(last_30d_avg_profit, 0) / 1.0, 1.0) * 100
      ELSE 
        -- データ不足時は全体実績を使用（割引）
        LEAST(GREATEST(avg_profit_rate, 0) / 1.0, 1.0) * 50
    END as recency_score
  FROM
    `kabu-376213.kabu2411.d02_signal_performance_4axis`
),
score_calculation AS (
  SELECT
    *,
    -- 総合スコア（加重平均）
    reliability_score * 0.2 +    -- 信頼性 20%
    stability_score * 0.3 +      -- 安定性 30%
    LEAST(GREATEST(avg_profit_rate, 0) / 1.0, 1.0) * 100 * 0.3 +  -- 収益性 30%
    recency_score * 0.2          -- 直近実績 20%
    as composite_score
  FROM
    group_evaluation
)
SELECT
  signal_type,
  signal_bin,
  trade_type,
  stock_code,
  is_effective,
  effectiveness_reason,
  total_count,
  win_rate,
  avg_profit_rate,
  sharpe_ratio,
  ROUND(reliability_score, 1) as reliability_score,
  ROUND(stability_score, 1) as stability_score,
  ROUND(recency_score, 1) as recency_score,
  ROUND(composite_score, 1) as composite_score,
  CURRENT_DATE('Asia/Tokyo') as evaluation_date,
  CURRENT_TIMESTAMP() as updated_at
FROM
  score_calculation;

-- ============================================================================
-- 4. カテゴリ別の統計情報を集計
-- ============================================================================
WITH category_stats AS (
  SELECT
    st.signal_category,
    eg.trade_type,
    COUNT(*) as total_groups,
    COUNTIF(eg.is_effective) as effective_groups,
    ROUND(COUNTIF(eg.is_effective) / COUNT(*) * 100, 1) as effective_rate,
    -- 有効グループの平均実績（d02からstock_nameを取得）
    ROUND(AVG(CASE WHEN eg.is_effective THEN eg.win_rate END), 1) as avg_win_rate,
    ROUND(AVG(CASE WHEN eg.is_effective THEN eg.avg_profit_rate END), 2) as avg_profit_rate,
    ROUND(AVG(CASE WHEN eg.is_effective THEN eg.sharpe_ratio END), 2) as avg_sharpe_ratio,
    ROUND(AVG(CASE WHEN eg.is_effective THEN eg.composite_score END), 1) as avg_composite_score,
    -- 上位グループ数
    COUNTIF(eg.is_effective AND eg.composite_score >= 70) as high_score_groups,
    COUNTIF(eg.is_effective AND eg.avg_profit_rate >= 0.5) as high_profit_groups
  FROM
    `kabu-376213.kabu2411.d03_effective_4axis_groups` eg
  JOIN
    `kabu-376213.kabu2411.m01_signal_types` st
  ON
    eg.signal_type = st.signal_type
  GROUP BY
    st.signal_category, eg.trade_type
)
SELECT
  '=== カテゴリ別統計 ===' as section,
  signal_category,
  trade_type,
  total_groups,
  effective_groups,
  effective_rate as effective_pct,
  avg_win_rate as win_rate,
  avg_profit_rate as profit_rate,
  avg_sharpe_ratio as sharpe,
  avg_composite_score as score,
  high_score_groups,
  high_profit_groups
FROM
  category_stats
ORDER BY
  signal_category, trade_type;

-- ============================================================================
-- 5. 無効化された理由の集計
-- ============================================================================
WITH reason_summary AS (
  SELECT
    effectiveness_reason,
    COUNT(*) as group_count,
    ROUND(COUNT(*) / (SELECT COUNT(*) FROM `kabu-376213.kabu2411.d03_effective_4axis_groups`) * 100, 1) as percentage,
    ROUND(AVG(total_count), 0) as avg_sample_count,
    ROUND(AVG(win_rate), 1) as avg_win_rate,
    ROUND(AVG(avg_profit_rate), 2) as avg_profit_rate
  FROM
    `kabu-376213.kabu2411.d03_effective_4axis_groups`
  GROUP BY
    effectiveness_reason
)
SELECT
  '=== 有効性判定理由の分布 ===' as section,
  effectiveness_reason,
  group_count,
  percentage as pct,
  avg_sample_count,
  avg_win_rate,
  avg_profit_rate
FROM
  reason_summary
ORDER BY
  group_count DESC;

-- ============================================================================
-- 6. トップパフォーマー（有効グループのTOP20）
-- ============================================================================
WITH top_performers AS (
  SELECT
    eg.signal_type,
    eg.trade_type,
    eg.stock_code,
    pf.stock_name,
    eg.total_count,
    eg.win_rate,
    eg.avg_profit_rate,
    eg.sharpe_ratio,
    eg.composite_score,
    pf.last_30d_count,
    pf.last_30d_avg_profit,
    RANK() OVER (PARTITION BY eg.trade_type ORDER BY eg.composite_score DESC) as score_rank,
    RANK() OVER (PARTITION BY eg.trade_type ORDER BY eg.avg_profit_rate DESC) as profit_rank
  FROM
    `kabu-376213.kabu2411.d03_effective_4axis_groups` eg
  LEFT JOIN 
    `kabu-376213.kabu2411.d02_signal_performance_4axis` pf
  ON
    eg.signal_type = pf.signal_type
    AND eg.signal_bin = pf.signal_bin
    AND eg.trade_type = pf.trade_type
    AND eg.stock_code = pf.stock_code
  WHERE
    eg.is_effective = true
    AND eg.total_count >= 50  -- より信頼性の高いグループ
)
SELECT
  '=== トップパフォーマー（Buy）===' as section,
  score_rank as rank,
  signal_type,
  stock_code,
  stock_name,
  ROUND(composite_score, 1) as score,
  ROUND(avg_profit_rate, 2) as profit_rate,
  ROUND(win_rate, 1) as win_rate,
  ROUND(sharpe_ratio, 2) as sharpe,
  total_count,
  last_30d_count as last_30d
FROM
  top_performers
WHERE
  trade_type = 'Buy'
  AND score_rank <= 10
UNION ALL
SELECT
  '=== トップパフォーマー（Sell）===' as section,
  score_rank as rank,
  signal_type,
  stock_code,
  stock_name,
  ROUND(composite_score, 1) as score,
  ROUND(avg_profit_rate, 2) as profit_rate,
  ROUND(win_rate, 1) as win_rate,
  ROUND(sharpe_ratio, 2) as sharpe,
  total_count,
  last_30d_count as last_30d
FROM
  top_performers
WHERE
  trade_type = 'Sell'
  AND score_rank <= 10
ORDER BY
  section, rank;

-- ============================================================================
-- 7. 更新結果のサマリー
-- ============================================================================
WITH update_summary AS (
  SELECT
    COUNT(*) as total_groups,
    COUNTIF(is_effective) as effective_groups,
    ROUND(COUNTIF(is_effective) / COUNT(*) * 100, 1) as effective_rate,
    -- 有効グループの内訳
    COUNTIF(is_effective AND trade_type = 'Buy') as effective_buy,
    COUNTIF(is_effective AND trade_type = 'Sell') as effective_sell,
    -- スコア分布
    COUNTIF(is_effective AND composite_score >= 70) as high_score_count,
    COUNTIF(is_effective AND composite_score >= 50 AND composite_score < 70) as medium_score_count,
    COUNTIF(is_effective AND composite_score < 50) as low_score_count,
    -- 銘柄カバレッジ
    COUNT(DISTINCT CASE WHEN is_effective THEN stock_code END) as effective_stocks,
    COUNT(DISTINCT stock_code) as total_stocks
  FROM
    `kabu-376213.kabu2411.d03_effective_4axis_groups`
)
SELECT
  '有効4軸グループの更新が完了しました' as message,
  total_groups,
  effective_groups,
  effective_rate as effective_pct,
  effective_buy,
  effective_sell,
  ROUND(effective_stocks / total_stocks * 100, 1) as stock_coverage_pct,
  high_score_count,
  medium_score_count,
  low_score_count,
  CURRENT_TIMESTAMP() as completed_at
FROM
  update_summary;

-- ============================================================================
-- 8. シグナルタイプ別の有効率
-- ============================================================================
SELECT
  signal_type,
  COUNT(*) as total_groups,
  COUNTIF(is_effective) as effective_groups,
  ROUND(COUNTIF(is_effective) / COUNT(*) * 100, 1) as effective_rate,
  ROUND(AVG(CASE WHEN is_effective THEN composite_score END), 1) as avg_score,
  ROUND(AVG(CASE WHEN is_effective THEN avg_profit_rate END), 2) as avg_profit
FROM
  `kabu-376213.kabu2411.d03_effective_4axis_groups`
GROUP BY
  signal_type
ORDER BY
  effective_rate DESC, avg_score DESC
LIMIT 15;