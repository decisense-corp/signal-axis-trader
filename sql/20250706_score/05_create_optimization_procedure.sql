-- ============================================================================
-- ファイル名: 05_create_optimization_procedure.sql
-- 作成日: 2025-01-05
-- 説明: 逐次最適化のメインロジック - 37回のループで最も説明力の高い指標から順に係数を最適化
--       処理済み指標は自動的に除外され、重複なく全指標を最適化
-- ============================================================================

-- ============================================================================
-- メインプロシージャ：単一指標の最適化（H3P, H1P等を個別に処理）
-- ============================================================================
CREATE OR REPLACE PROCEDURE `kabu-376213.kabu2411.optimize_single_metric`(
  IN target_metric STRING,  -- 'H3P', 'H1P', 'L3P', 'L1P', 'CU3P', 'CU1P', 'CD3P', 'CD1P'
  IN target_trade_type STRING  -- 'BUY' or 'SELL'
)
BEGIN
  DECLARE optimization_round INT64 DEFAULT 1;
  DECLARE continue_flag BOOL DEFAULT TRUE;
  DECLARE best_signal_type STRING;
  DECLARE best_cv FLOAT64;
  DECLARE start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE round_start_time TIMESTAMP;
  DECLARE input_metric STRING DEFAULT target_metric;  -- 変数のコピーを作成
  DECLARE input_trade_type STRING DEFAULT target_trade_type;  -- 変数のコピーを作成
  
  -- 開始メッセージ
  SELECT 
    CONCAT('🚀 ', target_metric, ' (', target_trade_type, ') の最適化開始') as message,
    CURRENT_TIMESTAMP() as start_time;
  
  -- 37回の最適化ループ
  WHILE optimization_round <= 37 AND continue_flag DO
    SET round_start_time = CURRENT_TIMESTAMP();
    
    -- ステップ1: 現在の係数でスコアを計算し、補正後タッチ率を算出
    CREATE OR REPLACE TEMP TABLE current_scores AS
    WITH score_calculation AS (
      SELECT 
        d.signal_date,
        d.stock_code,
        d.signal_type,
        d.signal_bin,
        d.trade_type,
        
        -- 実際のタッチ（指標に応じて条件を変更）
        CASE 
          WHEN target_metric = 'H3P' AND d.open_to_high_percent >= 3.0 THEN 1.0
          WHEN target_metric = 'H1P' AND d.open_to_high_percent >= 1.0 THEN 1.0
          WHEN target_metric = 'L3P' AND d.open_to_low_percent <= -3.0 THEN 1.0
          WHEN target_metric = 'L1P' AND d.open_to_low_percent <= -1.0 THEN 1.0
          WHEN target_metric = 'CU3P' AND d.open_to_close_percent >= 3.0 THEN 1.0
          WHEN target_metric = 'CU1P' AND d.open_to_close_percent >= 1.0 THEN 1.0
          WHEN target_metric = 'CD3P' AND d.open_to_close_percent <= -3.0 THEN 1.0
          WHEN target_metric = 'CD1P' AND d.open_to_close_percent <= -1.0 THEN 1.0
          ELSE 0.0
        END as actual_touch,
        
        -- 該当指標の係数
        CASE 
          WHEN target_metric = 'H3P' THEN c.coef_h3p
          WHEN target_metric = 'H1P' THEN c.coef_h1p
          WHEN target_metric = 'L3P' THEN c.coef_l3p
          WHEN target_metric = 'L1P' THEN c.coef_l1p
          WHEN target_metric = 'CU3P' THEN c.coef_cu3p
          WHEN target_metric = 'CU1P' THEN c.coef_cu1p
          WHEN target_metric = 'CD3P' THEN c.coef_cd3p
          WHEN target_metric = 'CD1P' THEN c.coef_cd1p
        END as target_coefficient
        
      FROM `kabu-376213.kabu2411.D010_enhanced_analysis` d
      JOIN `kabu-376213.kabu2411.signal_coefficients_8indicators` c
        ON d.signal_type = c.signal_type 
        AND d.signal_bin = c.signal_bin
        AND d.trade_type = c.trade_type
      WHERE d.signal_date <= '2025-05-31'  -- 学習期間
        AND d.trade_type = target_trade_type
    )
    SELECT 
      sc.*,
      -- 現在のスコア（37指標の係数の積）- 対数変換で計算
      EXP(SUM(LN(GREATEST(target_coefficient, 0.0001))) OVER (
        PARTITION BY signal_date, stock_code
      )) as current_score
    FROM score_calculation sc;
    
    -- ステップ2: 各指標×binごとの補正後タッチ率を計算
    CREATE OR REPLACE TEMP TABLE corrected_touch_rates AS
    SELECT 
      cs.signal_type,
      cs.signal_bin,
      COUNT(*) as sample_count,
      AVG(cs.actual_touch) as raw_touch_rate,
      -- 他の指標の影響を除いた補正後タッチ率
      AVG(cs.actual_touch * cs.current_score / GREATEST(cs.target_coefficient, 0.0001)) as corrected_touch_rate
    FROM current_scores cs
    GROUP BY cs.signal_type, cs.signal_bin
    HAVING COUNT(*) >= 10;  -- 最低10サンプル
    
    -- ステップ3: 処理済み指標を除外して最も説明力の高い指標を特定
    CREATE OR REPLACE TEMP TABLE best_indicator AS
    SELECT 
      signal_type,
      AVG(corrected_touch_rate) as avg_corrected_rate,
      STDDEV(corrected_touch_rate) as std_corrected_rate,
      -- 変動係数（CV）= 標準偏差 / 平均
      SAFE_DIVIDE(STDDEV(corrected_touch_rate), AVG(corrected_touch_rate)) as cv,
      COUNT(DISTINCT signal_bin) as bins_with_data
    FROM corrected_touch_rates ctr
    WHERE NOT EXISTS (
      -- 処理済み指標を除外（同じ指標・同じ売買種別内でのみチェック）
      SELECT 1 
      FROM `kabu-376213.kabu2411.optimization_history` oh
      WHERE oh.optimized_signal_type = ctr.signal_type
        AND oh.target_metric = input_metric  -- input_metricを使用
        AND oh.trade_type = input_trade_type  -- input_trade_typeを使用
        AND oh.optimization_round < optimization_round  -- 現在のラウンドより前のみ
    )
    GROUP BY signal_type
    HAVING COUNT(DISTINCT signal_bin) >= 15  -- 最低15bin以上のデータ
    ORDER BY cv DESC
    LIMIT 1;
    
    -- 最良指標の情報を取得
    SET best_signal_type = (SELECT signal_type FROM best_indicator);
    SET best_cv = (SELECT cv FROM best_indicator);
    
    -- 最良指標が見つからない場合は終了
    IF best_signal_type IS NULL THEN
      SET continue_flag = FALSE;
    ELSE
      -- ステップ4: 選ばれた指標の20bin分の新係数を計算
      CREATE OR REPLACE TEMP TABLE new_coefficients AS
      SELECT 
        ctr.signal_type,
        ctr.signal_bin,
        ctr.corrected_touch_rate,
        ctr.raw_touch_rate,
        -- 全体平均が1.0になるように正規化
        SAFE_DIVIDE(
          ctr.corrected_touch_rate,
          AVG(ctr.corrected_touch_rate) OVER (PARTITION BY ctr.signal_type)
        ) as new_coefficient
      FROM corrected_touch_rates ctr
      WHERE ctr.signal_type = best_signal_type;
      
      -- ステップ5: 係数テーブルを更新
      -- H3Pの場合
      IF target_metric = 'H3P' THEN
        UPDATE `kabu-376213.kabu2411.signal_coefficients_8indicators` c
        SET 
          coef_h3p = nc.new_coefficient,
          updated_at = CURRENT_TIMESTAMP()
        FROM new_coefficients nc
        WHERE c.signal_type = nc.signal_type
          AND c.signal_bin = nc.signal_bin
          AND c.trade_type = target_trade_type;
      -- H1Pの場合
      ELSEIF target_metric = 'H1P' THEN
        UPDATE `kabu-376213.kabu2411.signal_coefficients_8indicators` c
        SET 
          coef_h1p = nc.new_coefficient,
          updated_at = CURRENT_TIMESTAMP()
        FROM new_coefficients nc
        WHERE c.signal_type = nc.signal_type
          AND c.signal_bin = nc.signal_bin
          AND c.trade_type = target_trade_type;
      -- L3Pの場合
      ELSEIF target_metric = 'L3P' THEN
        UPDATE `kabu-376213.kabu2411.signal_coefficients_8indicators` c
        SET 
          coef_l3p = nc.new_coefficient,
          updated_at = CURRENT_TIMESTAMP()
        FROM new_coefficients nc
        WHERE c.signal_type = nc.signal_type
          AND c.signal_bin = nc.signal_bin
          AND c.trade_type = target_trade_type;
      -- L1Pの場合
      ELSEIF target_metric = 'L1P' THEN
        UPDATE `kabu-376213.kabu2411.signal_coefficients_8indicators` c
        SET 
          coef_l1p = nc.new_coefficient,
          updated_at = CURRENT_TIMESTAMP()
        FROM new_coefficients nc
        WHERE c.signal_type = nc.signal_type
          AND c.signal_bin = nc.signal_bin
          AND c.trade_type = target_trade_type;
      -- CU3Pの場合
      ELSEIF target_metric = 'CU3P' THEN
        UPDATE `kabu-376213.kabu2411.signal_coefficients_8indicators` c
        SET 
          coef_cu3p = nc.new_coefficient,
          updated_at = CURRENT_TIMESTAMP()
        FROM new_coefficients nc
        WHERE c.signal_type = nc.signal_type
          AND c.signal_bin = nc.signal_bin
          AND c.trade_type = target_trade_type;
      -- CU1Pの場合
      ELSEIF target_metric = 'CU1P' THEN
        UPDATE `kabu-376213.kabu2411.signal_coefficients_8indicators` c
        SET 
          coef_cu1p = nc.new_coefficient,
          updated_at = CURRENT_TIMESTAMP()
        FROM new_coefficients nc
        WHERE c.signal_type = nc.signal_type
          AND c.signal_bin = nc.signal_bin
          AND c.trade_type = target_trade_type;
      -- CD3Pの場合
      ELSEIF target_metric = 'CD3P' THEN
        UPDATE `kabu-376213.kabu2411.signal_coefficients_8indicators` c
        SET 
          coef_cd3p = nc.new_coefficient,
          updated_at = CURRENT_TIMESTAMP()
        FROM new_coefficients nc
        WHERE c.signal_type = nc.signal_type
          AND c.signal_bin = nc.signal_bin
          AND c.trade_type = target_trade_type;
      -- CD1Pの場合
      ELSEIF target_metric = 'CD1P' THEN
        UPDATE `kabu-376213.kabu2411.signal_coefficients_8indicators` c
        SET 
          coef_cd1p = nc.new_coefficient,
          updated_at = CURRENT_TIMESTAMP()
        FROM new_coefficients nc
        WHERE c.signal_type = nc.signal_type
          AND c.signal_bin = nc.signal_bin
          AND c.trade_type = target_trade_type;
      END IF;
      
      -- ステップ6: 処理履歴に記録
      INSERT INTO `kabu-376213.kabu2411.optimization_history`
      (optimization_round, target_metric, trade_type, optimized_signal_type, 
       coefficient_of_variation, bins_updated, avg_coefficient, 
       min_coefficient, max_coefficient, raw_avg_touch_rate, 
       corrected_avg_touch_rate, sample_count, processing_time_seconds)
      SELECT 
        optimization_round,
        target_metric,
        target_trade_type,
        best_signal_type,
        best_cv,
        COUNT(DISTINCT signal_bin),
        AVG(new_coefficient),
        MIN(new_coefficient),
        MAX(new_coefficient),
        AVG(raw_touch_rate),
        AVG(corrected_touch_rate),
        SUM(sample_count),
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), round_start_time, SECOND)
      FROM new_coefficients
      CROSS JOIN (SELECT COUNT(*) as sample_count FROM current_scores WHERE signal_type = best_signal_type);
      
      -- 進捗表示
      SELECT 
        CONCAT('✅ Round ', optimization_round, '/37 完了') as status,
        best_signal_type as optimized_signal,
        ROUND(best_cv, 4) as cv_score,
        CONCAT(ROUND(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), round_start_time, SECOND), 1), ' 秒') as processing_time;
      
    END IF;
    
    SET optimization_round = optimization_round + 1;
  END WHILE;
  
  -- 最終結果サマリー（修正版）
  WITH final_summary AS (
    SELECT 
      COUNT(*) as total_optimized_indicators,
      ROUND(AVG(coefficient_of_variation), 4) as avg_cv_score,
      ROUND(SUM(processing_time_seconds), 1) as total_processing_seconds
    FROM `kabu-376213.kabu2411.optimization_history` oh
    WHERE oh.target_metric = input_metric  -- コピーした変数を使用
      AND oh.trade_type = input_trade_type  -- コピーした変数を使用
  )
  SELECT 
    CONCAT('🎉 ', input_metric, ' (', input_trade_type, ') 最適化完了') as status,
    total_optimized_indicators,
    avg_cv_score,
    total_processing_seconds,
    CONCAT(ROUND(total_processing_seconds / 60, 1), ' 分') as total_processing_time
  FROM final_summary;
  
END;