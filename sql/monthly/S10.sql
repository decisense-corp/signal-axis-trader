-- ============================================================================
-- ストアドプロシージャ: S10_optimize_single_metric
-- 作成日: 2025-01-17
-- 説明: 単一指標の逐次最適化（37ラウンド）
--       D10_trading_signalsテーブルに対応し、パーセント計算を内部で実装
--       期間指定可能、テーブル名をD81_/D82_プレフィックスに統一
-- パラメータ:
--   - target_metric: 最適化対象の指標（H3P, H1P, L3P, L1P, CU3P, CU1P, CD3P, CD1P, UD75P, DD75P, UC3P, DC3P, DIRECTION, VOL3P, VOL5P）
--   - target_trade_type: 売買種別（BUY/SELL）
--   - start_date: 集計開始日（デフォルト: 2022-01-01）
--   - end_date: 集計終了日（デフォルト: 2025-05-31）
-- ============================================================================

CREATE OR REPLACE PROCEDURE `kabu-376213.kabu2411.S10_optimize_single_metric`(
  target_metric STRING,
  target_trade_type STRING,
  start_date DATE,
  end_date DATE
)
BEGIN
  DECLARE optimization_round INT64 DEFAULT 1;
  DECLARE continue_flag BOOL DEFAULT TRUE;
  DECLARE best_signal_type STRING;
  DECLARE best_cv FLOAT64;
  DECLARE start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE round_start_time TIMESTAMP;
  DECLARE input_metric STRING DEFAULT target_metric;
  DECLARE input_trade_type STRING DEFAULT target_trade_type;
  DECLARE input_start_date DATE;
  DECLARE input_end_date DATE;
  
  -- パラメータのデフォルト値を設定
  SET input_start_date = IFNULL(start_date, DATE('2022-01-01'));
  SET input_end_date = IFNULL(end_date, DATE('2025-05-31'));
  
  -- 開始メッセージ
  SELECT 
    CONCAT('🚀 ', target_metric, ' (', target_trade_type, ') の最適化開始') as message,
    CONCAT('期間: ', input_start_date, ' ～ ', input_end_date) as period,
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
        
        -- パーセント計算を内部で実装
        SAFE_DIVIDE(d.open_to_high_gap, d.day_open) * 100 as open_to_high_percent,
        SAFE_DIVIDE(d.open_to_low_gap, d.day_open) * 100 as open_to_low_percent,
        SAFE_DIVIDE(d.open_to_close_gap, d.day_open) * 100 as open_to_close_percent,
        SAFE_DIVIDE(d.daily_range, d.day_open) * 100 as daily_range_percent,
        
        -- 実際のタッチ（指標に応じて条件を変更）
        CASE 
          -- 既存8指標
          WHEN target_metric = 'H3P' AND SAFE_DIVIDE(d.open_to_high_gap, d.day_open) * 100 >= 3.0 THEN 1.0
          WHEN target_metric = 'H1P' AND SAFE_DIVIDE(d.open_to_high_gap, d.day_open) * 100 >= 1.0 THEN 1.0
          WHEN target_metric = 'L3P' AND SAFE_DIVIDE(d.open_to_low_gap, d.day_open) * 100 <= -3.0 THEN 1.0
          WHEN target_metric = 'L1P' AND SAFE_DIVIDE(d.open_to_low_gap, d.day_open) * 100 <= -1.0 THEN 1.0
          WHEN target_metric = 'CU3P' AND SAFE_DIVIDE(d.open_to_close_gap, d.day_open) * 100 >= 3.0 THEN 1.0
          WHEN target_metric = 'CU1P' AND SAFE_DIVIDE(d.open_to_close_gap, d.day_open) * 100 >= 1.0 THEN 1.0
          WHEN target_metric = 'CD3P' AND SAFE_DIVIDE(d.open_to_close_gap, d.day_open) * 100 <= -3.0 THEN 1.0
          WHEN target_metric = 'CD1P' AND SAFE_DIVIDE(d.open_to_close_gap, d.day_open) * 100 <= -1.0 THEN 1.0
          -- 新4指標の目的変数
          WHEN target_metric = 'UD75P' AND 
            SAFE_DIVIDE(SAFE_DIVIDE(d.open_to_close_gap, d.day_open) * 100, NULLIF(SAFE_DIVIDE(d.daily_range, d.day_open) * 100, 0)) > 0.75 THEN 1.0
          WHEN target_metric = 'DD75P' AND 
            SAFE_DIVIDE(SAFE_DIVIDE(d.open_to_close_gap, d.day_open) * 100, NULLIF(SAFE_DIVIDE(d.daily_range, d.day_open) * 100, 0)) < -0.75 THEN 1.0
          WHEN target_metric = 'UC3P' AND 
            SAFE_DIVIDE(d.open_to_close_gap, d.day_open) * 100 >= 3.0 AND 
            ABS(SAFE_DIVIDE(d.open_to_low_gap, d.day_open) * 100) <= 0.5 THEN 1.0
          WHEN target_metric = 'DC3P' AND 
            SAFE_DIVIDE(d.open_to_close_gap, d.day_open) * 100 <= -3.0 AND 
            SAFE_DIVIDE(d.open_to_high_gap, d.day_open) * 100 <= 0.5 THEN 1.0
          -- 方向性の目的変数
          WHEN target_metric = 'DIRECTION' THEN
            CASE 
              WHEN target_trade_type = 'BUY' AND SAFE_DIVIDE(d.open_to_close_gap, d.day_open) * 100 > 0 THEN 1.0
              WHEN target_trade_type = 'SELL' AND SAFE_DIVIDE(d.open_to_close_gap, d.day_open) * 100 < 0 THEN 1.0
              ELSE 0.0
            END
          -- ボラティリティの目的変数
          WHEN target_metric = 'VOL3P' AND ABS(SAFE_DIVIDE(d.open_to_close_gap, d.day_open) * 100) >= 3.0 THEN 1.0
          WHEN target_metric = 'VOL5P' AND ABS(SAFE_DIVIDE(d.open_to_close_gap, d.day_open) * 100) >= 5.0 THEN 1.0
          ELSE 0.0
        END as actual_touch,
        
        -- 該当指標の係数
        CASE 
          -- 既存8指標係数
          WHEN target_metric = 'H3P' THEN c.coef_h3p
          WHEN target_metric = 'H1P' THEN c.coef_h1p
          WHEN target_metric = 'L3P' THEN c.coef_l3p
          WHEN target_metric = 'L1P' THEN c.coef_l1p
          WHEN target_metric = 'CU3P' THEN c.coef_cu3p
          WHEN target_metric = 'CU1P' THEN c.coef_cu1p
          WHEN target_metric = 'CD3P' THEN c.coef_cd3p
          WHEN target_metric = 'CD1P' THEN c.coef_cd1p
          -- 新4指標係数
          WHEN target_metric = 'UD75P' THEN c.coef_ud75p
          WHEN target_metric = 'DD75P' THEN c.coef_dd75p
          WHEN target_metric = 'UC3P' THEN c.coef_uc3p
          WHEN target_metric = 'DC3P' THEN c.coef_dc3p
          -- 方向性係数
          WHEN target_metric = 'DIRECTION' THEN c.coef_direction
          -- ボラティリティ係数
          WHEN target_metric = 'VOL3P' THEN c.coef_vol3p
          WHEN target_metric = 'VOL5P' THEN c.coef_vol5p
        END as target_coefficient
        
      FROM `kabu-376213.kabu2411.D10_trading_signals` d
      JOIN `kabu-376213.kabu2411.D81_signal_coefficients_8indicators` c
        ON d.signal_type = c.signal_type 
        AND d.signal_bin = c.signal_bin
        AND d.trade_type = c.trade_type
      WHERE d.signal_date BETWEEN input_start_date AND input_end_date  -- 期間指定
        AND d.trade_type = target_trade_type
    )
    SELECT 
      sc.*,
      -- 現在のスコア（37指標の係数の積）- 対数変換で計算
      EXP(SUM(LN(GREATEST(target_coefficient, 0.01))) OVER (
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
      -- 修正版：他の指標の影響を除去（除算に変更）
      AVG(
        CASE 
          WHEN cs.current_score > 0 AND cs.target_coefficient > 0 THEN
            cs.actual_touch * cs.target_coefficient / cs.current_score
          ELSE 
            cs.actual_touch  -- フォールバック
        END
      ) as corrected_touch_rate
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
      -- 処理済み指標を除外
      SELECT 1 
      FROM `kabu-376213.kabu2411.D82_optimization_history` oh
      WHERE oh.optimized_signal_type = ctr.signal_type
        AND oh.target_metric = input_metric
        AND oh.trade_type = input_trade_type
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
        -- 全体平均が1.0になるように正規化し、下限値0.01を設定
        GREATEST(
          SAFE_DIVIDE(
            ctr.corrected_touch_rate,
            AVG(ctr.corrected_touch_rate) OVER (PARTITION BY ctr.signal_type)
          ),
          0.01  -- 最小係数値を0.01に設定
        ) as new_coefficient
      FROM corrected_touch_rates ctr
      WHERE ctr.signal_type = best_signal_type;
      
      -- ステップ5: 係数テーブルを更新（全指標対応）
      IF target_metric = 'H3P' THEN
        UPDATE `kabu-376213.kabu2411.D81_signal_coefficients_8indicators` c
        SET coef_h3p = nc.new_coefficient, updated_at = CURRENT_TIMESTAMP()
        FROM new_coefficients nc
        WHERE c.signal_type = nc.signal_type AND c.signal_bin = nc.signal_bin AND c.trade_type = target_trade_type;
      ELSEIF target_metric = 'H1P' THEN
        UPDATE `kabu-376213.kabu2411.D81_signal_coefficients_8indicators` c
        SET coef_h1p = nc.new_coefficient, updated_at = CURRENT_TIMESTAMP()
        FROM new_coefficients nc
        WHERE c.signal_type = nc.signal_type AND c.signal_bin = nc.signal_bin AND c.trade_type = target_trade_type;
      ELSEIF target_metric = 'L3P' THEN
        UPDATE `kabu-376213.kabu2411.D81_signal_coefficients_8indicators` c
        SET coef_l3p = nc.new_coefficient, updated_at = CURRENT_TIMESTAMP()
        FROM new_coefficients nc
        WHERE c.signal_type = nc.signal_type AND c.signal_bin = nc.signal_bin AND c.trade_type = target_trade_type;
      ELSEIF target_metric = 'L1P' THEN
        UPDATE `kabu-376213.kabu2411.D81_signal_coefficients_8indicators` c
        SET coef_l1p = nc.new_coefficient, updated_at = CURRENT_TIMESTAMP()
        FROM new_coefficients nc
        WHERE c.signal_type = nc.signal_type AND c.signal_bin = nc.signal_bin AND c.trade_type = target_trade_type;
      ELSEIF target_metric = 'CU3P' THEN
        UPDATE `kabu-376213.kabu2411.D81_signal_coefficients_8indicators` c
        SET coef_cu3p = nc.new_coefficient, updated_at = CURRENT_TIMESTAMP()
        FROM new_coefficients nc
        WHERE c.signal_type = nc.signal_type AND c.signal_bin = nc.signal_bin AND c.trade_type = target_trade_type;
      ELSEIF target_metric = 'CU1P' THEN
        UPDATE `kabu-376213.kabu2411.D81_signal_coefficients_8indicators` c
        SET coef_cu1p = nc.new_coefficient, updated_at = CURRENT_TIMESTAMP()
        FROM new_coefficients nc
        WHERE c.signal_type = nc.signal_type AND c.signal_bin = nc.signal_bin AND c.trade_type = target_trade_type;
      ELSEIF target_metric = 'CD3P' THEN
        UPDATE `kabu-376213.kabu2411.D81_signal_coefficients_8indicators` c
        SET coef_cd3p = nc.new_coefficient, updated_at = CURRENT_TIMESTAMP()
        FROM new_coefficients nc
        WHERE c.signal_type = nc.signal_type AND c.signal_bin = nc.signal_bin AND c.trade_type = target_trade_type;
      ELSEIF target_metric = 'CD1P' THEN
        UPDATE `kabu-376213.kabu2411.D81_signal_coefficients_8indicators` c
        SET coef_cd1p = nc.new_coefficient, updated_at = CURRENT_TIMESTAMP()
        FROM new_coefficients nc
        WHERE c.signal_type = nc.signal_type AND c.signal_bin = nc.signal_bin AND c.trade_type = target_trade_type;
      ELSEIF target_metric = 'UD75P' THEN
        UPDATE `kabu-376213.kabu2411.D81_signal_coefficients_8indicators` c
        SET coef_ud75p = nc.new_coefficient, updated_at = CURRENT_TIMESTAMP()
        FROM new_coefficients nc
        WHERE c.signal_type = nc.signal_type AND c.signal_bin = nc.signal_bin AND c.trade_type = target_trade_type;
      ELSEIF target_metric = 'DD75P' THEN
        UPDATE `kabu-376213.kabu2411.D81_signal_coefficients_8indicators` c
        SET coef_dd75p = nc.new_coefficient, updated_at = CURRENT_TIMESTAMP()
        FROM new_coefficients nc
        WHERE c.signal_type = nc.signal_type AND c.signal_bin = nc.signal_bin AND c.trade_type = target_trade_type;
      ELSEIF target_metric = 'UC3P' THEN
        UPDATE `kabu-376213.kabu2411.D81_signal_coefficients_8indicators` c
        SET coef_uc3p = nc.new_coefficient, updated_at = CURRENT_TIMESTAMP()
        FROM new_coefficients nc
        WHERE c.signal_type = nc.signal_type AND c.signal_bin = nc.signal_bin AND c.trade_type = target_trade_type;
      ELSEIF target_metric = 'DC3P' THEN
        UPDATE `kabu-376213.kabu2411.D81_signal_coefficients_8indicators` c
        SET coef_dc3p = nc.new_coefficient, updated_at = CURRENT_TIMESTAMP()
        FROM new_coefficients nc
        WHERE c.signal_type = nc.signal_type AND c.signal_bin = nc.signal_bin AND c.trade_type = target_trade_type;
      ELSEIF target_metric = 'DIRECTION' THEN
        UPDATE `kabu-376213.kabu2411.D81_signal_coefficients_8indicators` c
        SET coef_direction = nc.new_coefficient, updated_at = CURRENT_TIMESTAMP()
        FROM new_coefficients nc
        WHERE c.signal_type = nc.signal_type AND c.signal_bin = nc.signal_bin AND c.trade_type = target_trade_type;
      -- ボラティリティ係数の更新
      ELSEIF target_metric = 'VOL3P' THEN
        UPDATE `kabu-376213.kabu2411.D81_signal_coefficients_8indicators` c
        SET coef_vol3p = nc.new_coefficient, updated_at = CURRENT_TIMESTAMP()
        FROM new_coefficients nc
        WHERE c.signal_type = nc.signal_type AND c.signal_bin = nc.signal_bin AND c.trade_type = target_trade_type;
      ELSEIF target_metric = 'VOL5P' THEN
        UPDATE `kabu-376213.kabu2411.D81_signal_coefficients_8indicators` c
        SET coef_vol5p = nc.new_coefficient, updated_at = CURRENT_TIMESTAMP()
        FROM new_coefficients nc
        WHERE c.signal_type = nc.signal_type AND c.signal_bin = nc.signal_bin AND c.trade_type = target_trade_type;
      END IF;
      
      -- ステップ6: 処理履歴に記録
      INSERT INTO `kabu-376213.kabu2411.D82_optimization_history`
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
  
  -- 最終結果サマリー
  WITH final_summary AS (
    SELECT 
      COUNT(*) as total_optimized_indicators,
      ROUND(AVG(coefficient_of_variation), 4) as avg_cv_score,
      ROUND(SUM(processing_time_seconds), 1) as total_processing_seconds
    FROM `kabu-376213.kabu2411.D82_optimization_history` oh
    WHERE oh.target_metric = input_metric
      AND oh.trade_type = input_trade_type
  )
  SELECT 
    CONCAT('🎉 ', input_metric, ' (', input_trade_type, ') 最適化完了') as status,
    total_optimized_indicators,
    avg_cv_score,
    total_processing_seconds,
    CONCAT(ROUND(total_processing_seconds / 60, 1), ' 分') as total_processing_time,
    CONCAT('期間: ', input_start_date, ' ～ ', input_end_date) as optimized_period
  FROM final_summary;
  
END;