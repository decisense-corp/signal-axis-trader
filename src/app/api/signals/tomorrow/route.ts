// src/app/api/signals/tomorrow/route.ts (銘柄×売買方向グループ化版)
// 🎯 銘柄×売買方向でグループ化して出力
import { NextRequest, NextResponse } from 'next/server';
import { BigQueryClient } from '@/lib/bigquery';

interface TomorrowSignalCandidate {
  stock_code: string;
  stock_name: string;
  trade_type: 'LONG' | 'SHORT';  // 後で 'BUY' | 'SELL' に変更予定
  
  // 集約データ
  max_win_rate: number;
  max_expected_value: number;
  excellent_pattern_count: number;
  total_samples: number;
  avg_win_rate: number;
  avg_expected_return: number;
  
  // 参考情報
  signal_patterns: number;  // その銘柄×方向の4軸パターン数
}

export async function GET(request: NextRequest) {
  try {
    console.log('🎯 明日のシグナル処理開始（銘柄×売買方向グループ化版）...');
    
    const bigquery = new BigQueryClient();
    
    // URLパラメータから設定を取得
    const { searchParams } = new URL(request.url);
    const limit = parseInt(searchParams.get('limit') || '50');
    const offset = parseInt(searchParams.get('offset') || '0');
    const orderBy = searchParams.get('orderBy') || 'max_win_rate';
    const orderDir = searchParams.get('orderDir') || 'DESC';
    const tradeType = searchParams.get('tradeType'); // 'LONG' | 'SHORT' | null
    const minWinRate = parseFloat(searchParams.get('minWinRate') || '55');

    // 🔥 銘柄×売買方向でグループ化したクエリ
    const query = `
      WITH todays_signals AS (
        -- 今日発生したシグナル（d15の最新日）
        SELECT 
          signal_date,
          stock_code,
          stock_name,
          signal_type,
          signal_bin,
          signal_value
        FROM \`kabu-376213.kabu2411.d15_signals_with_bins\`
        WHERE signal_date = (
          SELECT MAX(signal_date) 
          FROM \`kabu-376213.kabu2411.d15_signals_with_bins\`
        )
      ),
      axis_combinations AS (
        -- 今日発火した4軸組み合わせを生成（LONG/SHORT両方）
        SELECT 
          ts.signal_date,
          ts.stock_code,
          ts.stock_name,
          ts.signal_type,
          ts.signal_bin,
          ts.signal_value,
          tt.trade_type
        FROM todays_signals ts
        CROSS JOIN (
          SELECT 'LONG' as trade_type
          UNION ALL
          SELECT 'SHORT' as trade_type
        ) tt
      ),
      individual_patterns AS (
        -- 個別4軸パターンの統計
        SELECT 
          ac.stock_code,
          ac.stock_name,
          ac.trade_type,
          ac.signal_type,
          ac.signal_bin,
          d40.learning_win_rate,
          d40.learning_avg_profit,
          d40.learning_total_signals,
          d40.learning_sharpe_ratio,
          d40.is_excellent_pattern
        FROM axis_combinations ac
        INNER JOIN \`kabu-376213.kabu2411.d40_axis_performance_stats\` d40
          ON ac.signal_type = d40.signal_type
          AND ac.signal_bin = d40.signal_bin
          AND ac.stock_code = d40.stock_code
          AND ac.trade_type = d40.trade_type
        LEFT JOIN \`kabu-376213.kabu2411.u10_user_decisions\` u10
          ON d40.signal_type = u10.signal_type
          AND d40.signal_bin = u10.signal_bin
          AND d40.stock_code = u10.stock_code
          AND d40.trade_type = u10.trade_type
        WHERE 1=1
          -- 学習期間で優秀判定
          AND COALESCE(d40.is_excellent_pattern, false) = true
          AND COALESCE(d40.learning_win_rate, 0) >= 55
          AND COALESCE(d40.learning_avg_profit, 0) >= 0.5
          AND COALESCE(d40.learning_total_signals, 0) >= 20
          -- 未設定フィルタ
          AND u10.decision_id IS NULL
          -- 追加フィルタ
          ${minWinRate > 0 ? `AND COALESCE(d40.learning_win_rate, 0) >= ${minWinRate}` : ''}
          ${tradeType ? `AND ac.trade_type = '${tradeType}'` : ''}
      )
      SELECT 
        stock_code,
        stock_name,
        trade_type,
        
        -- 集約統計
        MAX(learning_win_rate) as max_win_rate,
        MAX(learning_avg_profit) as max_expected_value,
        COUNT(CASE WHEN is_excellent_pattern = true THEN 1 END) as excellent_pattern_count,
        SUM(learning_total_signals) as total_samples,
        ROUND(AVG(learning_win_rate), 1) as avg_win_rate,
        ROUND(AVG(learning_avg_profit), 4) as avg_expected_return,
        
        -- 参考情報
        COUNT(*) as signal_patterns
        
      FROM individual_patterns
      GROUP BY stock_code, stock_name, trade_type
      ORDER BY 
        CASE 
          WHEN '${orderBy}' = 'max_win_rate' THEN MAX(learning_win_rate)
          WHEN '${orderBy}' = 'max_expected_value' THEN MAX(learning_avg_profit)
          WHEN '${orderBy}' = 'excellent_pattern_count' THEN COUNT(CASE WHEN is_excellent_pattern = true THEN 1 END)
          WHEN '${orderBy}' = 'signal_patterns' THEN COUNT(*)
          ELSE MAX(learning_win_rate)
        END ${orderDir}
      LIMIT ${limit} OFFSET ${offset}
    `;

    console.log('⚡ 銘柄×売買方向でグループ化して集約中...');
    const results = await bigquery.query(query);
    
    // 型変換とフォーマット
    const candidates: TomorrowSignalCandidate[] = results.map((row: any) => ({
      stock_code: row.stock_code,
      stock_name: row.stock_name,
      trade_type: row.trade_type as 'LONG' | 'SHORT',
      max_win_rate: row.max_win_rate || 0,
      max_expected_value: row.max_expected_value || 0,
      excellent_pattern_count: row.excellent_pattern_count || 0,
      total_samples: row.total_samples || 0,
      avg_win_rate: row.avg_win_rate || 0,
      avg_expected_return: row.avg_expected_return || 0,
      signal_patterns: row.signal_patterns || 0,
    }));

    // 🔥 総件数取得（同じ条件で）
    const countQuery = `
      WITH todays_signals AS (
        SELECT 
          signal_date,
          stock_code,
          stock_name,
          signal_type,
          signal_bin
        FROM \`kabu-376213.kabu2411.d15_signals_with_bins\`
        WHERE signal_date = (
          SELECT MAX(signal_date) 
          FROM \`kabu-376213.kabu2411.d15_signals_with_bins\`
        )
      ),
      axis_combinations AS (
        SELECT 
          ts.stock_code,
          ts.stock_name,
          ts.signal_type,
          ts.signal_bin,
          tt.trade_type
        FROM todays_signals ts
        CROSS JOIN (
          SELECT 'LONG' as trade_type
          UNION ALL
          SELECT 'SHORT' as trade_type
        ) tt
      ),
      individual_patterns AS (
        SELECT 
          ac.stock_code,
          ac.trade_type,
          d40.learning_win_rate,
          d40.is_excellent_pattern
        FROM axis_combinations ac
        INNER JOIN \`kabu-376213.kabu2411.d40_axis_performance_stats\` d40
          ON ac.signal_type = d40.signal_type
          AND ac.signal_bin = d40.signal_bin
          AND ac.stock_code = d40.stock_code
          AND ac.trade_type = d40.trade_type
        LEFT JOIN \`kabu-376213.kabu2411.u10_user_decisions\` u10
          ON d40.signal_type = u10.signal_type
          AND d40.signal_bin = u10.signal_bin
          AND d40.stock_code = u10.stock_code
          AND d40.trade_type = u10.trade_type
        WHERE 1=1
          AND COALESCE(d40.is_excellent_pattern, false) = true
          AND COALESCE(d40.learning_win_rate, 0) >= 55
          AND COALESCE(d40.learning_avg_profit, 0) >= 0.5
          AND COALESCE(d40.learning_total_signals, 0) >= 20
          AND u10.decision_id IS NULL
          ${minWinRate > 0 ? `AND COALESCE(d40.learning_win_rate, 0) >= ${minWinRate}` : ''}
          ${tradeType ? `AND ac.trade_type = '${tradeType}'` : ''}
      )
      SELECT COUNT(DISTINCT CONCAT(stock_code, '-', trade_type)) as total_count
      FROM individual_patterns
    `;
    
    const countResult = await bigquery.query(countQuery);
    const totalCount = countResult[0]?.total_count || 0;

    // 🔥 最新シグナル日も取得
    const latestDateQuery = `
      SELECT MAX(signal_date) as latest_signal_date
      FROM \`kabu-376213.kabu2411.d15_signals_with_bins\`
    `;
    const latestDateResult = await bigquery.query(latestDateQuery);
    const latestSignalDate = latestDateResult[0]?.latest_signal_date;

    // 🔥 全体統計も取得（参考情報）
    const statsQuery = `
      WITH todays_signals AS (
        SELECT signal_type, signal_bin, stock_code
        FROM \`kabu-376213.kabu2411.d15_signals_with_bins\`
        WHERE signal_date = (
          SELECT MAX(signal_date) 
          FROM \`kabu-376213.kabu2411.d15_signals_with_bins\`
        )
      )
      SELECT 
        COUNT(*) * 2 as total_todays_signals, -- LONG/SHORT両方
        (
          SELECT COUNT(*) 
          FROM \`kabu-376213.kabu2411.d40_axis_performance_stats\` 
          WHERE is_excellent_pattern = true
        ) as total_excellent_patterns,
        (
          SELECT COUNT(*) 
          FROM \`kabu-376213.kabu2411.u10_user_decisions\`
        ) as total_configured_decisions
      FROM todays_signals
    `;
    
    const statsResult = await bigquery.query(statsQuery);
    const stats = statsResult[0] || {};

    console.log(`✅ 銘柄×売買方向グループ化完了: ${candidates.length}件取得`);
    console.log(`📊 統計: 今日発火${stats.total_todays_signals}件 → グループ化後${totalCount}件`);

    return NextResponse.json({
      success: true,
      data: candidates,
      pagination: {
        total: totalCount,
        limit,
        offset,
        hasMore: offset + limit < totalCount
      },
      metadata: {
        query_time: new Date().toISOString(),
        latest_signal_date: latestSignalDate,
        source_tables: 'd40_axis_performance_stats(メイン) + d15_signals_with_bins(今日発火) + u10_user_decisions(未設定)',
        join_strategy: 'INNER JOIN d40 + INNER JOIN d15 + LEFT JOIN u10(未設定フィルタ)',
        optimization: '今日発火 × 学習期間優秀 × 未設定 → 銘柄×売買方向グループ化',
        description: '銘柄×売買方向でグループ化した明日の取引候補',
        aggregation: 'GROUP BY stock_code, trade_type',
        filters_applied: {
          excellent_pattern: true,
          min_win_rate: 55,
          min_profit_rate: 0.5,
          min_samples: 20,
          unconfigured_only: true
        },
        statistics: {
          todays_total_signals: stats.total_todays_signals,
          excellent_patterns_total: stats.total_excellent_patterns,
          configured_decisions_total: stats.total_configured_decisions,
          grouped_result: totalCount
        }
      }
    });

  } catch (error) {
    console.error('❌ 銘柄×売買方向グループ化エラー:', error);
    
    return NextResponse.json({
      success: false,
      error: 'データの取得に失敗しました',
      details: error instanceof Error ? error.message : '不明なエラー',
      phase: 'Phase 7: 明日のシグナルAPI修正（グループ化版）'
    }, { status: 500 });
  }
}