// src/app/api/signals/tomorrow/route.ts (Phase 7完成版)
// 🎯 d30_learning_period_snapshot + d15_signals_with_bins + u10_user_decisions
// 今日発火 × 学習期間優秀 × 未設定 = 真の明日シグナル
import { NextRequest, NextResponse } from 'next/server';
import { BigQueryClient } from '@/lib/bigquery';

interface TomorrowSignalCandidate {
  signal_date: string;
  stock_code: string;
  stock_name: string;
  signal_type: string;
  signal_bin: number;
  trade_type: 'LONG' | 'SHORT';
  signal_value: number;
  
  // 学習期間統計（メインデータ）
  learning_win_rate: number;
  learning_avg_profit: number;
  learning_samples: number;
  learning_sharpe_ratio: number;
  
  // 優秀パターン判定
  is_excellent_pattern: boolean;
  pattern_category: string;
}

export async function GET(request: NextRequest) {
  try {
    console.log('🎯 Phase 7: 真の明日のシグナル処理開始（d30+d15+u10実装）...');
    
    const bigquery = new BigQueryClient();
    
    // URLパラメータから設定を取得
    const { searchParams } = new URL(request.url);
    const limit = parseInt(searchParams.get('limit') || '50');
    const offset = parseInt(searchParams.get('offset') || '0');
    const orderBy = searchParams.get('orderBy') || 'learning_win_rate';
    const orderDir = searchParams.get('orderDir') || 'DESC';
    const tradeType = searchParams.get('tradeType'); // 'LONG' | 'SHORT' | null
    const minWinRate = parseFloat(searchParams.get('minWinRate') || '55');
    const excellentOnly = searchParams.get('excellentOnly') === 'true';

    // 🔥 Phase 7完成版：正しいJOIN構造による明日のシグナル取得
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
      )
      SELECT 
        ac.signal_date,
        ac.stock_code,
        ac.stock_name,
        ac.signal_type,
        ac.signal_bin,
        ac.trade_type,
        ROUND(ac.signal_value, 4) as signal_value,
        
        -- 学習期間統計（d30がメインデータソース）
        ROUND(COALESCE(d30.learning_win_rate, 0), 1) as learning_win_rate,
        ROUND(COALESCE(d30.learning_avg_profit, 0), 4) as learning_avg_profit,
        COALESCE(d30.learning_total_signals, 0) as learning_samples,
        ROUND(COALESCE(d30.learning_sharpe_ratio, 0), 3) as learning_sharpe_ratio,
        
        -- 優秀パターン判定
        COALESCE(d30.is_excellent_pattern, false) as is_excellent_pattern,
        COALESCE(d30.pattern_category, 'UNKNOWN') as pattern_category
        
      FROM axis_combinations ac
      INNER JOIN \`kabu-376213.kabu2411.d30_learning_period_snapshot\` d30
        ON ac.signal_type = d30.signal_type
        AND ac.signal_bin = d30.signal_bin
        AND ac.stock_code = d30.stock_code
        AND ac.trade_type = d30.trade_type
      LEFT JOIN \`kabu-376213.kabu2411.u10_user_decisions\` u10
        ON d30.signal_type = u10.signal_type
        AND d30.signal_bin = u10.signal_bin
        AND d30.stock_code = u10.stock_code
        AND d30.trade_type = u10.trade_type
      WHERE 1=1
        -- 学習期間で優秀判定
        AND COALESCE(d30.is_excellent_pattern, false) = true
        AND COALESCE(d30.learning_win_rate, 0) >= 55
        AND COALESCE(d30.learning_avg_profit, 0) >= 0.5
        AND COALESCE(d30.learning_total_signals, 0) >= 20
        -- 未設定フィルタ（u10に存在しない）
        AND u10.decision_id IS NULL
        -- 追加フィルタ
        ${excellentOnly ? '' : ''} -- 既に優秀パターンのみ
        ${minWinRate > 0 ? `AND COALESCE(d30.learning_win_rate, 0) >= ${minWinRate}` : ''}
        ${tradeType ? `AND ac.trade_type = '${tradeType}'` : ''}
      ORDER BY 
        CASE 
          WHEN '${orderBy}' = 'learning_win_rate' THEN COALESCE(d30.learning_win_rate, 0)
          WHEN '${orderBy}' = 'learning_avg_profit' THEN COALESCE(d30.learning_avg_profit, 0)
          WHEN '${orderBy}' = 'learning_sharpe_ratio' THEN COALESCE(d30.learning_sharpe_ratio, 0)
          WHEN '${orderBy}' = 'signal_value' THEN ac.signal_value
          ELSE COALESCE(d30.learning_win_rate, 0)
        END ${orderDir}
      LIMIT ${limit} OFFSET ${offset}
    `;

    console.log('⚡ d30(学習期間) × d15(今日発火) × u10(未設定) でJOIN実行中...');
    const results = await bigquery.query(query);
    
    // 型変換とフォーマット
    const candidates: TomorrowSignalCandidate[] = results.map((row: any) => ({
      signal_date: row.signal_date,
      stock_code: row.stock_code,
      stock_name: row.stock_name,
      signal_type: row.signal_type,
      signal_bin: row.signal_bin,
      trade_type: row.trade_type as 'LONG' | 'SHORT',
      signal_value: row.signal_value || 0,
      learning_win_rate: row.learning_win_rate || 0,
      learning_avg_profit: row.learning_avg_profit || 0,
      learning_samples: row.learning_samples || 0,
      learning_sharpe_ratio: row.learning_sharpe_ratio || 0,
      is_excellent_pattern: row.is_excellent_pattern || false,
      pattern_category: row.pattern_category || 'UNKNOWN',
    }));

    // 🔥 総件数取得（同じ条件で）
    const countQuery = `
      WITH todays_signals AS (
        SELECT 
          signal_date,
          stock_code,
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
          ts.signal_type,
          ts.signal_bin,
          ts.stock_code,
          tt.trade_type
        FROM todays_signals ts
        CROSS JOIN (
          SELECT 'LONG' as trade_type
          UNION ALL
          SELECT 'SHORT' as trade_type
        ) tt
      )
      SELECT COUNT(*) as total_count
      FROM axis_combinations ac
      INNER JOIN \`kabu-376213.kabu2411.d30_learning_period_snapshot\` d30
        ON ac.signal_type = d30.signal_type
        AND ac.signal_bin = d30.signal_bin
        AND ac.stock_code = d30.stock_code
        AND ac.trade_type = d30.trade_type
      LEFT JOIN \`kabu-376213.kabu2411.u10_user_decisions\` u10
        ON d30.signal_type = u10.signal_type
        AND d30.signal_bin = u10.signal_bin
        AND d30.stock_code = u10.stock_code
        AND d30.trade_type = u10.trade_type
      WHERE 1=1
        AND COALESCE(d30.is_excellent_pattern, false) = true
        AND COALESCE(d30.learning_win_rate, 0) >= 55
        AND COALESCE(d30.learning_avg_profit, 0) >= 0.5
        AND COALESCE(d30.learning_total_signals, 0) >= 20
        AND u10.decision_id IS NULL
        ${minWinRate > 0 ? `AND COALESCE(d30.learning_win_rate, 0) >= ${minWinRate}` : ''}
        ${tradeType ? `AND ac.trade_type = '${tradeType}'` : ''}
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
          FROM \`kabu-376213.kabu2411.d30_learning_period_snapshot\` 
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

    console.log(`✅ Phase 7完成: 真の明日のシグナル ${candidates.length}件取得完了`);
    console.log(`📊 統計: 今日発火${stats.total_todays_signals}件 → 優秀+未設定${totalCount}件`);

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
        source_tables: 'd30_learning_period_snapshot(メイン) + d15_signals_with_bins(今日発火) + u10_user_decisions(未設定)',
        join_strategy: 'INNER JOIN d30 + INNER JOIN d15 + LEFT JOIN u10(未設定フィルタ)',
        optimization: '今日発火 × 学習期間優秀 × 未設定 = 真の明日候補',
        description: 'Phase 7完成: 明日取引すべき真の候補（条件設定が必要な4軸のみ）',
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
          filtered_result: totalCount
        }
      }
    });

  } catch (error) {
    console.error('❌ Phase 7: 明日のシグナル取得エラー:', error);
    
    return NextResponse.json({
      success: false,
      error: 'データの取得に失敗しました',
      details: error instanceof Error ? error.message : '不明なエラー',
      phase: 'Phase 7: 明日のシグナルAPI修正'
    }, { status: 500 });
  }
}