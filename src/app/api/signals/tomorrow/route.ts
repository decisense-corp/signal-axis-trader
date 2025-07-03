// src/app/api/signals/tomorrow/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { BigQueryClient } from '@/lib/bigquery';

interface TomorrowSignal {
  stock_code: string;
  stock_name: string;
  trade_type: 'BUY' | 'SELL';
  signal_type: string;
  signal_bin: number;
  sample_count: number;
  win_rate: number;
  expected_value: number;
  decision_status: 'pending' | 'configured' | 'rejected';
}

interface TomorrowSignalsResponse {
  success: boolean;
  data: TomorrowSignal[];
  pagination: {
    total: number;
    page: number;
    per_page: number;
    total_pages: number;
  };
  metadata: {
    query_time: string;
    target_date: string;
  };
  error?: string;
}

export async function GET(request: NextRequest) {
  try {
    console.log('🎯 明日のシグナル一覧API開始...');
    
    const bigquery = new BigQueryClient();
    
    // URLパラメータの取得
    const { searchParams } = new URL(request.url);
    const page = parseInt(searchParams.get('page') || '1');
    const perPage = parseInt(searchParams.get('per_page') || '15');
    const sortBy = searchParams.get('sort_by') || 'win_rate';
    const sortDir = searchParams.get('sort_dir') || 'DESC';
    const minWinRate = searchParams.get('min_win_rate') ? parseFloat(searchParams.get('min_win_rate')!) : null;
    const minExpectedValue = searchParams.get('min_expected_value') ? parseFloat(searchParams.get('min_expected_value')!) : null;
    const decisionStatus = searchParams.get('decision_status');
    
    const offset = (page - 1) * perPage;

    // WHERE条件の構築（デフォルト絞り込み条件追加）
    const whereConditions: string[] = [
      'learning_total_signals >= 20',  // サンプル数20件以上
      'learning_win_rate >= 55',       // 勝率55%以上
      'learning_avg_profit >= 0.5'     // 期待値0.5%以上
    ];
    
    // ユーザー指定フィルターがある場合は上書き
    if (minWinRate !== null) {
      whereConditions[1] = `learning_win_rate >= ${minWinRate}`;
    }
    
    if (minExpectedValue !== null) {
      whereConditions[2] = `learning_avg_profit >= ${minExpectedValue}`;
    }
    
    if (decisionStatus) {
      whereConditions.push(`decision_status = '${decisionStatus}'`);
    }
    
    const whereClause = `WHERE ${whereConditions.join(' AND ')}`;

    // ソート条件の検証
    const validSortColumns = ['win_rate', 'expected_value', 'sample_count'];
    let finalSortBy = 'learning_win_rate';
    
    if (sortBy === 'win_rate') {
      finalSortBy = 'learning_win_rate';
    } else if (sortBy === 'expected_value') {
      finalSortBy = 'learning_avg_profit';
    } else if (sortBy === 'sample_count') {
      finalSortBy = 'learning_total_signals';
    }
    
    const finalSortDir = sortDir.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';

    // メインクエリ
    const signalTypeCondition = `CASE WHEN d10.signal_value > 0 THEN 'LONG' ELSE 'SHORT' END`;
    const outputTypeCondition = `CASE WHEN ts.trade_type = 'LONG' THEN 'BUY' WHEN ts.trade_type = 'SHORT' THEN 'SELL' ELSE ts.trade_type END`;

    const mainQuery = `
      WITH tomorrow_signals AS (
        SELECT DISTINCT
          d10.stock_code,
          ms.company_name as stock_name,
          d10.signal_type,
          sb.signal_bin,
          ${signalTypeCondition} as trade_type
        FROM \`kabu-376213.kabu2411.d10_simple_signals\` d10
        INNER JOIN \`kabu-376213.kabu2411.master_trading_stocks\` ms
          ON d10.stock_code = ms.stock_code
        INNER JOIN \`kabu-376213.kabu2411.m30_signal_bins\` sb
          ON d10.signal_type = sb.signal_type
          AND d10.signal_value > sb.lower_bound
          AND d10.signal_value <= sb.upper_bound
        WHERE d10.signal_date = (
          SELECT MAX(signal_date) 
          FROM \`kabu-376213.kabu2411.d10_simple_signals\`
        )
      ),
      signals_with_stats AS (
        SELECT 
          ts.stock_code,
          ts.stock_name,
          ${outputTypeCondition} as trade_type,
          ts.signal_type,
          ts.signal_bin,
          COALESCE(d40.learning_total_signals, 0) as learning_total_signals,
          COALESCE(d40.learning_win_rate, 0) as learning_win_rate,
          COALESCE(d40.learning_avg_profit, 0) as learning_avg_profit,
          COALESCE(u10.decision_status, 'pending') as decision_status
        FROM tomorrow_signals ts
        LEFT JOIN \`kabu-376213.kabu2411.m10_axis_combinations\` d40
          ON ts.signal_type = d40.signal_type
          AND ts.signal_bin = d40.signal_bin
          AND ts.trade_type = d40.trade_type
          AND ts.stock_code = d40.stock_code
        LEFT JOIN \`kabu-376213.kabu2411.u10_user_decisions\` u10
          ON ts.signal_type = u10.signal_type
          AND ts.signal_bin = u10.signal_bin
          AND ts.trade_type = u10.trade_type
          AND ts.stock_code = u10.stock_code
      )
      SELECT 
        stock_code,
        stock_name,
        trade_type,
        signal_type,
        signal_bin,
        learning_total_signals as sample_count,
        ROUND(learning_win_rate, 1) as win_rate,
        ROUND(learning_avg_profit, 2) as expected_value,
        decision_status
      FROM signals_with_stats
      ${whereClause}
      ORDER BY ${finalSortBy} ${finalSortDir}
      LIMIT ${perPage} OFFSET ${offset}
    `;

    // 件数取得クエリ
    const countQuery = `
      WITH tomorrow_signals AS (
        SELECT DISTINCT
          d10.stock_code,
          ms.company_name as stock_name,
          d10.signal_type,
          sb.signal_bin,
          ${signalTypeCondition} as trade_type
        FROM \`kabu-376213.kabu2411.d10_simple_signals\` d10
        INNER JOIN \`kabu-376213.kabu2411.master_trading_stocks\` ms
          ON d10.stock_code = ms.stock_code
        INNER JOIN \`kabu-376213.kabu2411.m30_signal_bins\` sb
          ON d10.signal_type = sb.signal_type
          AND d10.signal_value > sb.lower_bound
          AND d10.signal_value <= sb.upper_bound
        WHERE d10.signal_date = (
          SELECT MAX(signal_date) 
          FROM \`kabu-376213.kabu2411.d10_simple_signals\`
        )
      ),
      signals_with_stats AS (
        SELECT 
          ts.stock_code,
          ts.stock_name,
          ${outputTypeCondition} as trade_type,
          ts.signal_type,
          ts.signal_bin,
          COALESCE(d40.learning_total_signals, 0) as learning_total_signals,
          COALESCE(d40.learning_win_rate, 0) as learning_win_rate,
          COALESCE(d40.learning_avg_profit, 0) as learning_avg_profit,
          COALESCE(u10.decision_status, 'pending') as decision_status
        FROM tomorrow_signals ts
        LEFT JOIN \`kabu-376213.kabu2411.m10_axis_combinations\` d40
          ON ts.signal_type = d40.signal_type
          AND ts.signal_bin = d40.signal_bin
          AND ts.trade_type = d40.trade_type
          AND ts.stock_code = d40.stock_code
        LEFT JOIN \`kabu-376213.kabu2411.u10_user_decisions\` u10
          ON ts.signal_type = u10.signal_type
          AND ts.signal_bin = u10.signal_bin
          AND ts.trade_type = u10.trade_type
          AND ts.stock_code = u10.stock_code
      )
      SELECT COUNT(*) as total
      FROM signals_with_stats
      ${whereClause}
    `;

    console.log('📊 メインクエリ実行中...');
    const [signals, countResult] = await Promise.all([
      bigquery.query(mainQuery),
      bigquery.query(countQuery)
    ]);

    const total = countResult[0]?.total || 0;
    const totalPages = Math.ceil(total / perPage);

    // 対象日取得
    const targetDateQuery = `
      SELECT MAX(signal_date) as target_date
      FROM \`kabu-376213.kabu2411.d10_simple_signals\`
    `;
    
    const targetDateResult = await bigquery.query(targetDateQuery);
    const targetDate = targetDateResult[0]?.target_date || new Date().toISOString().split('T')[0];

    const response: TomorrowSignalsResponse = {
      success: true,
      data: signals.map(signal => ({
        stock_code: signal.stock_code,
        stock_name: signal.stock_name,
        trade_type: signal.trade_type,
        signal_type: signal.signal_type,
        signal_bin: signal.signal_bin,
        sample_count: signal.sample_count,
        win_rate: signal.win_rate,
        expected_value: signal.expected_value,
        decision_status: signal.decision_status
      })),
      pagination: {
        total,
        page,
        per_page: perPage,
        total_pages: totalPages
      },
      metadata: {
        query_time: new Date().toISOString(),
        target_date: targetDate
      }
    };

    console.log(`✅ 明日のシグナル一覧取得完了: ${signals.length}件`);
    return NextResponse.json(response);

  } catch (error) {
    console.error('❌ 明日のシグナル一覧APIエラー:', error);
    return NextResponse.json({
      success: false,
      data: [],
      pagination: { total: 0, page: 1, per_page: 15, total_pages: 0 },
      metadata: { query_time: new Date().toISOString(), target_date: '' },
      error: error instanceof Error ? error.message : '不明なエラー'
    }, { status: 500 });
  }
}