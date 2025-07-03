// src/app/api/signals/tomorrow/route.ts (完全一から書き直し版)
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
    console.log('🎯 明日のシグナル一覧API開始（完全書き直し版）...');
    
    const bigquery = new BigQueryClient();
    
    // URLパラメータの取得
    const { searchParams } = new URL(request.url);
    const page = parseInt(searchParams.get('page') || '1');
    const perPage = parseInt(searchParams.get('per_page') || '15');
    const sortBy = searchParams.get('sort_by') || 'win_rate';
    const sortDir = searchParams.get('sort_dir') || 'DESC';
    const minWinRate = searchParams.get('min_win_rate') ? parseFloat(searchParams.get('min_win_rate')!) : 55;
    const minExpectedValue = searchParams.get('min_expected_value') ? parseFloat(searchParams.get('min_expected_value')!) : 0.5;
    const minSampleCount = 20;
    const decisionStatus = searchParams.get('decision_status');
    
    const offset = (page - 1) * perPage;

    // ソート設定
    let sortColumn = 'win_rate';
    if (sortBy === 'win_rate') {
      sortColumn = 'win_rate';
    } else if (sortBy === 'expected_value') {
      sortColumn = 'expected_value';
    } else if (sortBy === 'sample_count') {
      sortColumn = 'sample_count';
    }

    // メインクエリ
    const mainQuery = `
      SELECT 
        signals.stock_code,
        signals.stock_name,
        signals.trade_type,
        signals.signal_type,
        signals.signal_bin,
        signals.sample_count,
        signals.win_rate,
        signals.expected_value,
        COALESCE(decisions.decision_status, 'pending') as decision_status
      FROM (
        SELECT 
          result.stock_code,
          result.stock_name,
          result.trade_type,
          result.signal_type,
          result.signal_bin,
          result.learning_total_signals as sample_count,
          ROUND(result.learning_win_rate, 1) as win_rate,
          ROUND(result.learning_avg_profit, 3) as expected_value
        FROM (
          SELECT 
            d10.stock_code,
            ms.company_name as stock_name,
            CASE WHEN d10.signal_value > 0 THEN 'BUY' ELSE 'SELL' END as trade_type,
            d10.signal_type,
            sb.signal_bin,
            d30.learning_total_signals,
            d30.learning_win_rate,
            d30.learning_avg_profit
          FROM \`kabu-376213.kabu2411.d10_simple_signals\` d10
          JOIN \`kabu-376213.kabu2411.master_trading_stocks\` ms
            ON d10.stock_code = ms.stock_code
          JOIN \`kabu-376213.kabu2411.m30_signal_bins\` sb
            ON d10.signal_type = sb.signal_type
            AND d10.signal_value > sb.lower_bound
            AND d10.signal_value <= sb.upper_bound
          JOIN \`kabu-376213.kabu2411.d30_learning_period_snapshot\` d30
            ON d10.stock_code = d30.stock_code
            AND d10.signal_type = d30.signal_type
            AND sb.signal_bin = d30.signal_bin
            AND CASE WHEN d10.signal_value > 0 THEN 'LONG' ELSE 'SHORT' END = d30.trade_type
          WHERE d10.signal_date = (
            SELECT MAX(signal_date) 
            FROM \`kabu-376213.kabu2411.d10_simple_signals\`
          )
          AND d30.learning_total_signals >= ${minSampleCount}
          AND d30.learning_win_rate >= ${minWinRate}
          AND d30.learning_avg_profit >= ${minExpectedValue}
        ) result
      ) signals
      LEFT JOIN \`kabu-376213.kabu2411.u10_user_decisions\` decisions
        ON signals.stock_code = decisions.stock_code
        AND signals.signal_type = decisions.signal_type
        AND signals.signal_bin = decisions.signal_bin
        AND signals.trade_type = CASE 
          WHEN decisions.trade_type = 'LONG' THEN 'BUY' 
          WHEN decisions.trade_type = 'SHORT' THEN 'SELL' 
          ELSE decisions.trade_type 
        END
      ${decisionStatus ? `WHERE decisions.decision_status = '${decisionStatus}'` : ''}
      ORDER BY ${sortColumn} ${sortDir}
      LIMIT ${perPage} OFFSET ${offset}
    `;

    // 件数取得クエリ
    const countQuery = `
      SELECT COUNT(*) as total
      FROM (
        SELECT 
          d10.stock_code,
          d10.signal_type,
          sb.signal_bin,
          CASE WHEN d10.signal_value > 0 THEN 'BUY' ELSE 'SELL' END as trade_type
        FROM \`kabu-376213.kabu2411.d10_simple_signals\` d10
        JOIN \`kabu-376213.kabu2411.master_trading_stocks\` ms
          ON d10.stock_code = ms.stock_code
        JOIN \`kabu-376213.kabu2411.m30_signal_bins\` sb
          ON d10.signal_type = sb.signal_type
          AND d10.signal_value > sb.lower_bound
          AND d10.signal_value <= sb.upper_bound
        JOIN \`kabu-376213.kabu2411.d30_learning_period_snapshot\` d30
          ON d10.stock_code = d30.stock_code
          AND d10.signal_type = d30.signal_type
          AND sb.signal_bin = d30.signal_bin
          AND CASE WHEN d10.signal_value > 0 THEN 'LONG' ELSE 'SHORT' END = d30.trade_type
        WHERE d10.signal_date = (
          SELECT MAX(signal_date) 
          FROM \`kabu-376213.kabu2411.d10_simple_signals\`
        )
        AND d30.learning_total_signals >= ${minSampleCount}
        AND d30.learning_win_rate >= ${minWinRate}
        AND d30.learning_avg_profit >= ${minExpectedValue}
      ) base_count
      ${decisionStatus ? `
        LEFT JOIN \`kabu-376213.kabu2411.u10_user_decisions\` decisions
          ON base_count.stock_code = decisions.stock_code
          AND base_count.signal_type = decisions.signal_type
          AND base_count.signal_bin = decisions.signal_bin
          AND base_count.trade_type = CASE 
            WHEN decisions.trade_type = 'LONG' THEN 'BUY' 
            WHEN decisions.trade_type = 'SHORT' THEN 'SELL' 
            ELSE decisions.trade_type 
          END
        WHERE decisions.decision_status = '${decisionStatus}'
      ` : ''}
    `;

    console.log('📊 完全書き直し版クエリ実行中...');
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

    console.log(`✅ 完全書き直し版完了: ${signals.length}件`);
    return NextResponse.json(response);

  } catch (error) {
    console.error('❌ 完全書き直し版APIエラー:', error);
    return NextResponse.json({
      success: false,
      data: [],
      pagination: { total: 0, page: 1, per_page: 15, total_pages: 0 },
      metadata: { query_time: new Date().toISOString(), target_date: '' },
      error: error instanceof Error ? error.message : '不明なエラー'
    }, { status: 500 });
  }
}