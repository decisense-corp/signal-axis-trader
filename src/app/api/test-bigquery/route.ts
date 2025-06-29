// src/app/api/signals/tomorrow/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { BigQueryClient } from '@/lib/bigquery';

interface TomorrowSignalCandidate {
  stock_code: string;
  stock_name: string;
  trade_type: 'Buy' | 'Sell';
  max_win_rate: number;
  max_expected_return: number;
  excellent_pattern_count: number;
  processing_status: '未処理' | '済（対象あり）' | '済（対象なし）';
  total_samples: number;
  avg_win_rate: number;
  avg_expected_return: number;
}

export async function GET(request: NextRequest) {
  try {
    const bigquery = new BigQueryClient();
    
    // URLパラメータから設定を取得
    const { searchParams } = new URL(request.url);
    const limit = parseInt(searchParams.get('limit') || '50');
    const offset = parseInt(searchParams.get('offset') || '0');
    const orderBy = searchParams.get('orderBy') || 'max_win_rate';
    const orderDir = searchParams.get('orderDir') || 'DESC';

    // 明日のシグナル候補を取得するSQL
    const query = `
      WITH tomorrow_signals AS (
        -- 明日発生予定のシグナルを特定
        SELECT DISTINCT
          sr.stock_code,
          tsm.company_name as stock_name,
          CASE 
            WHEN sr.signal_value > 0 THEN 'Buy'
            ELSE 'Sell'
          END as trade_type
        FROM \`kabu-376213.kabu2411.d01_signals_raw\` sr
        INNER JOIN \`kabu-376213.kabu2411.master_trading_stocks\` tsm
          ON sr.stock_code = tsm.stock_code
        WHERE sr.signal_date = DATE_ADD(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 DAY)
          AND sr.signal_value IS NOT NULL
      ),
      
      performance_stats AS (
        -- 4軸パフォーマンス統計を集計
        SELECT
          perf.stock_code,
          perf.trade_type,
          MAX(perf.win_rate) as max_win_rate,
          MAX(perf.expected_return) as max_expected_return,
          AVG(perf.win_rate) as avg_win_rate,
          AVG(perf.expected_return) as avg_expected_return,
          SUM(perf.sample_count) as total_samples,
          COUNT(CASE 
            WHEN perf.win_rate >= 55.0 
              AND perf.expected_return >= 0.5 
              AND perf.sample_count >= 30 
            THEN 1 
          END) as excellent_pattern_count
        FROM \`kabu-376213.kabu2411.d02_signal_performance_4axis\` perf
        WHERE perf.sample_count >= 10  -- 最低限のサンプル数
          AND perf.calculation_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        GROUP BY perf.stock_code, perf.trade_type
      ),
      
      processing_status AS (
        -- 処理状況を確認（将来的に実装予定のテーブル）
        SELECT
          ts.stock_code,
          ts.trade_type,
          CASE 
            WHEN COUNT(*) > 0 THEN '済（対象あり）'
            ELSE '未処理'
          END as status
        FROM tomorrow_signals ts
        LEFT JOIN \`kabu-376213.kabu2411.t01_trading_conditions\` tc
          ON ts.stock_code = tc.stock_code 
          AND ts.trade_type = tc.trade_type
          AND tc.is_active = true
        GROUP BY ts.stock_code, ts.trade_type
      )
      
      SELECT
        ts.stock_code,
        ts.stock_name,
        ts.trade_type,
        COALESCE(ps.max_win_rate, 0) as max_win_rate,
        COALESCE(ps.max_expected_return, 0) as max_expected_return,
        COALESCE(ps.excellent_pattern_count, 0) as excellent_pattern_count,
        COALESCE(pst.status, '未処理') as processing_status,
        COALESCE(ps.total_samples, 0) as total_samples,
        COALESCE(ps.avg_win_rate, 0) as avg_win_rate,
        COALESCE(ps.avg_expected_return, 0) as avg_expected_return
      FROM tomorrow_signals ts
      LEFT JOIN performance_stats ps
        ON ts.stock_code = ps.stock_code 
        AND ts.trade_type = ps.trade_type
      LEFT JOIN processing_status pst
        ON ts.stock_code = pst.stock_code 
        AND ts.trade_type = pst.trade_type
      WHERE COALESCE(ps.excellent_pattern_count, 0) > 0  -- 優秀パターンがある銘柄のみ
      ORDER BY 
        CASE 
          WHEN '${orderBy}' = 'max_win_rate' THEN ps.max_win_rate
          WHEN '${orderBy}' = 'max_expected_return' THEN ps.max_expected_return
          WHEN '${orderBy}' = 'excellent_pattern_count' THEN ps.excellent_pattern_count
          ELSE ps.max_win_rate
        END ${orderDir}
      LIMIT ${limit} OFFSET ${offset}
    `;

    const results = await bigquery.query(query);
    
    // 型変換とフォーマット
    const candidates: TomorrowSignalCandidate[] = results.map((row: any) => ({
      stock_code: row.stock_code,
      stock_name: row.stock_name,
      trade_type: row.trade_type,
      max_win_rate: Math.round(row.max_win_rate * 10) / 10, // 小数点1桁
      max_expected_return: Math.round(row.max_expected_return * 10) / 10,
      excellent_pattern_count: row.excellent_pattern_count,
      processing_status: row.processing_status,
      total_samples: row.total_samples,
      avg_win_rate: Math.round(row.avg_win_rate * 10) / 10,
      avg_expected_return: Math.round(row.avg_expected_return * 10) / 10,
    }));

    // 総件数も取得（ページネーション用）
    const countQuery = `
      SELECT COUNT(*) as total_count
      FROM (
        ${query.replace(/ORDER BY.*LIMIT.*OFFSET.*/, '')}
      )
    `;
    
    const countResult = await bigquery.query(countQuery);
    const totalCount = countResult[0]?.total_count || 0;

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
        target_date: new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString().split('T')[0] // 明日の日付
      }
    });

  } catch (error) {
    console.error('明日のシグナル取得エラー:', error);
    
    return NextResponse.json({
      success: false,
      error: 'データの取得に失敗しました',
      details: error instanceof Error ? error.message : '不明なエラー'
    }, { status: 500 });
  }
}