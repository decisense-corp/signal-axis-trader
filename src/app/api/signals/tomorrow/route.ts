// src/app/api/signals/tomorrow/route.ts (劇的単純化版)
// 🔥 d60_stock_tradetype_summary使用で80行→15行に削減
import { NextRequest, NextResponse } from 'next/server';
import { BigQueryClient } from '@/lib/bigquery';

interface TomorrowSignalCandidate {
  stock_code: string;
  stock_name: string;
  trade_type: 'BUY' | 'SELL';
  max_win_rate: number;
  max_expected_value: number;
  excellent_pattern_count: number;
  processing_status: '済（対象あり）' | '済（対象なし）' | '未（対象あり）' | '未（対象なし）';
  total_samples: number;
  avg_win_rate: number;
  avg_expected_return: number;
}

export async function GET(request: NextRequest) {
  try {
    console.log('🚀 機能1: 明日のシグナル処理開始（劇的単純化版）...');
    
    const bigquery = new BigQueryClient();
    
    // URLパラメータから設定を取得
    const { searchParams } = new URL(request.url);
    const limit = parseInt(searchParams.get('limit') || '50');
    const offset = parseInt(searchParams.get('offset') || '0');
    const orderBy = searchParams.get('orderBy') || 'max_win_rate';
    const orderDir = searchParams.get('orderDir') || 'DESC';
    const tradeType = searchParams.get('tradeType'); // 'BUY' | 'SELL' | null
    const minWinRate = parseFloat(searchParams.get('minWinRate') || '0');
    const minExcellentPatterns = parseInt(searchParams.get('minExcellentPatterns') || '1');

    // 🔥 劇的単純化: 事前計算済みテーブルから直接取得（15行のみ）
    const query = `
      SELECT 
        stock_code,
        stock_name,
        trade_type,
        ROUND(COALESCE(max_win_rate, 0), 1) as max_win_rate,
        ROUND(COALESCE(max_avg_profit, 0), 4) as max_expected_value,
        excellent_patterns as excellent_pattern_count,
        processing_status,
        total_patterns as total_samples,
        ROUND(COALESCE(avg_win_rate, 0), 1) as avg_win_rate,
        ROUND(COALESCE(avg_avg_profit, 0), 4) as avg_expected_return
      FROM \`kabu-376213.kabu2411.d60_stock_tradetype_summary\`
      WHERE excellent_patterns >= ${minExcellentPatterns}
        AND COALESCE(max_win_rate, 0) >= ${minWinRate}
        ${tradeType ? `AND trade_type = '${tradeType}'` : ''}
      ORDER BY 
        CASE 
          WHEN '${orderBy}' = 'max_win_rate' THEN COALESCE(max_win_rate, 0)
          WHEN '${orderBy}' = 'max_avg_profit' THEN COALESCE(max_avg_profit, 0)
          WHEN '${orderBy}' = 'excellent_patterns' THEN excellent_patterns
          ELSE COALESCE(max_win_rate, 0)
        END ${orderDir}
      LIMIT ${limit} OFFSET ${offset}
    `;

    console.log('⚡ 事前計算済みテーブルから高速取得中...');
    const results = await bigquery.query(query);
    
    // 型変換とフォーマット
    const candidates: TomorrowSignalCandidate[] = results.map((row: any) => ({
      stock_code: row.stock_code,
      stock_name: row.stock_name,
      trade_type: row.trade_type as 'BUY' | 'SELL',
      max_win_rate: row.max_win_rate || 0,
      max_expected_value: row.max_expected_value || 0,
      excellent_pattern_count: row.excellent_pattern_count || 0,
      processing_status: row.processing_status as '済（対象あり）' | '済（対象なし）' | '未（対象あり）' | '未（対象なし）',
      total_samples: row.total_samples || 0,
      avg_win_rate: row.avg_win_rate || 0,
      avg_expected_return: row.avg_expected_return || 0,
    }));

    // 🔥 総件数も超高速取得
    const countQuery = `
      SELECT COUNT(*) as total_count
      FROM \`kabu-376213.kabu2411.d60_stock_tradetype_summary\`
      WHERE excellent_patterns >= ${minExcellentPatterns}
        AND COALESCE(max_win_rate, 0) >= ${minWinRate}
        ${tradeType ? `AND trade_type = '${tradeType}'` : ''}
    `;
    
    const countResult = await bigquery.query(countQuery);
    const totalCount = countResult[0]?.total_count || 0;

    console.log(`✅ 明日のシグナル ${candidates.length}件を超高速取得完了`);

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
        source_table: 'd60_stock_tradetype_summary',
        optimization: '事前計算済みテーブル使用で劇的高速化',
        description: '機能1: 明日発生するシグナルの条件設定対象（銘柄×売買方向）'
      }
    });

  } catch (error) {
    console.error('❌ 明日のシグナル取得エラー:', error);
    
    return NextResponse.json({
      success: false,
      error: 'データの取得に失敗しました',
      details: error instanceof Error ? error.message : '不明なエラー'
    }, { status: 500 });
  }
}