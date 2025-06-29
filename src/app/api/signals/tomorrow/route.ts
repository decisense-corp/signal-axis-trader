// src/app/api/signals/tomorrow/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { BigQueryClient } from '@/lib/bigquery';

interface TomorrowSignalCandidate {
  stock_code: string;
  stock_name: string;
  trade_type: 'Buy' | 'Sell';
  max_win_rate: number;
  max_expected_value: number;
  excellent_pattern_count: number;
  processing_status: string;
}

export async function GET(request: NextRequest) {
  try {
    console.log('🔍 機能1: 明日のシグナル処理開始...');
    
    const bigquery = new BigQueryClient();
    
    // URLパラメータから設定を取得
    const { searchParams } = new URL(request.url);
    const limit = parseInt(searchParams.get('limit') || '50');
    const offset = parseInt(searchParams.get('offset') || '0');

    // Phase 3で作成済みの高品質データを直接活用
    const query = `
      SELECT
        stock_code,
        stock_name,
        trade_type,
        max_win_rate,
        max_avg_profit as max_expected_value,
        excellent_patterns as excellent_pattern_count,
        processing_status
      FROM \`kabu-376213.kabu2411.d60_stock_tradetype_summary\`
      WHERE processing_status = '未（対象あり）'  -- 未処理で対象ありのみ
        AND excellent_patterns > 0  -- 優秀パターンがある場合のみ
      ORDER BY max_win_rate DESC, max_avg_profit DESC
      LIMIT ${limit} OFFSET ${offset}
    `;

    console.log('📊 Phase 3高品質データ取得中...');
    const results = await bigquery.query(query);
    
    // 型変換とフォーマット
    const candidates: TomorrowSignalCandidate[] = results.map((row: any) => ({
      stock_code: row.stock_code,
      stock_name: row.stock_name,
      trade_type: row.trade_type,
      max_win_rate: Math.round(row.max_win_rate * 10) / 10,
      max_expected_value: Math.round(row.max_expected_value * 100) / 100,
      excellent_pattern_count: row.excellent_pattern_count,
      processing_status: row.processing_status
    }));

    // 総件数も取得
    const countQuery = `
      SELECT COUNT(*) as total_count
      FROM \`kabu-376213.kabu2411.d60_stock_tradetype_summary\`
      WHERE processing_status = '未（対象あり）'
        AND excellent_patterns > 0
    `;
    
    const countResult = await bigquery.query(countQuery);
    const totalCount = countResult[0]?.total_count || 0;

    console.log(`✅ Phase 3データ取得成功: ${candidates.length}件`);

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
        description: 'Phase 3完了：25,143件の高品質パターンから抽出',
        data_source: 'd60_stock_tradetype_summary',
        filter: '勝率75.59%、利益率0.93%の高品質データ'
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