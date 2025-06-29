// src/app/api/test-bigquery/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { BigQueryClient } from '@/lib/bigquery';

export async function GET(request: NextRequest) {
  try {
    console.log('🔍 BigQuery接続テスト開始...');
    
    const bigquery = new BigQueryClient();
    
    // 超シンプルなテーブル一覧取得
    const query = `
      SELECT 
        table_id as table_name,
        ROUND(size_bytes / 1024 / 1024, 2) as size_mb,
        row_count,
        creation_time
      FROM \`kabu-376213.kabu2411.__TABLES__\`
      WHERE table_id NOT LIKE '%partition%'
      ORDER BY size_bytes DESC
      LIMIT 20
    `;
    
    console.log('📊 テーブル一覧取得中...');
    const results = await bigquery.query(query);
    
    console.log(`✅ 成功: ${results.length}個のテーブルを確認`);
    
    return NextResponse.json({
      success: true,
      message: `BigQuery接続成功: ${results.length}個のテーブルを確認`,
      timestamp: new Date().toISOString(),
      tables: results.map((row: any) => ({
        name: row.table_name,
        size_mb: row.size_mb,
        row_count: row.row_count,
        created: row.creation_time
      }))
    });
    
  } catch (error) {
    console.error('❌ BigQuery接続エラー:', error);
    
    return NextResponse.json({
      success: false,
      error: 'BigQuery接続テストに失敗しました',
      details: error instanceof Error ? error.message : '不明なエラー',
      timestamp: new Date().toISOString()
    }, { status: 500 });
  }
}