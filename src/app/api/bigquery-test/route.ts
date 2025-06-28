// src/app/api/bigquery-test/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { BigQueryClient } from '@/lib/bigquery';

export async function GET(request: NextRequest) {
  try {
    console.log('🔍 BigQuery接続テストを開始します...');

    // 環境変数の確認
    const envCheck = {
      GOOGLE_CLOUD_PROJECT_ID: process.env.GOOGLE_CLOUD_PROJECT_ID || 'Not set',
      GOOGLE_CLOUD_DATASET_ID: process.env.GOOGLE_CLOUD_DATASET_ID || 'Not set',
      GOOGLE_APPLICATION_CREDENTIALS: process.env.GOOGLE_APPLICATION_CREDENTIALS || 'Not set',
      NODE_ENV: process.env.NODE_ENV || 'Not set'
    };

    // BigQueryクライアントで接続テスト
    const bigquery = new BigQueryClient();
    const testResult = await bigquery.testConnectionSimple();
    
    // 接続情報も取得
    const connectionInfo = bigquery.getConnectionInfo();

    return NextResponse.json({
      success: true,
      message: 'BigQuery接続テスト完了',
      timestamp: new Date().toISOString(),
      environment: envCheck,
      connection: connectionInfo,
      testResult: testResult
    });

  } catch (error) {
    console.error('❌ テストエラー:', error);
    
    return NextResponse.json(
      {
        success: false,
        error: 'BigQuery接続テストに失敗しました',
        details: error instanceof Error ? error.message : '不明なエラー',
        timestamp: new Date().toISOString()
      },
      { status: 500 }
    );
  }
}