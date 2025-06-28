import { NextRequest, NextResponse } from 'next/server';

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

    // 認証ファイルの存在確認
    let credentialsFileExists = false;
    try {
      const fs = require('fs');
      const path = require('path');
      const credentialsPath = path.join(process.cwd(), 'service-account-key.json');
      credentialsFileExists = fs.existsSync(credentialsPath);
    } catch (error) {
      console.warn('認証ファイル確認エラー:', error);
    }

    // BigQuery接続テスト
    let bigQueryTestResult = null;
    try {
      const { BigQuery } = require('@google-cloud/bigquery');
      
      const bigquery = new BigQuery({
        projectId: envCheck.GOOGLE_CLOUD_PROJECT_ID,
        keyFilename: envCheck.GOOGLE_APPLICATION_CREDENTIALS,
      });

      // 簡単な接続テスト
      const [datasets] = await bigquery.getDatasets();
      const targetDataset = datasets.find((d: any) => d.id === 'kabu2411');
      
      if (targetDataset) {
        // テーブルの存在確認
        const [tables] = await targetDataset.getTables();
        const tableNames = tables.map((t: any) => t.id);
        
        bigQueryTestResult = {
          success: true,
          datasetFound: true,
          tablesCount: tables.length,
          sampleTables: tableNames.slice(0, 5),
          allTables: tableNames
        };
      } else {
        bigQueryTestResult = {
          success: false,
          datasetFound: false,
          error: 'Dataset kabu2411 not found'
        };
      }

    } catch (bigQueryError) {
      bigQueryTestResult = {
        success: false,
        error: bigQueryError instanceof Error ? bigQueryError.message : '不明なBigQueryエラー'
      };
    }

    return NextResponse.json({
      success: true,
      message: 'BigQuery設定確認 & 接続テスト',
      timestamp: new Date().toISOString(),
      environment: envCheck,
      credentialsFile: {
        exists: credentialsFileExists,
        path: './service-account-key.json'
      },
      bigQueryTest: bigQueryTestResult
    });

  } catch (error) {
    console.error('❌ テストエラー:', error);
    
    return NextResponse.json(
      {
        success: false,
        error: 'テストで問題が発生しました',
        details: error instanceof Error ? error.message : '不明なエラー',
        timestamp: new Date().toISOString()
      },
      { status: 500 }
    );
  }
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    
    return NextResponse.json({
      success: true,
      message: 'POST request received',
      data: body,
      timestamp: new Date().toISOString()
    });

  } catch (error) {
    return NextResponse.json(
      {
        success: false,
        error: 'POST request failed',
        details: error instanceof Error ? error.message : '不明なエラー'
      },
      { status: 500 }
    );
  }
}