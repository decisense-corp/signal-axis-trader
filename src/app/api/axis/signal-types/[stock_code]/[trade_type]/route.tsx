// src/app/api/axis/signal-types/[stock_code]/[trade_type]/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { BigQueryClient } from '@/lib/bigquery';

interface SignalTypeDetail {
  signal_type: string;
  signal_category: string;
  description: string;
  excellent_bins: number[];
  max_win_rate: number;
  max_expected_value: number;
  total_excellent_patterns: number;
  avg_win_rate: number;
  avg_expected_value: number;
  total_samples: number;
  best_bin: number;
  best_bin_win_rate: number;
  best_bin_expected_value: number;
  best_bin_samples: number;
}

interface RouteParams {
  params: {
    stock_code: string;
    trade_type: string;
  };
}

export async function GET(request: NextRequest, context: RouteParams) {
  try {
    const { stock_code, trade_type } = context.params;
    
    console.log(`🔍 4軸シグナルタイプ取得: ${stock_code} ${trade_type} ...`);
    
    // パラメータバリデーション
    if (!stock_code || !trade_type) {
      return NextResponse.json({
        success: false,
        error: 'stock_code と trade_type は必須です'
      }, { status: 400 });
    }
    
    if (!['Buy', 'Sell'].includes(trade_type)) {
      return NextResponse.json({
        success: false,
        error: 'trade_type は Buy または Sell である必要があります'
      }, { status: 400 });
    }

    const bigquery = new BigQueryClient();
    
    // 銘柄×売買方向の優秀シグナルタイプを取得（新システム用テーブル使用）
    const query = `
      WITH excellent_4axis AS (
        -- 優秀な4軸パターンを抽出（学習期間ベースのスナップショットから）
        SELECT
          signal_type,
          signal_bin,
          win_rate,
          avg_profit_rate as expected_value,
          total_signals as samples
        FROM \`kabu-376213.kabu2411.d30_learning_period_snapshot\` 
        WHERE stock_code = '${stock_code}'
          AND trade_type = '${trade_type}'
          AND win_rate >= 55.0
          AND avg_profit_rate >= 0.5
          AND total_signals >= 10  -- 最低サンプル数
      ),
      
      signal_type_summary AS (
        -- シグナルタイプごとに集計
        SELECT
          signal_type,
          ARRAY_AGG(signal_bin ORDER BY signal_bin) as excellent_bins,
          COUNT(*) as total_excellent_patterns,
          MAX(win_rate) as max_win_rate,
          MAX(expected_value) as max_expected_value,
          AVG(win_rate) as avg_win_rate,
          AVG(expected_value) as avg_expected_value,
          SUM(samples) as total_samples,
          -- 最も良いbin値の詳細
          ARRAY_AGG(
            STRUCT(signal_bin, win_rate, expected_value, samples)
            ORDER BY expected_value DESC, win_rate DESC
            LIMIT 1
          )[OFFSET(0)] as best_bin_info
        FROM excellent_4axis
        GROUP BY signal_type
      )
      
      SELECT
        sts.signal_type,
        COALESCE(st.signal_category, 'Unknown') as signal_category,
        COALESCE(st.description, sts.signal_type) as description,
        sts.excellent_bins,
        ROUND(sts.max_win_rate, 1) as max_win_rate,
        ROUND(sts.max_expected_value, 2) as max_expected_value,
        sts.total_excellent_patterns,
        ROUND(sts.avg_win_rate, 1) as avg_win_rate,
        ROUND(sts.avg_expected_value, 2) as avg_expected_value,
        sts.total_samples,
        sts.best_bin_info.signal_bin as best_bin,
        ROUND(sts.best_bin_info.win_rate, 1) as best_bin_win_rate,
        ROUND(sts.best_bin_info.expected_value, 2) as best_bin_expected_value,
        sts.best_bin_info.samples as best_bin_samples
      FROM signal_type_summary sts
      LEFT JOIN \`kabu-376213.kabu2411.m20_signal_types\` st
        ON sts.signal_type = st.signal_type
      ORDER BY sts.max_expected_value DESC, sts.max_win_rate DESC
    `;

    console.log('📊 シグナルタイプ一覧取得クエリ実行中...');
    
    const results = await bigquery.query(query);

    // 型変換とフォーマット
    const signalTypes: SignalTypeDetail[] = results.map((row: any) => ({
      signal_type: row.signal_type,
      signal_category: row.signal_category,
      description: row.description,
      excellent_bins: row.excellent_bins,
      max_win_rate: row.max_win_rate,
      max_expected_value: row.max_expected_value,
      total_excellent_patterns: row.total_excellent_patterns,
      avg_win_rate: row.avg_win_rate,
      avg_expected_value: row.avg_expected_value,
      total_samples: row.total_samples,
      best_bin: row.best_bin,
      best_bin_win_rate: row.best_bin_win_rate,
      best_bin_expected_value: row.best_bin_expected_value,
      best_bin_samples: row.best_bin_samples,
    }));

    console.log(`✅ ${stock_code} ${trade_type} のシグナルタイプ ${signalTypes.length}件を取得`);

    return NextResponse.json({
      success: true,
      data: signalTypes,
      metadata: {
        stock_code,
        trade_type,
        signal_types_count: signalTypes.length,
        query_time: new Date().toISOString(),
        description: `4軸分析: ${stock_code} ${trade_type} の優秀シグナルタイプ一覧`
      }
    });

  } catch (error) {
    console.error('❌ シグナルタイプ一覧取得エラー:', error);
    
    return NextResponse.json({
      success: false,
      error: 'シグナルタイプデータの取得に失敗しました',
      details: error instanceof Error ? error.message : '不明なエラー'
    }, { status: 500 });
  }
}