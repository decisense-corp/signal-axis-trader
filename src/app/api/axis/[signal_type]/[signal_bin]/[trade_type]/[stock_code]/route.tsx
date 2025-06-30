// src/app/api/axis/[signal_type]/[signal_bin]/[trade_type]/[stock_code]/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { BigQueryClient } from '@/lib/bigquery';

interface AxisDetailData {
  signal_type: string;
  signal_bin: number;
  trade_type: string;
  stock_code: string;
  stock_name: string;
  // 学習期間統計
  learning_win_rate: number;
  learning_avg_profit: number;
  learning_samples: number;
  learning_sharpe_ratio: number;
  learning_std_deviation: number;
  learning_median_profit: number;
  learning_max_profit: number;
  learning_min_profit: number;
  learning_first_date: string;
  learning_last_date: string;
  // 最近の実績（検証期間）
  recent_win_rate: number;
  recent_avg_profit: number;
  recent_samples: number;
  recent_sharpe_ratio: number;
  recent_std_deviation: number;
  // 優秀パターン判定
  is_excellent: boolean;
  pattern_category: string;
  // シグナルタイプ情報
  signal_category: string;
  signal_description: string;
}

interface RouteContext {
  params: Promise<{
    signal_type: string;
    signal_bin: string;
    trade_type: string;
    stock_code: string;
  }>;
}

export async function GET(request: NextRequest, context: RouteContext) {
  try {
    // Next.js 15: paramsはPromiseなのでawaitが必要
    const { signal_type, signal_bin, trade_type, stock_code } = await context.params;
    
    console.log(`🔍 4軸詳細取得: ${signal_type}/${signal_bin}/${trade_type}/${stock_code}`);
    
    // パラメータバリデーション
    if (!signal_type || !signal_bin || !trade_type || !stock_code) {
      return NextResponse.json({
        success: false,
        error: '全てのパラメータが必須です: signal_type, signal_bin, trade_type, stock_code'
      }, { status: 400 });
    }
    
    const binNumber = parseInt(signal_bin);
    if (isNaN(binNumber) || binNumber < 1 || binNumber > 20) {
      return NextResponse.json({
        success: false,
        error: 'signal_binは1-20の範囲である必要があります'
      }, { status: 400 });
    }
    
    // trade_typeの正規化
    const normalizedTradeType = trade_type.toUpperCase();
    if (!['BUY', 'SELL'].includes(normalizedTradeType)) {
      return NextResponse.json({
        success: false,
        error: 'trade_type は BUY または SELL である必要があります'
      }, { status: 400 });
    }

    const bigquery = new BigQueryClient();
    
    // 4軸詳細データを取得
    const query = `
      SELECT
        -- 基本情報
        lps.signal_type,
        lps.signal_bin,
        lps.trade_type,
        lps.stock_code,
        lps.stock_name,
        
        -- 学習期間統計（d30_learning_period_snapshot）
        ROUND(lps.win_rate, 1) as learning_win_rate,
        ROUND(lps.avg_profit_rate, 2) as learning_avg_profit,
        lps.total_signals as learning_samples,
        ROUND(lps.sharpe_ratio, 3) as learning_sharpe_ratio,
        ROUND(lps.std_deviation, 3) as learning_std_deviation,
        ROUND(lps.median_profit_rate, 2) as learning_median_profit,
        ROUND(lps.max_profit_rate, 2) as learning_max_profit,
        ROUND(lps.min_profit_rate, 2) as learning_min_profit,
        lps.first_signal_date as learning_first_date,
        lps.last_signal_date as learning_last_date,
        
        -- 最近の実績（検証期間、d40_axis_performance_stats）
        ROUND(COALESCE(aps.recent_win_rate, 0), 1) as recent_win_rate,
        ROUND(COALESCE(aps.recent_avg_profit, 0), 2) as recent_avg_profit,
        COALESCE(aps.recent_total_signals, 0) as recent_samples,
        ROUND(COALESCE(aps.recent_sharpe_ratio, 0), 3) as recent_sharpe_ratio,
        ROUND(COALESCE(aps.recent_std_deviation, 0), 3) as recent_std_deviation,
        
        -- 優秀パターン判定
        CASE 
          WHEN lps.win_rate >= 55.0 
          AND lps.avg_profit_rate >= 0.5 
          AND lps.total_signals >= 10 
          THEN true 
          ELSE false 
        END as is_excellent,
        
        CASE 
          WHEN lps.win_rate >= 70.0 AND lps.avg_profit_rate >= 1.0 THEN '超優秀'
          WHEN lps.win_rate >= 65.0 AND lps.avg_profit_rate >= 0.8 THEN '優秀'
          WHEN lps.win_rate >= 60.0 AND lps.avg_profit_rate >= 0.6 THEN '良好'
          WHEN lps.win_rate >= 55.0 AND lps.avg_profit_rate >= 0.5 THEN '標準'
          ELSE '要注意'
        END as pattern_category,
        
        -- シグナルタイプ情報
        COALESCE(st.signal_category, 'Unknown') as signal_category,
        COALESCE(st.description, lps.signal_type) as signal_description
        
      FROM \`kabu-376213.kabu2411.d30_learning_period_snapshot\` lps
      LEFT JOIN \`kabu-376213.kabu2411.d40_axis_performance_stats\` aps
        ON lps.signal_type = aps.signal_type
        AND lps.signal_bin = aps.signal_bin
        AND lps.trade_type = aps.trade_type
        AND lps.stock_code = aps.stock_code
      LEFT JOIN \`kabu-376213.kabu2411.m20_signal_types\` st
        ON lps.signal_type = st.signal_type
      WHERE lps.signal_type = '${signal_type}'
        AND lps.signal_bin = ${binNumber}
        AND lps.trade_type = '${normalizedTradeType}'
        AND lps.stock_code = '${stock_code}'
    `;

    console.log('📊 4軸詳細データ取得クエリ実行中...');
    
    const results = await bigquery.query(query);
    
    if (results.length === 0) {
      return NextResponse.json({
        success: false,
        error: '指定された4軸パターンが見つかりませんでした',
        details: `${signal_type}/${binNumber}/${normalizedTradeType}/${stock_code} のデータが存在しません`
      }, { status: 404 });
    }

    const row = results[0];
    const axisDetail: AxisDetailData = {
      signal_type: row.signal_type,
      signal_bin: row.signal_bin,
      trade_type: row.trade_type,
      stock_code: row.stock_code,
      stock_name: row.stock_name,
      learning_win_rate: row.learning_win_rate || 0,
      learning_avg_profit: row.learning_avg_profit || 0,
      learning_samples: row.learning_samples || 0,
      learning_sharpe_ratio: row.learning_sharpe_ratio || 0,
      learning_std_deviation: row.learning_std_deviation || 0,
      learning_median_profit: row.learning_median_profit || 0,
      learning_max_profit: row.learning_max_profit || 0,
      learning_min_profit: row.learning_min_profit || 0,
      learning_first_date: row.learning_first_date || '',
      learning_last_date: row.learning_last_date || '',
      recent_win_rate: row.recent_win_rate || 0,
      recent_avg_profit: row.recent_avg_profit || 0,
      recent_samples: row.recent_samples || 0,
      recent_sharpe_ratio: row.recent_sharpe_ratio || 0,
      recent_std_deviation: row.recent_std_deviation || 0,
      is_excellent: row.is_excellent || false,
      pattern_category: row.pattern_category || '要注意',
      signal_category: row.signal_category || 'Unknown',
      signal_description: row.signal_description || row.signal_type
    };

    console.log(`✅ 4軸詳細データ取得完了: ${axisDetail.pattern_category}パターン`);

    return NextResponse.json({
      success: true,
      data: axisDetail,
      metadata: {
        signal_type,
        signal_bin: binNumber,
        trade_type: normalizedTradeType,
        stock_code,
        query_time: new Date().toISOString(),
        description: `4軸詳細: ${signal_type}(${binNumber}) ${normalizedTradeType} ${stock_code} の統計データ`
      }
    });

  } catch (error) {
    console.error('❌ 4軸詳細データ取得エラー:', error);
    
    return NextResponse.json({
      success: false,
      error: '4軸詳細データの取得に失敗しました',
      details: error instanceof Error ? error.message : '不明なエラー'
    }, { status: 500 });
  }
}