// src/app/api/signals/tomorrow/[stock_code]/[trade_type]/details/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { BigQueryClient } from '@/lib/bigquery';

interface SignalTypeBinData {
  signal_type: string;
  bins: BinDetail[];
  tomorrow_bins: number[];
  excellent_bins_count: number;
}

interface BinDetail {
  bin: number;
  win_rate: number;
  avg_profit_rate: number;
  sample_count: number;
  sharpe_ratio: number;
  is_excellent: boolean;
  is_tomorrow: boolean;
}

interface RouteContext {
  params: Promise<{
    stock_code: string;
    trade_type: string;
  }>;
}

export async function GET(request: NextRequest, context: RouteContext) {
  try {
    const { stock_code, trade_type } = await context.params;
    
    console.log(`🔍 bin選択データ取得: ${stock_code} ${trade_type}`);
    
    // パラメータバリデーション
    if (!stock_code || !trade_type) {
      return NextResponse.json({
        success: false,
        error: 'stock_code と trade_type は必須です'
      }, { status: 400 });
    }
    
    const normalizedTradeType = trade_type.toUpperCase();
    if (!['LONG', 'SHORT'].includes(normalizedTradeType)) {
      return NextResponse.json({
        success: false,
        error: 'trade_type は LONG または SHORT である必要があります'
      }, { status: 400 });
    }

    const bigquery = new BigQueryClient();
    
    // 営業日カレンダーから翌営業日を取得
    const nextTradingDateQuery = `
      WITH latest_quote_date AS (
        SELECT MAX(Date) as latest_date
        FROM \`kabu-376213.kabu2411.daily_quotes\`
      )
      SELECT MIN(tc.Date) as next_trading_date
      FROM \`kabu-376213.kabu2411.trading_calendar\` tc
      CROSS JOIN latest_quote_date lqd
      WHERE tc.Date > lqd.latest_date
        AND tc.HolidayDivision = '1'
    `;
    
    const tradingDateResult = await bigquery.query(nextTradingDateQuery);
    const rawNextTradingDate = tradingDateResult[0]?.next_trading_date;
    
    if (!rawNextTradingDate) {
      throw new Error('次の営業日データが取得できませんでした');
    }
    
    // 日付フォーマット処理
    const tomorrowStr = (() => {
      try {
        if (rawNextTradingDate instanceof Date) {
          return rawNextTradingDate.toISOString().split('T')[0];
        } else if (typeof rawNextTradingDate === 'object' && rawNextTradingDate && 'value' in rawNextTradingDate) {
          return String(rawNextTradingDate.value);
        } else {
          const dateStr = String(rawNextTradingDate);
          return dateStr.includes('T') ? dateStr.split('T')[0] : dateStr;
        }
      } catch (error) {
        throw new Error(`日付フォーマットエラー: ${error}`);
      }
    })() as string;

    // 🚀 高速化: 1つのクエリで全データを取得
    const optimizedQuery = `
      WITH tomorrow_signals AS (
        SELECT 
          signal_type,
          signal_bin,
          ANY_VALUE(stock_name) as stock_name
        FROM \`kabu-376213.kabu2411.d15_signals_with_bins\`
        WHERE signal_date = DATE('${tomorrowStr}')
          AND stock_code = '${stock_code}'
          AND signal_bin IS NOT NULL
          AND signal_value IS NOT NULL
        GROUP BY signal_type, signal_bin
      ),
      bin_stats AS (
        SELECT
          d30.signal_type,
          d30.signal_bin,
          ROUND(AVG(d30.learning_win_rate), 1) as win_rate,
          ROUND(AVG(d30.learning_avg_profit), 2) as avg_profit_rate,
          SUM(d30.learning_total_signals) as sample_count,
          ROUND(AVG(d30.learning_sharpe_ratio), 3) as sharpe_ratio,
          LOGICAL_OR(d30.is_excellent_pattern) as is_excellent,
          CASE WHEN ts.signal_bin IS NOT NULL THEN true ELSE false END as is_tomorrow
        FROM \`kabu-376213.kabu2411.d30_learning_period_snapshot\` d30
        LEFT JOIN tomorrow_signals ts
          ON d30.signal_type = ts.signal_type AND d30.signal_bin = ts.signal_bin
        WHERE d30.stock_code = '${stock_code}'
          AND d30.trade_type = '${normalizedTradeType}'
          AND d30.learning_total_signals >= 5
          AND d30.signal_type IN (SELECT DISTINCT signal_type FROM tomorrow_signals)
        GROUP BY d30.signal_type, d30.signal_bin, ts.signal_bin
        ORDER BY d30.signal_type, d30.signal_bin
      )
      SELECT 
        signal_type,
        ARRAY_AGG(STRUCT(
          signal_bin as bin,
          win_rate,
          avg_profit_rate,
          sample_count,
          sharpe_ratio,
          is_excellent,
          is_tomorrow
        ) ORDER BY signal_bin) as bins,
        ARRAY_AGG(CASE WHEN is_tomorrow THEN signal_bin END IGNORE NULLS ORDER BY signal_bin) as tomorrow_bins,
        COUNTIF(is_excellent) as excellent_bins_count,
        (SELECT ANY_VALUE(stock_name) FROM tomorrow_signals LIMIT 1) as stock_name
      FROM bin_stats
      GROUP BY signal_type
      ORDER BY signal_type
    `;
    
    console.log(`🚀 高速化クエリ実行中（1クエリで全データ取得）...`);
    const results = await bigquery.query(optimizedQuery);
    
    if (results.length === 0) {
      return NextResponse.json({
        success: false,
        error: '明日発火するシグナルが見つかりませんでした',
        details: `${stock_code} の明日発火シグナルがありません`
      }, { status: 404 });
    }
    
    const stockName = results[0].stock_name;
    
    // レスポンス構築
    const signalTypesData: SignalTypeBinData[] = results.map((row: any) => ({
      signal_type: row.signal_type,
      bins: row.bins || [],
      tomorrow_bins: row.tomorrow_bins || [],
      excellent_bins_count: row.excellent_bins_count || 0
    }));

    console.log(`✅ 高速化完了: ${signalTypesData.length}個のシグナルタイプを1クエリで取得`);

    return NextResponse.json({
      success: true,
      data: {
        stock_code,
        stock_name: stockName,
        trade_type: normalizedTradeType,
        target_date: tomorrowStr,
        signal_types: signalTypesData
      },
      metadata: {
        signal_types_count: signalTypesData.length,
        total_excellent_bins: signalTypesData.reduce((sum, st) => sum + st.excellent_bins_count, 0),
        total_tomorrow_signals: signalTypesData.reduce((sum, st) => sum + st.tomorrow_bins.length, 0),
        query_time: new Date().toISOString(),
        description: `${stock_code} ${normalizedTradeType} の明日発火bin付きシグナル選択用データ（高速化版）`
      }
    });

  } catch (error) {
    console.error('❌ bin選択データ取得エラー:', error);
    
    return NextResponse.json({
      success: false,
      error: 'bin選択データの取得に失敗しました',
      details: error instanceof Error ? error.message : '不明なエラー'
    }, { status: 500 });
  }
}