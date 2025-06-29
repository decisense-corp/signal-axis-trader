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
  processing_status: '済（対象あり）' | '済（対象なし）' | '未（対象あり）' | '未（対象なし）';
  total_samples: number;
  avg_win_rate: number;
  avg_expected_return: number;
}

export async function GET(request: NextRequest) {
  try {
    console.log('🔍 機能1: 明日のシグナル処理開始...');
    
    const bigquery = new BigQueryClient();
    
    // URLパラメータから設定を取得
    const { searchParams } = new URL(request.url);
    const limit = parseInt(searchParams.get('limit') || '1000');  // デフォルトを大きく
    const offset = parseInt(searchParams.get('offset') || '0');
    const orderBy = searchParams.get('orderBy') || 'max_win_rate';
    const orderDir = searchParams.get('orderDir') || 'DESC';

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
    
    if (!tradingDateResult || tradingDateResult.length === 0) {
      throw new Error('営業日カレンダーからデータを取得できませんでした');
    }
    
    const rawNextTradingDate = tradingDateResult[0]?.next_trading_date;
    
    if (!rawNextTradingDate) {
      throw new Error('次の営業日データが取得できませんでした');
    }
    
    // 日付フォーマット処理（型安全版）
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

    console.log(`📅 対象日: ${tomorrowStr}`);

    // 仕様書通りのクエリ: 明日発生する具体的な4軸シグナルのみを対象
    const query = `
      WITH tomorrow_4axis_signals AS (
        -- 明日発生予定の具体的な4軸シグナルを取得
        SELECT 
          sr.stock_code,
          sr.stock_name,
          CASE 
            WHEN sr.signal_value > 0 THEN 'Buy'
            ELSE 'Sell'
          END as trade_type,
          sr.signal_type,
          sr.signal_bin
        FROM \`kabu-376213.kabu2411.d01_signals_raw\` sr
        WHERE sr.signal_date = DATE('${tomorrowStr}')
          AND sr.stock_code IN (
            SELECT stock_code 
            FROM \`kabu-376213.kabu2411.master_trading_stocks\`
          )
          AND sr.signal_value IS NOT NULL
      ),
      
      axis_performance AS (
        -- 明日の4軸シグナルに対応するパフォーマンス統計のみを取得
        SELECT
          t4.stock_code,
          t4.stock_name,
          t4.trade_type,
          t4.signal_type,
          t4.signal_bin,
          pf.win_rate,
          pf.avg_profit_rate as expected_value,
          pf.total_count,
          CASE 
            WHEN pf.win_rate >= 55.0 AND pf.avg_profit_rate >= 0.5 
            THEN 1 ELSE 0 
          END as is_excellent
        FROM tomorrow_4axis_signals t4
        LEFT JOIN \`kabu-376213.kabu2411.d02_signal_performance_4axis\` pf
          ON t4.stock_code = pf.stock_code
          AND t4.trade_type = pf.trade_type
          AND t4.signal_type = pf.signal_type
          AND t4.signal_bin = pf.signal_bin
      ),
      
      stock_summary AS (
        -- 銘柄×売買方向でサマリを作成
        SELECT
          stock_code,
          stock_name,
          trade_type,
          COUNT(*) as total_4axis_signals,
          COUNT(CASE WHEN is_excellent = 1 THEN 1 END) as excellent_pattern_count,
          MAX(COALESCE(win_rate, 0)) as max_win_rate,
          MAX(COALESCE(expected_value, 0)) as max_expected_value,
          AVG(COALESCE(win_rate, 0)) as avg_win_rate,
          AVG(COALESCE(expected_value, 0)) as avg_expected_return,
          SUM(COALESCE(total_count, 0)) as total_samples
        FROM axis_performance
        GROUP BY stock_code, stock_name, trade_type
        HAVING COUNT(CASE WHEN is_excellent = 1 THEN 1 END) > 0  -- 優秀パターンがある場合のみ
      )
      
      SELECT
        stock_code,
        stock_name,
        trade_type,
        ROUND(max_win_rate, 1) as max_win_rate,
        ROUND(max_expected_value, 2) as max_expected_value,
        excellent_pattern_count,
        '未（対象あり）' as processing_status,  -- 暫定的に固定
        total_samples,
        ROUND(avg_win_rate, 1) as avg_win_rate,
        ROUND(avg_expected_return, 2) as avg_expected_return
      FROM stock_summary
      ORDER BY 
        CASE 
          WHEN '${orderBy}' = 'max_win_rate' THEN max_win_rate
          WHEN '${orderBy}' = 'max_expected_value' THEN max_expected_value
          WHEN '${orderBy}' = 'excellent_pattern_count' THEN excellent_pattern_count
          ELSE max_win_rate
        END ${orderDir}
      LIMIT ${limit} OFFSET ${offset}
    `;

    console.log('📊 明日のシグナル取得クエリ実行中...');
    const results = await bigquery.query(query);
    
    // 型変換とフォーマット
    const candidates: TomorrowSignalCandidate[] = results.map((row: any) => ({
      stock_code: row.stock_code,
      stock_name: row.stock_name,
      trade_type: row.trade_type,
      max_win_rate: row.max_win_rate,
      max_expected_value: row.max_expected_value,
      excellent_pattern_count: row.excellent_pattern_count,
      processing_status: row.processing_status,  // 文字列として取得
      total_samples: row.total_samples,
      avg_win_rate: row.avg_win_rate,
      avg_expected_return: row.avg_expected_return,
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

    console.log(`✅ 明日のシグナル ${candidates.length}件を取得`);

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
        target_date: tomorrowStr,
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