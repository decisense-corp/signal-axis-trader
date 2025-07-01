// src/app/api/signals/tomorrow/[stock_code]/[trade_type]/details/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { BigQueryClient } from '@/lib/bigquery';

interface SignalTypeBinData {
  signal_type: string;
  signal_category: string;
  description: string;
  bins: BinDetail[];
  tomorrow_bins: number[]; // 明日発火するbin一覧
  excellent_bins_count: number; // 優秀bin数
}

interface BinDetail {
  bin: number;
  win_rate: number;
  avg_profit_rate: number;
  sample_count: number;
  sharpe_ratio: number;
  is_excellent: boolean;
  is_tomorrow: boolean; // 明日発火するかどうか
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
    if (!['BUY', 'SELL'].includes(normalizedTradeType)) {
      return NextResponse.json({
        success: false,
        error: 'trade_type は BUY または SELL である必要があります'
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

    // 明日発火するbinが優秀パターンのシグナルタイプのみ取得
    const excellentTomorrowSignalsQuery = `
      WITH tomorrow_signals AS (
        SELECT 
          signal_type,
          signal_bin,
          stock_name
        FROM \`kabu-376213.kabu2411.d10_simple_signals\`
        WHERE signal_date = DATE('${tomorrowStr}')
          AND stock_code = '${stock_code}'
          AND signal_bin IS NOT NULL
          AND signal_value IS NOT NULL
      ),
      
      excellent_tomorrow_signals AS (
        SELECT 
          ts.signal_type,
          ts.signal_bin,
          ts.stock_name,
          lps.win_rate,
          lps.avg_profit_rate,
          lps.total_signals
        FROM tomorrow_signals ts
        INNER JOIN \`kabu-376213.kabu2411.d30_learning_period_snapshot\` lps
          ON ts.signal_type = lps.signal_type
          AND ts.signal_bin = lps.signal_bin  -- 明日発火するbin自体をチェック
          AND lps.stock_code = '${stock_code}'
          AND lps.trade_type = '${normalizedTradeType}'
        WHERE lps.win_rate >= 55.0 
          AND lps.avg_profit_rate >= 0.5
          AND lps.total_signals >= 10
      )
      
      SELECT 
        signal_type,
        signal_bin,
        stock_name,
        win_rate,
        avg_profit_rate,
        total_signals
      FROM excellent_tomorrow_signals
      ORDER BY signal_type, signal_bin
    `;
    
    const excellentTomorrowSignals = await bigquery.query(excellentTomorrowSignalsQuery);
    
    if (excellentTomorrowSignals.length === 0) {
      return NextResponse.json({
        success: false,
        error: '明日発火する優秀binが見つかりませんでした',
        details: `${stock_code} ${normalizedTradeType} の明日発火binに優秀パターンがありません`
      }, { status: 404 });
    }
    
    const stockName = excellentTomorrowSignals[0].stock_name;
    const excellentSignalTypes = Array.from(new Set(excellentTomorrowSignals.map((s: any) => s.signal_type)));
    
    // 明日発火する優秀binの詳細ログ
    excellentTomorrowSignals.forEach((signal: any) => {
      console.log(`🌟 明日発火する優秀bin: ${signal.signal_type}/Bin${signal.signal_bin} (勝率${signal.win_rate}%, 期待値${signal.avg_profit_rate}%)`);
    });
    
    console.log(`📊 明日発火する優秀binを持つシグナルタイプ: ${excellentSignalTypes.length}個 (${excellentSignalTypes.join(', ')})`);
    
    // 優秀シグナルタイプごとのbin統計を取得
    const signalTypesData: SignalTypeBinData[] = [];
    
    for (const signalType of excellentSignalTypes) {
      // このシグナルタイプの全bin統計を取得（学習期間ベース）
      const binStatsQuery = `
        SELECT
          signal_bin,
          ROUND(win_rate, 1) as win_rate,
          ROUND(avg_profit_rate, 2) as avg_profit_rate,
          total_signals as sample_count,
          ROUND(sharpe_ratio, 3) as sharpe_ratio,
          CASE 
            WHEN win_rate >= 55.0 
            AND avg_profit_rate >= 0.5 
            AND total_signals >= 10 
            THEN true 
            ELSE false 
          END as is_excellent
        FROM \`kabu-376213.kabu2411.d30_learning_period_snapshot\`
        WHERE signal_type = '${signalType}'
          AND trade_type = '${normalizedTradeType}'
          AND stock_code = '${stock_code}'
          AND total_signals >= 5  -- 最低限のサンプル数
        ORDER BY signal_bin
      `;
      
      const binStats = await bigquery.query(binStatsQuery);
      
      // 優秀binの数をカウント
      const excellentBinsCount = binStats.filter((row: any) => row.is_excellent).length;
      
      // 優秀パターンがないシグナルタイプはスキップ
      if (excellentBinsCount === 0) {
        console.log(`⚠️ ${signalType}: 優秀binが0個のためスキップ`);
        continue;
      }
      
      // シグナルタイプ情報を取得
      const signalTypeInfoQuery = `
        SELECT 
          signal_category,
          description
        FROM \`kabu-376213.kabu2411.m20_signal_types\`
        WHERE signal_type = '${signalType}'
      `;
      
      const signalTypeInfo = await bigquery.query(signalTypeInfoQuery);
      
      // 明日発火するbinを抽出
      const tomorrowBins = excellentTomorrowSignals
        .filter(s => s.signal_type === signalType)
        .map(s => s.signal_bin);
      
      // bin詳細データを構築
      const bins: BinDetail[] = binStats.map(row => ({
        bin: row.signal_bin,
        win_rate: row.win_rate || 0,
        avg_profit_rate: row.avg_profit_rate || 0,
        sample_count: row.sample_count || 0,
        sharpe_ratio: row.sharpe_ratio || 0,
        is_excellent: row.is_excellent || false,
        is_tomorrow: tomorrowBins.includes(row.signal_bin)
      }));
      
      signalTypesData.push({
        signal_type: signalType,
        signal_category: signalTypeInfo[0]?.signal_category || 'Unknown',
        description: signalTypeInfo[0]?.description || signalType,
        bins,
        tomorrow_bins: tomorrowBins,
        excellent_bins_count: excellentBinsCount
      });
      
      console.log(`✅ ${signalType}: 全bin ${bins.length}個, 優秀bin ${excellentBinsCount}個, 明日発火bin ${tomorrowBins.length}個`);
    }

    console.log(`✅ ${stock_code} ${normalizedTradeType} のbin選択データ取得完了: ${signalTypesData.length}個の優秀シグナルタイプ`);

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
        total_excellent_tomorrow_signals: excellentTomorrowSignals.length,
        query_time: new Date().toISOString(),
        description: `${stock_code} ${normalizedTradeType} の明日発火する優秀bin付きシグナル選択用データ`
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