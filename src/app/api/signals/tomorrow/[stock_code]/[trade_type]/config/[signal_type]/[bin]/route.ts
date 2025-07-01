// src/app/api/signals/tomorrow/[stock_code]/[trade_type]/config/[signal_type]/[bin]/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { BigQueryClient } from '@/lib/bigquery';

interface LearningPeriodData {
  signal_date: string;
  signal_value: number;
  entry_price: number;
  exit_price: number;
  profit_rate: number;
  is_win: boolean;
  trading_volume: number;
  reference_date: string;
  day_open: number;
  day_high: number;
  day_low: number;
  day_close: number;
  prev_close: number;
}

interface ConfigStats {
  total_samples: number;
  win_rate: number;
  avg_profit_rate: number;
  total_profit_rate: number;
  max_profit_rate: number;
  min_profit_rate: number;
  std_deviation: number;
  sharpe_ratio: number;
  median_profit_rate: number;
}

interface ConfigFilterConditions {
  profit_target_yen?: number;      // 利確目標（円）
  loss_cut_yen?: number;          // 損切設定（円）
  prev_close_gap_condition?: 'all' | 'above' | 'below';  // 前日終値ギャップ
  prev_close_gap_threshold?: number;  // ギャップ閾値
}

interface ConfigResponse {
  learning_data: LearningPeriodData[];
  baseline_stats: ConfigStats;      // フィルタ前統計
  filtered_stats?: ConfigStats | undefined;     // フィルタ後統計（undefinedを明示的に許可）
  signal_info: {
    signal_type: string;
    signal_bin: number;
    trade_type: string;
    stock_code: string;
    stock_name: string;
    signal_description: string;
  };
}

interface RouteContext {
  params: Promise<{
    stock_code: string;
    trade_type: string;
    signal_type: string;
    bin: string;
  }>;
}

export async function GET(request: NextRequest, context: RouteContext) {
  try {
    // Next.js 15: paramsはPromiseなのでawaitが必要
    const { stock_code, trade_type, signal_type, bin } = await context.params;
    
    console.log(`🔍 条件設定画面データ取得: ${signal_type}/${bin}/${trade_type}/${stock_code}`);
    
    // パラメータバリデーション
    const binNumber = parseInt(bin);
    if (isNaN(binNumber) || binNumber < 1 || binNumber > 20) {
      return NextResponse.json({
        success: false,
        error: 'binは1-20の範囲である必要があります'
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
    
    // URLパラメータからフィルタ条件を取得
    const { searchParams } = new URL(request.url);
    const profit_target_yen = searchParams.get('profit_target_yen') ? 
      parseFloat(searchParams.get('profit_target_yen')!) : undefined;
    const loss_cut_yen = searchParams.get('loss_cut_yen') ? 
      parseFloat(searchParams.get('loss_cut_yen')!) : undefined;
    const prev_close_gap_condition = searchParams.get('prev_close_gap_condition') as 'all' | 'above' | 'below' || 'all';
    const prev_close_gap_threshold = searchParams.get('prev_close_gap_threshold') ? 
      parseFloat(searchParams.get('prev_close_gap_threshold')!) : 0;

    // 基本情報取得（シグナルタイプ情報）
    const signalInfoQuery = `
      SELECT
        st.signal_type,
        st.signal_category,
        st.description as signal_description,
        mts.company_name as stock_name
      FROM \`kabu-376213.kabu2411.m20_signal_types\` st
      CROSS JOIN \`kabu-376213.kabu2411.master_trading_stocks\` mts
      WHERE st.signal_type = '${signal_type}'
        AND mts.stock_code = '${stock_code}'
    `;
    
    const signalInfoResult = await bigquery.query(signalInfoQuery);
    
    if (signalInfoResult.length === 0) {
      return NextResponse.json({
        success: false,
        error: 'シグナルタイプまたは銘柄が見つかりません'
      }, { status: 404 });
    }
    
    const signalInfo = signalInfoResult[0];

    // 学習期間の基本データ取得（〜2024年6月30日）- 四本値追加版
    let baseQuery = `
      WITH price_data AS (
        SELECT
          bsr.signal_date,
          bsr.signal_value,
          bsr.entry_price,
          bsr.exit_price,
          bsr.profit_rate,
          bsr.is_win,
          bsr.trading_volume,
          bsr.reference_date,
          -- 四本値データを取得
          dq.Open as day_open,
          dq.High as day_high,
          dq.Low as day_low,
          dq.Close as day_close,
          -- 前日終値を取得
          LAG(dq.Close) OVER (PARTITION BY bsr.stock_code ORDER BY bsr.signal_date) as prev_close
        FROM \`kabu-376213.kabu2411.d20_basic_signal_results\` bsr
        LEFT JOIN \`kabu-376213.kabu2411.daily_quotes\` dq
          ON bsr.stock_code = dq.Code
          AND bsr.signal_date = dq.Date
        WHERE bsr.signal_type = '${signal_type}'
          AND bsr.signal_bin = ${binNumber}
          AND bsr.trade_type = '${normalizedTradeType}'
          AND bsr.stock_code = '${stock_code}'
          AND bsr.signal_date <= '2024-06-30'  -- 学習期間のみ
          AND bsr.entry_price IS NOT NULL
          AND bsr.exit_price IS NOT NULL
          AND bsr.profit_rate IS NOT NULL
      )
      SELECT
        signal_date,
        signal_value,
        entry_price,
        exit_price,
        profit_rate,
        is_win,
        trading_volume,
        reference_date,
        day_open,
        day_high,
        day_low,
        day_close,
        prev_close
      FROM price_data
      WHERE prev_close IS NOT NULL  -- 前日終値がある場合のみ
    `;

    // フィルタ条件を追加（BigQueryエラー修正：ウィンドウ関数をCTEで事前計算）
    if (prev_close_gap_condition !== 'all' && prev_close_gap_threshold !== undefined) {
      const gapCondition = prev_close_gap_condition === 'above' 
        ? `>= ${prev_close_gap_threshold}`
        : `< ${prev_close_gap_threshold}`;
      
      baseQuery = `
        WITH gap_calculated AS (
          ${baseQuery}
        ),
        gap_filtered AS (
          SELECT *,
            ((day_open - prev_close) / prev_close * 100) as gap_rate
          FROM gap_calculated
        )
        SELECT 
          signal_date,
          signal_value,
          entry_price,
          exit_price,
          profit_rate,
          is_win,
          trading_volume,
          reference_date,
          day_open,
          day_high,
          day_low,
          day_close,
          prev_close
        FROM gap_filtered
        WHERE gap_rate ${gapCondition}
      `;
    }

    baseQuery += ` ORDER BY signal_date DESC`;

    console.log('📊 学習期間データ取得クエリ実行中...');
    const learningData = await bigquery.query(baseQuery);
    
    // データ型変換
    const formattedLearningData: LearningPeriodData[] = learningData.map((row: any) => ({
      signal_date: row.signal_date,
      signal_value: row.signal_value || 0,
      entry_price: row.entry_price || 0,
      exit_price: row.exit_price || 0,
      profit_rate: row.profit_rate || 0,
      is_win: row.is_win || false,
      trading_volume: row.trading_volume || 0,
      reference_date: row.reference_date || '',
      day_open: row.day_open || 0,
      day_high: row.day_high || 0,
      day_low: row.day_low || 0,
      day_close: row.day_close || 0,
      prev_close: row.prev_close || 0
    }));

    // ベースライン統計計算
    const baselineStats = calculateStats(formattedLearningData);
    
    // フィルタ後統計（利確・損切条件適用）
    let filteredStats: ConfigStats | undefined;
    if (profit_target_yen || loss_cut_yen) {
      const filteredData = applyProfitLossFilter(formattedLearningData, profit_target_yen, loss_cut_yen);
      filteredStats = calculateStats(filteredData);
    }

    const response: ConfigResponse = {
      learning_data: formattedLearningData,
      baseline_stats: baselineStats,
      filtered_stats: filteredStats,
      signal_info: {
        signal_type,
        signal_bin: binNumber,
        trade_type: normalizedTradeType,
        stock_code,
        stock_name: signalInfo.stock_name,
        signal_description: signalInfo.signal_description
      }
    };

    console.log(`✅ 学習期間データ ${formattedLearningData.length}件を取得`);

    return NextResponse.json({
      success: true,
      data: response,
      metadata: {
        signal_type,
        signal_bin: binNumber,
        trade_type: normalizedTradeType,
        stock_code,
        data_period: '学習期間（〜2024年6月30日）',
        total_samples: formattedLearningData.length,
        filters_applied: {
          profit_target_yen,
          loss_cut_yen,
          prev_close_gap_condition,
          prev_close_gap_threshold
        },
        query_time: new Date().toISOString()
      }
    });

  } catch (error) {
    console.error('❌ 条件設定画面データ取得エラー:', error);
    
    return NextResponse.json({
      success: false,
      error: '条件設定データの取得に失敗しました',
      details: error instanceof Error ? error.message : '不明なエラー'
    }, { status: 500 });
  }
}

export async function POST(request: NextRequest, context: RouteContext) {
  try {
    const { stock_code, trade_type, signal_type, bin } = await context.params;
    const body = await request.json();
    
    console.log(`💾 条件設定保存: ${signal_type}/${bin}/${trade_type}/${stock_code}`);
    
    const {
      profit_target_yen,
      loss_cut_yen,
      prev_close_gap_condition = 'all',
      prev_close_gap_threshold = 0,
      additional_notes = ''
    } = body as ConfigFilterConditions & { additional_notes?: string };
    
    // TODO: 条件設定をu10_user_decisionsテーブルに保存
    // 現在は仮実装として成功レスポンスを返す
    
    return NextResponse.json({
      success: true,
      message: '条件設定が保存されました',
      data: {
        signal_type,
        signal_bin: parseInt(bin),
        trade_type: trade_type.toUpperCase(),
        stock_code,
        conditions: {
          profit_target_yen,
          loss_cut_yen,
          prev_close_gap_condition,
          prev_close_gap_threshold,
          additional_notes
        }
      }
    });

  } catch (error) {
    console.error('❌ 条件設定保存エラー:', error);
    
    return NextResponse.json({
      success: false,
      error: '条件設定の保存に失敗しました',
      details: error instanceof Error ? error.message : '不明なエラー'
    }, { status: 500 });
  }
}

// 統計計算関数
function calculateStats(data: LearningPeriodData[]): ConfigStats {
  if (data.length === 0) {
    return {
      total_samples: 0,
      win_rate: 0,
      avg_profit_rate: 0,
      total_profit_rate: 0,
      max_profit_rate: 0,
      min_profit_rate: 0,
      std_deviation: 0,
      sharpe_ratio: 0,
      median_profit_rate: 0
    };
  }

  const profits = data.map(d => d.profit_rate);
  const wins = data.filter(d => d.is_win).length;
  const totalProfits = profits.reduce((sum, p) => sum + p, 0);
  const avgProfit = totalProfits / data.length;
  
  // 標準偏差計算
  const variance = profits.reduce((sum, p) => sum + Math.pow(p - avgProfit, 2), 0) / data.length;
  const stdDev = Math.sqrt(variance);
  
  // シャープレシオ（リスクフリーレート0と仮定）
  const sharpeRatio = stdDev > 0 ? avgProfit / stdDev : 0;
  
  // 中央値計算
  const sortedProfits = [...profits].sort((a, b) => a - b);
  let median: number;
  if (data.length % 2 === 0) {
    // 偶数の場合：中央の2つの値の平均
    const mid1 = sortedProfits[data.length / 2 - 1];
    const mid2 = sortedProfits[data.length / 2];
    median = mid1 !== undefined && mid2 !== undefined ? (mid1 + mid2) / 2 : 0;
  } else {
    // 奇数の場合：中央の値
    const midValue = sortedProfits[Math.floor(data.length / 2)];
    median = midValue !== undefined ? midValue : 0;
  }

  return {
    total_samples: data.length,
    win_rate: (wins / data.length) * 100,
    avg_profit_rate: avgProfit,
    total_profit_rate: totalProfits,
    max_profit_rate: Math.max(...profits),
    min_profit_rate: Math.min(...profits),
    std_deviation: stdDev,
    sharpe_ratio: sharpeRatio,
    median_profit_rate: median
  };
}

// 利確・損切フィルタ適用関数
function applyProfitLossFilter(
  data: LearningPeriodData[], 
  profitTargetYen?: number, 
  lossCutYen?: number
): LearningPeriodData[] {
  return data.map(item => {
    let adjustedExitPrice = item.exit_price;
    let adjustedIsWin = item.is_win;
    let adjustedProfitRate = item.profit_rate;
    
    if (profitTargetYen && lossCutYen) {
      const profitTargetPrice = item.entry_price + profitTargetYen;
      const lossCutPrice = item.entry_price - lossCutYen;
      
      // 利確・損切の両方に到達した場合は損切優先
      if (item.exit_price >= profitTargetPrice && item.exit_price <= lossCutPrice) {
        adjustedExitPrice = lossCutPrice;
        adjustedIsWin = false;
      } else if (item.exit_price >= profitTargetPrice) {
        adjustedExitPrice = profitTargetPrice;
        adjustedIsWin = true;
      } else if (item.exit_price <= lossCutPrice) {
        adjustedExitPrice = lossCutPrice;
        adjustedIsWin = false;
      }
      
      adjustedProfitRate = ((adjustedExitPrice - item.entry_price) / item.entry_price) * 100;
    }
    
    return {
      ...item,
      exit_price: adjustedExitPrice,
      is_win: adjustedIsWin,
      profit_rate: adjustedProfitRate
    };
  });
}