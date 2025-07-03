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

// 🔧 修正: 統計項目を5項目に削減（4項目削除）
interface ConfigStats {
  total_samples: number;      // learning_total_signals
  win_rate: number;          // learning_win_rate
  avg_profit_rate: number;   // learning_avg_profit
  std_deviation: number;     // learning_std_deviation
  sharpe_ratio: number;      // learning_sharpe_ratio
  // ❌ 削除: total_profit_rate, max_profit_rate, min_profit_rate, median_profit_rate
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
    // 🔧 修正: LONG/SHORT バリデーションに統一
    if (!['LONG', 'SHORT'].includes(normalizedTradeType)) {
      return NextResponse.json({
        success: false,
        error: 'trade_type は LONG または SHORT である必要があります'
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
      parseFloat(searchParams.get('prev_close_gap_threshold')!) : undefined;

    // 🔧 修正: データソース最適化 - d30から baseline_stats を1クエリで取得
    const baselineStatsQuery = `
      SELECT 
        learning_total_signals,
        learning_win_rate,
        learning_avg_profit,
        learning_std_deviation,
        learning_sharpe_ratio,
        stock_name
      FROM \`kabu-376213.kabu2411.d30_learning_period_snapshot\`
      WHERE signal_type = '${signal_type}'
        AND signal_bin = ${binNumber}
        AND trade_type = '${normalizedTradeType}'
        AND stock_code = '${stock_code}'
      LIMIT 1
    `;

    const baselineResult = await bigquery.query(baselineStatsQuery);

    if (baselineResult.length === 0) {
      return NextResponse.json({
        success: false,
        error: '指定された条件のデータが見つかりません'
      }, { status: 404 });
    }

    const baselineRow = baselineResult[0];
    const baseline_stats: ConfigStats = {
      total_samples: baselineRow.learning_total_signals || 0,
      win_rate: baselineRow.learning_win_rate || 0,
      avg_profit_rate: baselineRow.learning_avg_profit || 0,
      std_deviation: baselineRow.learning_std_deviation || 0,
      sharpe_ratio: baselineRow.learning_sharpe_ratio || 0
    };

    // 🔧 修正: learning_data を d20 から学習期間分のみ取得（高速化）
    const learningDataQuery = `
      SELECT 
        signal_date,
        signal_value,
        day_open as entry_price,
        day_close as exit_price,
        profit_rate,
        is_win,
        trading_volume,
        reference_date,
        day_open,
        day_high,
        day_low,
        day_close,
        prev_close
      FROM \`kabu-376213.kabu2411.d20_basic_signal_results\`
      WHERE signal_type = '${signal_type}'
        AND signal_bin = ${binNumber}
        AND trade_type = '${normalizedTradeType}'
        AND stock_code = '${stock_code}'
        AND signal_date <= '2024-06-30'  -- 学習期間のみ
      ORDER BY signal_date DESC
      LIMIT 500  -- パフォーマンス制限
    `;

    const learningDataResult = await bigquery.query(learningDataQuery);

    const learning_data: LearningPeriodData[] = learningDataResult.map((row: any) => ({
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

    // フィルタ後統計の計算（フィルタ条件がある場合のみ）
    let filtered_stats: ConfigStats | undefined = undefined;
    
    if (profit_target_yen || loss_cut_yen || prev_close_gap_condition !== 'all') {
      // フィルタ適用ロジック（簡易版）
      let filtered_data = learning_data;

      // 前日終値ギャップ条件
      if (prev_close_gap_condition !== 'all' && prev_close_gap_threshold) {
        filtered_data = filtered_data.filter(row => {
          const gap = ((row.day_open - row.prev_close) / row.prev_close) * 100;
          if (prev_close_gap_condition === 'above') {
            return gap >= prev_close_gap_threshold;
          } else if (prev_close_gap_condition === 'below') {
            return gap <= -prev_close_gap_threshold;
          }
          return true;
        });
      }

      // 利確・損切シミュレーション（簡易版）
      if (profit_target_yen || loss_cut_yen) {
        filtered_data = filtered_data.map(row => {
          let simulated_profit_rate = row.profit_rate;
          const price_change = row.exit_price - row.entry_price;
          
          if (profit_target_yen && Math.abs(price_change) >= profit_target_yen) {
            simulated_profit_rate = (profit_target_yen / row.entry_price) * 100;
          }
          
          if (loss_cut_yen && price_change < 0 && Math.abs(price_change) >= loss_cut_yen) {
            simulated_profit_rate = -(loss_cut_yen / row.entry_price) * 100;
          }
          
          return { ...row, profit_rate: simulated_profit_rate, is_win: simulated_profit_rate > 0 };
        });
      }

      // フィルタ後統計計算
      if (filtered_data.length > 0) {
        const total_samples = filtered_data.length;
        const win_count = filtered_data.filter(row => row.is_win).length;
        const win_rate = (win_count / total_samples) * 100;
        const avg_profit_rate = filtered_data.reduce((sum, row) => sum + row.profit_rate, 0) / total_samples;
        
        const variance = filtered_data.reduce((sum, row) => 
          sum + Math.pow(row.profit_rate - avg_profit_rate, 2), 0) / total_samples;
        const std_deviation = Math.sqrt(variance);
        
        const sharpe_ratio = std_deviation !== 0 ? avg_profit_rate / std_deviation : 0;

        filtered_stats = {
          total_samples,
          win_rate: Math.round(win_rate * 100) / 100,
          avg_profit_rate: Math.round(avg_profit_rate * 100) / 100,
          std_deviation: Math.round(std_deviation * 100) / 100,
          sharpe_ratio: Math.round(sharpe_ratio * 100) / 100
        };
      }
    }

    // 🔧 修正: シグナル情報をマスタテーブルから取得
    const signalInfoQuery = `
      SELECT 
        st.description as signal_description,
        st.signal_category
      FROM \`kabu-376213.kabu2411.m01_signal_types\` st
      WHERE st.signal_type = '${signal_type}'
      LIMIT 1
    `;

    const signalInfoResult = await bigquery.query(signalInfoQuery);

    const signal_description = signalInfoResult.length > 0 ? 
      signalInfoResult[0].signal_description || signal_type : signal_type;

    const response: ConfigResponse = {
      learning_data,
      baseline_stats,
      filtered_stats,
      signal_info: {
        signal_type: signal_type,
        signal_bin: binNumber,
        trade_type: normalizedTradeType,
        stock_code: stock_code,
        stock_name: baselineRow.stock_name || stock_code,
        signal_description: signal_description
      }
    };

    console.log(`✅ 条件設定データ取得完了: ${learning_data.length}件`);

    return NextResponse.json({
      success: true,
      data: response
    });

  } catch (error) {
    console.error('条件設定データ取得エラー:', error);
    return NextResponse.json({
      success: false,
      error: 'データの取得中にエラーが発生しました'
    }, { status: 500 });
  }
}