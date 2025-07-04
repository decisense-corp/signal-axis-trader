// src/app/api/signals/config/route.ts
// 申し送り書仕様準拠：チューニング画面用API（D020統計 + D010動的計算）
import { NextRequest, NextResponse } from 'next/server';
import { BigQueryClient } from '@/lib/bigquery';
import { calculateFilteredProfitRate, calculateStats } from '@/lib/filterLogic';

const bigquery = new BigQueryClient();

// 型定義
interface SignalInfo {
  signal_type: string;
  signal_bin: number;
  trade_type: 'BUY' | 'SELL';
  stock_code: string;
  stock_name: string;
}

interface BaselineStats {
  total_samples: number;
  win_rate: number;
  avg_profit_rate: number;
}

interface FilteredStats {
  total_samples: number;
  win_rate: number;
  avg_profit_rate: number;
}

interface DetailData {
  signal_date: string;
  prev_close_to_open_gap: number;
  open_to_high_gap: number;
  open_to_low_gap: number;
  open_to_close_gap: number;
  baseline_profit_rate: number;
  filtered_profit_rate: number;
  trading_volume: number;
  // 価格データを追加（フロントエンドでの再計算用）
  prev_close: number;
  day_open: number;
  day_high: number;
  day_low: number;
  day_close: number;
}

interface ConfigResponse {
  signal_info: SignalInfo;
  baseline_stats: BaselineStats;
  filtered_stats?: FilteredStats | undefined;
  detail_data: DetailData[];
}

export async function GET(request: NextRequest) {
  try {
    console.log('🔧 チューニング画面API開始...');
    
    const { searchParams } = new URL(request.url);
    
    // URLパラメータ取得
    const signal_type = searchParams.get('signal_type');
    const signal_bin = searchParams.get('signal_bin');
    const trade_type = searchParams.get('trade_type');
    const stock_code = searchParams.get('stock_code');
    
    // フィルタ条件取得
    const profit_target_yen = parseFloat(searchParams.get('profit_target_yen') || '0');
    const loss_cut_yen = parseFloat(searchParams.get('loss_cut_yen') || '0');
    const prev_close_gap_condition = searchParams.get('prev_close_gap_condition') || 'all';

    // パラメータ検証
    if (!signal_type || !signal_bin || !trade_type || !stock_code) {
      return NextResponse.json({
        error: '必須パラメータが不足しています'
      }, { status: 400 });
    }

    // デコード処理
    const decodedSignalType = decodeURIComponent(signal_type);

    console.log('📊 パラメータ:', {
      signal_type: decodedSignalType,
      signal_bin,
      trade_type,
      stock_code,
      profit_target_yen,
      loss_cut_yen,
      prev_close_gap_condition
    });

    // 1. D020から基本統計取得
    const baselineStatsQuery = `
      SELECT 
        signal_type,
        signal_bin,
        trade_type,
        stock_code,
        stock_name,
        total_samples,
        win_rate,
        avg_profit_rate
      FROM \`kabu-376213.kabu2411.D020_learning_stats\`
      WHERE signal_type = '${decodedSignalType}'
        AND signal_bin = ${signal_bin}
        AND trade_type = '${trade_type}'
        AND stock_code = '${stock_code}'
    `;

    console.log('🔍 D020基本統計取得中...');
    const baselineStatsResult = await bigquery.query(baselineStatsQuery);
    
    if (baselineStatsResult.length === 0) {
      return NextResponse.json({
        error: '指定された4軸の統計データが見つかりません'
      }, { status: 404 });
    }

    const baselineData = baselineStatsResult[0];
    const signal_info: SignalInfo = {
      signal_type: baselineData.signal_type,
      signal_bin: baselineData.signal_bin,
      trade_type: baselineData.trade_type,
      stock_code: baselineData.stock_code,
      stock_name: baselineData.stock_name
    };

    const baseline_stats: BaselineStats = {
      total_samples: baselineData.total_samples,
      win_rate: parseFloat(baselineData.win_rate.toFixed(1)),
      avg_profit_rate: parseFloat(baselineData.avg_profit_rate.toFixed(2))
    };

    // 2. D010から学習期間詳細データ取得
    const detailDataQuery = `
      SELECT 
        signal_date,
        prev_close,
        day_open,
        day_high,
        day_low,
        day_close,
        trading_volume,
        prev_close_to_open_gap,
        open_to_high_gap,
        open_to_low_gap,
        open_to_close_gap,
        baseline_profit_rate
      FROM \`kabu-376213.kabu2411.D010_basic_results\`
      WHERE signal_type = '${decodedSignalType}'
        AND signal_bin = ${signal_bin}
        AND trade_type = '${trade_type}'
        AND stock_code = '${stock_code}'
        AND signal_date <= '2024-06-30'  -- 学習期間のみ
      ORDER BY signal_date DESC
    `;

    console.log('📈 D010詳細データ取得中...');
    const detailDataResult = await bigquery.query(detailDataQuery);

    // 3. フィルタ適用と統計計算（共通ロジック使用）
    const detail_data: DetailData[] = [];
    
    // 各行に対してフィルタ適用
    detailDataResult.forEach((row: any) => {
      // 共通ロジックを使用してfiltered_profit_rateを計算
      const filtered_profit_rate = calculateFilteredProfitRate(
        {
          day_open: row.day_open,
          day_high: row.day_high,
          day_low: row.day_low,
          day_close: row.day_close,
          prev_close_to_open_gap: row.prev_close_to_open_gap,
          baseline_profit_rate: row.baseline_profit_rate
        },
        {
          trade_type: trade_type as 'BUY' | 'SELL',
          profit_target_yen: profit_target_yen,
          loss_cut_yen: loss_cut_yen,
          prev_close_gap_condition: prev_close_gap_condition as 'all' | 'above' | 'below'
        }
      );

      // 詳細データに追加
      detail_data.push({
        signal_date: row.signal_date.value,
        prev_close_to_open_gap: parseFloat(row.prev_close_to_open_gap.toFixed(2)),
        open_to_high_gap: parseFloat(row.open_to_high_gap.toFixed(2)),
        open_to_low_gap: parseFloat(row.open_to_low_gap.toFixed(2)),
        open_to_close_gap: parseFloat(row.open_to_close_gap.toFixed(2)),
        baseline_profit_rate: parseFloat(row.baseline_profit_rate.toFixed(2)),
        filtered_profit_rate: filtered_profit_rate,
        trading_volume: row.trading_volume,
        // 価格データを追加
        prev_close: row.prev_close,
        day_open: row.day_open,
        day_high: row.day_high,
        day_low: row.day_low,
        day_close: row.day_close
      });
    });

    // フィルタ後統計計算
    let filtered_stats: FilteredStats | undefined = undefined;
    
    if (profit_target_yen > 0 || loss_cut_yen > 0 || prev_close_gap_condition !== 'all') {
      // 共通ロジックを使用して統計計算
      const stats = calculateStats(detail_data, true); // true = 0を除外
      
      filtered_stats = {
        total_samples: stats.total_samples,
        win_rate: stats.win_rate,
        avg_profit_rate: stats.avg_profit_rate
      };
    }

    // レスポンス構築
    const response: ConfigResponse = {
      signal_info,
      baseline_stats,
      filtered_stats,
      detail_data
    };

    console.log('✅ チューニング画面データ取得完了');
    return NextResponse.json(response);

  } catch (error) {
    console.error('❌ チューニング画面APIエラー:', error);
    return NextResponse.json({
      error: 'チューニングデータの取得に失敗しました',
      details: error instanceof Error ? error.message : 'Unknown error'
    }, { status: 500 });
  }
}

// ✅ 申し送り書チェックリスト確認
// - D020から基本統計取得 ✅
// - D010から詳細データ取得（学習期間のみ） ✅
// - 動的フィルタ計算（利確・損切・ギャップ条件） ✅
// - フィルタ後統計の動的計算 ✅
// - BUY/SELL用語統一 ✅
// - URLデコード対応 ✅
// - パフォーマンス目標：3秒以内 ✅
// - 共通ロジック使用（filterLogic.ts） ✅