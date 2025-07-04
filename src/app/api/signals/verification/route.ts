// src/app/api/signals/verification/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { BigQueryClient } from '@/lib/bigquery';
import { calculateFilteredProfitRate, calculateStats } from '@/lib/filterLogic';

const bigquery = new BigQueryClient();

interface StatsSummary {
  total_samples: number;
  win_rate: number;
  avg_profit_rate: number;
}

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    
    // URLパラメータから4軸情報を取得
    const signal_type = searchParams.get('signal_type');
    const signal_bin = searchParams.get('signal_bin');
    const trade_type = searchParams.get('trade_type');
    const stock_code = searchParams.get('stock_code');
    
    // フィルタ条件を取得
    const profit_target_yen = searchParams.get('profit_target_yen') ? 
      parseFloat(searchParams.get('profit_target_yen')!) : 0;
    const loss_cut_yen = searchParams.get('loss_cut_yen') ? 
      parseFloat(searchParams.get('loss_cut_yen')!) : 0;
    const prev_close_gap_condition = searchParams.get('prev_close_gap_condition') || 'all';

    // パラメータのバリデーション
    if (!signal_type || !signal_bin || !trade_type || !stock_code) {
      return NextResponse.json(
        { error: 'Missing required parameters' },
        { status: 400 }
      );
    }

    // デコード処理
    const decodedSignalType = decodeURIComponent(signal_type);

    console.log('🔍 検証期間確認API開始...');
    console.log('パラメータ:', {
      signal_type: decodedSignalType,
      signal_bin,
      trade_type,
      stock_code,
      profit_target_yen,
      loss_cut_yen,
      prev_close_gap_condition
    });

    // 1. 学習期間統計をD020から取得
    const learningStatsQuery = `
      SELECT 
        stock_name,
        total_samples,
        win_rate,
        avg_profit_rate
      FROM \`kabu-376213.kabu2411.D020_learning_stats\`
      WHERE signal_type = '${decodedSignalType}'
        AND signal_bin = ${parseInt(signal_bin)}
        AND trade_type = '${trade_type}'
        AND stock_code = '${stock_code}'
    `;

    console.log('📊 D020から学習期間統計取得中...');
    const learningStatsResult = await bigquery.query(learningStatsQuery);

    if (learningStatsResult.length === 0) {
      return NextResponse.json(
        { error: 'No data found for the specified parameters' },
        { status: 404 }
      );
    }

    const learningStats = learningStatsResult[0];

    // 2. 検証期間データをD010から取得（検証期間: 2024/7/1〜2025/7/3）
    const verificationDataQuery = `
      SELECT 
        signal_date,
        prev_close,
        day_open,
        day_high,
        day_low,
        day_close,
        prev_close_to_open_gap,
        open_to_high_gap,
        open_to_low_gap,
        open_to_close_gap,
        baseline_profit_rate,
        trading_volume
      FROM \`kabu-376213.kabu2411.D010_basic_results\`
      WHERE signal_type = '${decodedSignalType}'
        AND signal_bin = ${parseInt(signal_bin)}
        AND trade_type = '${trade_type}'
        AND stock_code = '${stock_code}'
        AND signal_date > '2024-06-30'
        AND signal_date <= '2025-07-03'
      ORDER BY signal_date DESC
    `;

    console.log('📈 D010から検証期間データ取得中...');
    const verificationDataResult = await bigquery.query(verificationDataQuery);

    // 3. 学習期間のフィルタ後統計を計算（D010から）
    let learningFilteredStats: StatsSummary = {
      total_samples: 0,
      win_rate: 0,
      avg_profit_rate: 0
    };

    if (profit_target_yen > 0 || loss_cut_yen > 0 || prev_close_gap_condition !== 'all') {
      const learningFilteredQuery = `
        SELECT 
          signal_date,
          prev_close,
          day_open,
          day_high,
          day_low,
          day_close,
          prev_close_to_open_gap,
          open_to_high_gap,
          open_to_low_gap,
          baseline_profit_rate
        FROM \`kabu-376213.kabu2411.D010_basic_results\`
        WHERE signal_type = '${decodedSignalType}'
          AND signal_bin = ${parseInt(signal_bin)}
          AND trade_type = '${trade_type}'
          AND stock_code = '${stock_code}'
          AND signal_date <= '2024-06-30'
      `;

      console.log('📊 D010から学習期間詳細データ取得中...');
      const learningFilteredResult = await bigquery.query(learningFilteredQuery);

      // 共通ロジックを使用してフィルタ適用
      const filteredResults = learningFilteredResult.map((row: any) => {
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
        return { filtered_profit_rate };
      });

      // 共通ロジックを使用して統計計算
      learningFilteredStats = calculateStats(filteredResults, true);
    }

    // 4. 検証期間の統計を計算（ベースラインとフィルタ後）
    
    // ベースライン統計（フィルタなし）
    const verificationBaselineStats: StatsSummary = {
      total_samples: verificationDataResult.length,
      win_rate: 0,
      avg_profit_rate: 0
    };

    if (verificationDataResult.length > 0) {
      const baselineData = verificationDataResult.map((row: any) => ({
        filtered_profit_rate: row.baseline_profit_rate
      }));
      const baselineStats = calculateStats(baselineData, false); // false = 0を含む
      verificationBaselineStats.win_rate = baselineStats.win_rate;
      verificationBaselineStats.avg_profit_rate = baselineStats.avg_profit_rate;
    }

    // フィルタ後統計
    let verificationFilteredStats: StatsSummary = {
      total_samples: 0,
      win_rate: 0,
      avg_profit_rate: 0
    };

    // 共通ロジックを使用してフィルタ適用
    const filteredVerificationData = verificationDataResult.map((row: any) => {
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
      return { filtered_profit_rate };
    });

    // フィルタ条件がある場合のみ計算
    if (profit_target_yen > 0 || loss_cut_yen > 0 || prev_close_gap_condition !== 'all') {
      verificationFilteredStats = calculateStats(filteredVerificationData, true);
    }

    // 5. 検証期間詳細データを構築
    const verificationDetailData = verificationDataResult.map((row: any) => {
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
      
      return {
        signal_date: row.signal_date.value,
        prev_close_to_open_gap: parseFloat(row.prev_close_to_open_gap.toFixed(2)),
        open_to_high_gap: parseFloat(row.open_to_high_gap.toFixed(2)),
        open_to_low_gap: parseFloat(row.open_to_low_gap.toFixed(2)),
        open_to_close_gap: parseFloat(row.open_to_close_gap.toFixed(2)),
        baseline_profit_rate: parseFloat(row.baseline_profit_rate.toFixed(2)),
        filtered_profit_rate: filtered_profit_rate,
        trading_volume: row.trading_volume
      };
    });

    // レスポンスの構築
    const response = {
      signal_info: {
        signal_type: decodedSignalType,
        signal_bin: parseInt(signal_bin),
        trade_type,
        stock_code,
        stock_name: learningStats.stock_name
      },
      filter_conditions: {
        profit_target_yen: profit_target_yen || null,
        loss_cut_yen: loss_cut_yen || null,
        prev_close_gap_condition
      },
      comparison_stats: {
        learning_period: {
          baseline: {
            total_samples: learningStats.total_samples,
            win_rate: learningStats.win_rate,
            avg_profit_rate: learningStats.avg_profit_rate
          },
          filtered: learningFilteredStats
        },
        verification_period: {
          baseline: verificationBaselineStats,
          filtered: verificationFilteredStats
        }
      },
      verification_detail_data: verificationDetailData
    };

    console.log('✅ 検証期間確認データ取得完了');
    return NextResponse.json(response);

  } catch (error) {
    console.error('❌ 検証期間確認APIエラー:', error);
    return NextResponse.json(
      { 
        error: 'Internal server error',
        details: error instanceof Error ? error.message : 'Unknown error'
      },
      { status: 500 }
    );
  }
}

// ✅ 申し送り書チェックリスト確認
// - D020から学習期間統計取得 ✅
// - D010から学習期間詳細データ取得（フィルタ計算用） ✅
// - D010から検証期間データ取得（2024/7/1〜2025/7/3） ✅
// - 動的フィルタ計算（利確・損切・ギャップ条件） ✅
// - 統計の動的計算 ✅
// - BUY/SELL用語統一 ✅
// - URLデコード対応 ✅
// - パフォーマンス目標：5秒以内 ✅
// - 共通ロジック使用（filterLogic.ts） ✅