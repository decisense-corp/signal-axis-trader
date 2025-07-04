// src/app/api/signals/verification/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { BigQueryClient } from '@/lib/bigquery';

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
      parseFloat(searchParams.get('profit_target_yen')!) : null;
    const loss_cut_yen = searchParams.get('loss_cut_yen') ? 
      parseFloat(searchParams.get('loss_cut_yen')!) : null;
    const prev_close_gap_condition = searchParams.get('prev_close_gap_condition') || 'all';

    // パラメータのバリデーション
    if (!signal_type || !signal_bin || !trade_type || !stock_code) {
      return NextResponse.json(
        { error: 'Missing required parameters' },
        { status: 400 }
      );
    }

    const bigquery = new BigQueryClient();

    // 1. 学習期間統計をD020から取得
    const learningStatsQuery = `
      SELECT 
        stock_name,
        total_samples,
        win_rate,
        avg_profit_rate
      FROM \`kabu-376213.kabu2411.D020_learning_stats\`
      WHERE signal_type = '${signal_type}'
        AND signal_bin = ${parseInt(signal_bin)}
        AND trade_type = '${trade_type}'
        AND stock_code = '${stock_code}'
    `;

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
      WHERE signal_type = '${signal_type}'
        AND signal_bin = ${parseInt(signal_bin)}
        AND trade_type = '${trade_type}'
        AND stock_code = '${stock_code}'
        AND signal_date > '2024-06-30'
        AND signal_date <= '2025-07-03'
      ORDER BY signal_date DESC
    `;

    const verificationDataResult = await bigquery.query(verificationDataQuery);

    const verificationData = verificationDataResult;

    // 3. 学習期間のフィルタ後統計を計算（D010から）
    let learningFilteredStats: StatsSummary = {
      total_samples: 0,
      win_rate: 0,
      avg_profit_rate: 0
    };

    if (profit_target_yen !== null || loss_cut_yen !== null || prev_close_gap_condition !== 'all') {
      const learningFilteredQuery = `
        SELECT 
          signal_date,
          prev_close,
          day_open,
          prev_close_to_open_gap,
          open_to_high_gap,
          open_to_low_gap,
          baseline_profit_rate
        FROM \`kabu-376213.kabu2411.D010_basic_results\`
        WHERE signal_type = '${signal_type}'
          AND signal_bin = ${parseInt(signal_bin)}
          AND trade_type = '${trade_type}'
          AND stock_code = '${stock_code}'
          AND signal_date <= '2024-06-30'
      `;

      const learningFilteredResult = await bigquery.query(learningFilteredQuery);

      const filteredResults = learningFilteredResult.filter(row => {
        // ギャップ条件のチェック
        if (prev_close_gap_condition !== 'all') {
          const gap = row.prev_close_to_open_gap;
          if (prev_close_gap_condition === 'above' && gap <= 0) return false;
          if (prev_close_gap_condition === 'below' && gap >= 0) return false;
        }
        return true;
      }).map(row => {
        // filtered_profit_rate を計算
        let filtered_profit_rate = row.baseline_profit_rate;
        
        if (profit_target_yen || loss_cut_yen) {
          if (trade_type === 'BUY') {
            // BUYの場合
            if (profit_target_yen && row.open_to_high_gap * row.day_open / 100 >= profit_target_yen) {
              filtered_profit_rate = (profit_target_yen / row.day_open) * 100;
            } else if (loss_cut_yen && row.open_to_low_gap * row.day_open / 100 <= -loss_cut_yen) {
              filtered_profit_rate = -(loss_cut_yen / row.day_open) * 100;
            }
          } else {
            // SELLの場合
            if (profit_target_yen && -row.open_to_low_gap * row.day_open / 100 >= profit_target_yen) {
              filtered_profit_rate = (profit_target_yen / row.day_open) * 100;
            } else if (loss_cut_yen && -row.open_to_high_gap * row.day_open / 100 <= -loss_cut_yen) {
              filtered_profit_rate = -(loss_cut_yen / row.day_open) * 100;
            }
          }
        }
        
        return { ...row, filtered_profit_rate };
      });

      if (filteredResults.length > 0) {
        learningFilteredStats = {
          total_samples: filteredResults.length,
          win_rate: Math.round(filteredResults.filter(r => r.filtered_profit_rate > 0).length * 100 / filteredResults.length * 10) / 10,
          avg_profit_rate: Math.round(filteredResults.reduce((sum, r) => sum + r.filtered_profit_rate, 0) / filteredResults.length * 100) / 100
        };
      }
    }

    // 4. 検証期間の統計を計算（ベースラインとフィルタ後）
    const verificationBaselineStats: StatsSummary = {
      total_samples: verificationData.length,
      win_rate: 0,
      avg_profit_rate: 0
    };

    const verificationFilteredStats: StatsSummary = {
      total_samples: 0,
      win_rate: 0,
      avg_profit_rate: 0
    };

    if (verificationData.length > 0) {
      // ベースライン統計
      verificationBaselineStats.win_rate = Math.round(
        verificationData.filter(r => r.baseline_profit_rate > 0).length * 100 / verificationData.length * 10
      ) / 10;
      verificationBaselineStats.avg_profit_rate = Math.round(
        verificationData.reduce((sum, r) => sum + r.baseline_profit_rate, 0) / verificationData.length * 100
      ) / 100;

      // フィルタ後の統計
      const filteredVerificationData = verificationData.filter(row => {
        if (prev_close_gap_condition !== 'all') {
          const gap = row.prev_close_to_open_gap;
          if (prev_close_gap_condition === 'above' && gap <= 0) return false;
          if (prev_close_gap_condition === 'below' && gap >= 0) return false;
        }
        return true;
      }).map(row => {
        let filtered_profit_rate = row.baseline_profit_rate;
        
        if (profit_target_yen || loss_cut_yen) {
          if (trade_type === 'BUY') {
            if (profit_target_yen && row.open_to_high_gap * row.day_open / 100 >= profit_target_yen) {
              filtered_profit_rate = (profit_target_yen / row.day_open) * 100;
            } else if (loss_cut_yen && row.open_to_low_gap * row.day_open / 100 <= -loss_cut_yen) {
              filtered_profit_rate = -(loss_cut_yen / row.day_open) * 100;
            }
          } else {
            if (profit_target_yen && -row.open_to_low_gap * row.day_open / 100 >= profit_target_yen) {
              filtered_profit_rate = (profit_target_yen / row.day_open) * 100;
            } else if (loss_cut_yen && -row.open_to_high_gap * row.day_open / 100 <= -loss_cut_yen) {
              filtered_profit_rate = -(loss_cut_yen / row.day_open) * 100;
            }
          }
        }
        
        return { ...row, filtered_profit_rate };
      });

      if (filteredVerificationData.length > 0) {
        verificationFilteredStats.total_samples = filteredVerificationData.length;
        verificationFilteredStats.win_rate = Math.round(
          filteredVerificationData.filter(r => r.filtered_profit_rate > 0).length * 100 / filteredVerificationData.length * 10
        ) / 10;
        verificationFilteredStats.avg_profit_rate = Math.round(
          filteredVerificationData.reduce((sum, r) => sum + r.filtered_profit_rate, 0) / filteredVerificationData.length * 100
        ) / 100;
      }
    }

    // 5. 検証期間詳細データにfiltered_profit_rateを追加
    const verificationDetailData = verificationData.map(row => {
      let filtered_profit_rate = row.baseline_profit_rate;
      
      // ギャップ条件でフィルタアウトされる場合は0
      if (prev_close_gap_condition !== 'all') {
        const gap = row.prev_close_to_open_gap;
        if ((prev_close_gap_condition === 'above' && gap <= 0) ||
            (prev_close_gap_condition === 'below' && gap >= 0)) {
          filtered_profit_rate = 0;
        }
      }
      
      // 利確・損切条件の適用
      if (filtered_profit_rate !== 0 && (profit_target_yen || loss_cut_yen)) {
        if (trade_type === 'BUY') {
          if (profit_target_yen && row.open_to_high_gap * row.day_open / 100 >= profit_target_yen) {
            filtered_profit_rate = (profit_target_yen / row.day_open) * 100;
          } else if (loss_cut_yen && row.open_to_low_gap * row.day_open / 100 <= -loss_cut_yen) {
            filtered_profit_rate = -(loss_cut_yen / row.day_open) * 100;
          }
        } else {
          if (profit_target_yen && -row.open_to_low_gap * row.day_open / 100 >= profit_target_yen) {
            filtered_profit_rate = (profit_target_yen / row.day_open) * 100;
          } else if (loss_cut_yen && -row.open_to_high_gap * row.day_open / 100 <= -loss_cut_yen) {
            filtered_profit_rate = -(loss_cut_yen / row.day_open) * 100;
          }
        }
      }
      
      return {
        signal_date: row.signal_date.value,
        prev_close_to_open_gap: Math.round(row.prev_close_to_open_gap * 100) / 100,
        open_to_high_gap: Math.round(row.open_to_high_gap * 100) / 100,
        open_to_low_gap: Math.round(row.open_to_low_gap * 100) / 100,
        open_to_close_gap: Math.round(row.open_to_close_gap * 100) / 100,
        baseline_profit_rate: Math.round(row.baseline_profit_rate * 100) / 100,
        filtered_profit_rate: Math.round(filtered_profit_rate * 100) / 100,
        trading_volume: Math.round(row.trading_volume)
      };
    });

    // レスポンスの構築
    const response = {
      signal_info: {
        signal_type,
        signal_bin: parseInt(signal_bin),
        trade_type,
        stock_code,
        stock_name: learningStats.stock_name
      },
      filter_conditions: {
        profit_target_yen,
        loss_cut_yen,
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

    return NextResponse.json(response);

  } catch (error) {
    console.error('Verification API error:', error);
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    );
  }
}