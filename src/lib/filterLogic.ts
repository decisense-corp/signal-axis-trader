 // /lib/filterLogic.ts
// Signal Axis Trader 共通フィルタ計算ロジック
// 価格ベースで利確・損切を判定する統一ロジック

/**
 * 価格データの型定義
 */
export interface PriceData {
  day_open: number;
  day_high: number;
  day_low: number;
  day_close: number;
  prev_close_to_open_gap: number;
  baseline_profit_rate: number;
}

/**
 * フィルタ条件の型定義
 */
export interface FilterConditions {
  trade_type: 'BUY' | 'SELL';
  profit_target_yen: number;
  loss_cut_yen: number;
  prev_close_gap_condition: 'all' | 'above' | 'below';
}

/**
 * フィルタ適用後の利益率を計算
 * 
 * @param data 価格データ
 * @param conditions フィルタ条件
 * @returns フィルタ適用後の利益率（%）
 */
export function calculateFilteredProfitRate(
  data: PriceData,
  conditions: FilterConditions
): number {
  const { trade_type, profit_target_yen, loss_cut_yen, prev_close_gap_condition } = conditions;
  const { day_open, day_high, day_low, baseline_profit_rate, prev_close_to_open_gap } = data;
  
  let filtered_profit_rate = baseline_profit_rate;
  let is_filtered = true;

  // ステップ1: 前日終値ギャップ条件チェック
  if (prev_close_gap_condition !== 'all') {
    if (prev_close_gap_condition === 'above' && prev_close_to_open_gap <= 0) {
      // ギャップアップのみ許可するが、ギャップダウンまたはフラット
      is_filtered = false;
    } else if (prev_close_gap_condition === 'below' && prev_close_to_open_gap >= 0) {
      // ギャップダウンのみ許可するが、ギャップアップまたはフラット
      is_filtered = false;
    }
  }

  // フィルタ条件に合わない場合は0を返す（統計から除外）
  if (!is_filtered) {
    return 0;
  }

  // ステップ2: 利確・損切条件の適用（ギャップ条件を通過した場合のみ）
  if (profit_target_yen > 0 || loss_cut_yen > 0) {
    
    if (trade_type === 'BUY') {
      // BUYの場合：始値で買う
      
      // 損切チェック（優先）
      if (loss_cut_yen > 0) {
        const loss_cut_price = day_open - loss_cut_yen;
        if (day_low <= loss_cut_price) {
          // 安値が損切価格以下 → 損切発動
          filtered_profit_rate = -(loss_cut_yen / day_open) * 100;
          return parseFloat(filtered_profit_rate.toFixed(2));
        }
      }
      
      // 利確チェック（損切に該当しない場合）
      if (profit_target_yen > 0) {
        const profit_target_price = day_open + profit_target_yen;
        if (day_high >= profit_target_price) {
          // 高値が利確価格以上 → 利確発動
          filtered_profit_rate = (profit_target_yen / day_open) * 100;
          return parseFloat(filtered_profit_rate.toFixed(2));
        }
      }
      
    } else if (trade_type === 'SELL') {
      // SELLの場合：始値で売る（空売り）
      
      // 損切チェック（優先）
      if (loss_cut_yen > 0) {
        const loss_cut_price = day_open + loss_cut_yen;
        if (day_high >= loss_cut_price) {
          // 高値が損切価格以上 → 損切発動
          filtered_profit_rate = -(loss_cut_yen / day_open) * 100;
          return parseFloat(filtered_profit_rate.toFixed(2));
        }
      }
      
      // 利確チェック（損切に該当しない場合）
      if (profit_target_yen > 0) {
        const profit_target_price = day_open - profit_target_yen;
        if (day_low <= profit_target_price) {
          // 安値が利確価格以下 → 利確発動
          filtered_profit_rate = (profit_target_yen / day_open) * 100;
          return parseFloat(filtered_profit_rate.toFixed(2));
        }
      }
    }
  }

  // どの条件にも該当しない場合は、baseline_profit_rateをそのまま返す
  return parseFloat(filtered_profit_rate.toFixed(2));
}

/**
 * 統計情報の型定義
 */
export interface StatsResult {
  total_samples: number;
  win_rate: number;
  avg_profit_rate: number;
}

/**
 * フィルタ適用後の統計を計算
 * 
 * @param data 詳細データ配列
 * @param excludeZero 0を除外するか（デフォルト: true）
 * @returns 統計情報
 */
export function calculateStats(
  data: Array<{ filtered_profit_rate: number }>,
  excludeZero: boolean = true
): StatsResult {
  // 0を除外する場合はフィルタリング
  const samples = excludeZero 
    ? data.filter(d => d.filtered_profit_rate !== 0)
    : data;
  
  // サンプルがない場合
  if (samples.length === 0) {
    return {
      total_samples: 0,
      win_rate: 0,
      avg_profit_rate: 0
    };
  }
  
  // 勝ちサンプル（プラスの利益率）をカウント
  const win_samples = samples.filter(d => d.filtered_profit_rate > 0);
  
  // 合計利益率を計算
  const total_profit = samples.reduce((sum, d) => sum + d.filtered_profit_rate, 0);
  
  return {
    total_samples: samples.length,
    win_rate: parseFloat((win_samples.length / samples.length * 100).toFixed(1)),
    avg_profit_rate: parseFloat((total_profit / samples.length).toFixed(2))
  };
}