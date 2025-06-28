 // ============================================================================
// 基本型定義
// ============================================================================

/**
 * 取引対象銘柄マスタ
 */
export interface TradingStock {
  stock_code: string;
  company_name: string;
  market_code: string;
  market_name: string;
  sector17_code: string;
  sector17_name: string;
  trading_days: number;
  avg_trading_value_million: number;
  avg_intraday_volatility: number;
  volatility_decile: number;
}

/**
 * 4軸パフォーマンス統計
 */
export interface SignalPerformance4Axis {
  signal_type: string;
  signal_bin: number;
  trade_type: 'Buy' | 'Sell';
  stock_code: string;
  stock_name: string;
  total_count: number;
  win_count: number;
  win_rate: number;
  avg_profit_rate: number;
  median_profit_rate: number;
  std_profit_rate: number;
  sharpe_ratio: number;
  max_profit_rate: number;
  min_profit_rate: number;
  last_30d_count: number;
  last_30d_win_rate: number;
  last_30d_avg_profit: number;
  last_signal_date: string;
  last_updated: string;
}

/**
 * 明日のシグナル候補
 */
export interface TomorrowSignalCandidate {
  stock_code: string;
  stock_name: string;
  trade_type: 'Buy' | 'Sell';
  max_win_rate: number;
  max_expected_value: number;
  excellent_pattern_count: number;
  processing_status: string;
}

/**
 * シグナルタイプマスタ
 */
export interface SignalType {
  signal_type: string;
  signal_category: string;
  description: string;
  calculation_method: string;
  priority_rank: number;
  is_active: boolean;
  is_score_type: boolean;
}

/**
 * BigQuery レスポンス基本型
 */
export interface BigQueryResponse<T = any> {
  success: boolean;
  data?: T;
  error?: string;
  message?: string;
  timestamp?: string;
}

/**
 * API レスポンス基本型
 */
export interface ApiResponse<T = any> {
  success: boolean;
  data?: T;
  error?: string;
  message?: string;
  timestamp: string;
}

/**
 * 処理状況
 */
export type ProcessingStatus = 
  | '済（対象あり）'    // 処理完了 & 明日取引予定
  | '済（対象なし）'    // 処理完了 & 取引ルールなし
  | '未（対象あり）'    // 未処理だが過去ルールで明日取引予定
  | '未（対象なし）';   // 未処理 & 過去ルールもなし

/**
 * 取引タイプ
 */
export type TradeType = 'Buy' | 'Sell';

/**
 * シグナル区分（1-20）
 */
export type SignalBin = 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 | 15 | 16 | 17 | 18 | 19 | 20;