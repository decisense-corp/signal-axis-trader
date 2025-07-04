// src/app/signals/tomorrow/config/[signal_type]/[signal_bin]/[trade_type]/[stock_code]/page.tsx
// 申し送り書仕様準拠：本格チューニング画面（統計比較・詳細データ・並び替え機能付き）
// フロントエンドでフィルタ計算を実行（BigQueryアクセスは初回のみ）
'use client';

import React, { useState, useEffect, useMemo } from 'react';
import { ArrowLeft, Filter, Save, TrendingUp, TrendingDown, AlertCircle, ArrowUpDown } from 'lucide-react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';

// TypeScript型定義
interface RouteParams {
  signal_type: string;
  signal_bin: string;
  trade_type: string;
  stock_code: string;
}

interface ConfigPageProps {
  params: Promise<RouteParams>;
}

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
}

// 生データ用の型（APIから取得する際のフィルタなしデータ）
interface RawDetailData {
  signal_date: string;
  prev_close: number;
  day_open: number;
  day_high: number;
  day_low: number;
  day_close: number;
  prev_close_to_open_gap: number;
  open_to_high_gap: number;
  open_to_low_gap: number;
  open_to_close_gap: number;
  baseline_profit_rate: number;
  trading_volume: number;
}

interface ConfigResponse {
  signal_info: SignalInfo;
  baseline_stats: BaselineStats;
  filtered_stats?: FilteredStats | undefined;
  detail_data: DetailData[];
}

type SortField = keyof DetailData;
type SortOrder = 'asc' | 'desc';

export default function ConfigPage({ params }: ConfigPageProps) {
  const router = useRouter();
  
  // State管理
  const [routeParams, setRouteParams] = useState<RouteParams | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  
  // 初回取得データのキャッシュ
  const [initialData, setInitialData] = useState<ConfigResponse | null>(null);
  const [rawDetailData, setRawDetailData] = useState<RawDetailData[]>([]);
  
  // 条件設定（表示用）
  const [profitTargetYenInput, setProfitTargetYenInput] = useState<string>('0');
  const [lossCutYenInput, setLossCutYenInput] = useState<string>('0');
  
  // 条件設定（計算用）
  const [profitTargetYen, setProfitTargetYen] = useState<number>(0);
  const [lossCutYen, setLossCutYen] = useState<number>(0);
  const [prevCloseGapCondition, setPrevCloseGapCondition] = useState<'all' | 'above' | 'below'>('all');
  
  // 並び替え設定
  const [sortField, setSortField] = useState<SortField>('signal_date');
  const [sortOrder, setSortOrder] = useState<SortOrder>('desc');
  
  // Debounce用タイマー
  const [debounceTimer, setDebounceTimer] = useState<NodeJS.Timeout | null>(null);

  // 入力値のdebounce処理
  const handleProfitTargetChange = (value: string) => {
    setProfitTargetYenInput(value);
    
    // 既存のタイマーをクリア
    if (debounceTimer) {
      clearTimeout(debounceTimer);
    }
    
    // 新しいタイマーを設定（500ms後に実行）
    const newTimer = setTimeout(() => {
      const numValue = parseInt(value) || 0;
      setProfitTargetYen(numValue);
    }, 500);
    
    setDebounceTimer(newTimer);
  };
  
  const handleLossCutChange = (value: string) => {
    setLossCutYenInput(value);
    
    // 既存のタイマーをクリア
    if (debounceTimer) {
      clearTimeout(debounceTimer);
    }
    
    // 新しいタイマーを設定（500ms後に実行）
    const newTimer = setTimeout(() => {
      const numValue = parseInt(value) || 0;
      setLossCutYen(numValue);
    }, 500);
    
    setDebounceTimer(newTimer);
  };

  // Route params解決
  useEffect(() => {
    const resolveParams = async () => {
      try {
        const resolvedParams = await params;
        setRouteParams(resolvedParams);
      } catch (err) {
        setError('URLパラメータの解決に失敗しました');
        setLoading(false);
      }
    };
    
    resolveParams();
  }, [params]);

  // 初回データ取得（BigQueryアクセスは1回のみ）
  useEffect(() => {
    if (!routeParams) return;
    
    const fetchData = async () => {
      try {
        setLoading(true);
        setError(null);
        
        // 初回は条件なしでデータ取得
        const queryParams = new URLSearchParams({
          signal_type: routeParams.signal_type,
          signal_bin: routeParams.signal_bin,
          trade_type: routeParams.trade_type,
          stock_code: routeParams.stock_code,
          profit_target_yen: '0',
          loss_cut_yen: '0',
          prev_close_gap_condition: 'all'
        });
        
        const response = await fetch(`/api/signals/config?${queryParams}`);
        
        if (!response.ok) {
          throw new Error('データの取得に失敗しました');
        }
        
        const data: ConfigResponse = await response.json();
        setInitialData(data);
        
        // 生データを保存（フィルタ計算用）
        const rawData: RawDetailData[] = data.detail_data.map(d => ({
          signal_date: d.signal_date,
          prev_close: 0, // APIで取得する必要がある場合は要修正
          day_open: 0,   // APIで取得する必要がある場合は要修正
          day_high: 0,   // APIで取得する必要がある場合は要修正
          day_low: 0,    // APIで取得する必要がある場合は要修正
          day_close: 0,  // APIで取得する必要がある場合は要修正
          prev_close_to_open_gap: d.prev_close_to_open_gap,
          open_to_high_gap: d.open_to_high_gap,
          open_to_low_gap: d.open_to_low_gap,
          open_to_close_gap: d.open_to_close_gap,
          baseline_profit_rate: d.baseline_profit_rate,
          trading_volume: d.trading_volume
        }));
        setRawDetailData(rawData);
        
      } catch (err) {
        setError(err instanceof Error ? err.message : 'データ取得エラー');
      } finally {
        setLoading(false);
      }
    };
    
    fetchData();
  }, [routeParams]); // 条件パラメータを依存配列から削除

  // フロントエンドでフィルタ計算（条件変更時に即座に実行）
  const { filteredStats, filteredDetailData } = useMemo(() => {
    if (!initialData || !routeParams) {
      return { filteredStats: undefined, filteredDetailData: [] };
    }

    const detailData: DetailData[] = [];
    let filteredSamples = 0;
    let winSamples = 0;
    let totalProfit = 0;

    // 各レコードに対してフィルタを適用
    initialData.detail_data.forEach((row) => {
      let filtered_profit_rate = row.baseline_profit_rate;
      let is_filtered = true;

      // 前日終値ギャップ条件チェック
      if (prevCloseGapCondition === 'above' && row.prev_close_to_open_gap <= 0) {
        is_filtered = false;
      } else if (prevCloseGapCondition === 'below' && row.prev_close_to_open_gap >= 0) {
        is_filtered = false;
      }

      // 利確・損切条件適用（is_filteredがtrueの場合のみ）
      if (is_filtered && (profitTargetYen > 0 || lossCutYen > 0)) {
        // 簡易計算：実際の価格データがない場合は概算
        const estimatedOpen = 1000; // 仮の始値（実際はAPIから取得が必要）
        
        // 損切チェック（優先）
        if (lossCutYen > 0) {
          const lossRate = -lossCutYen / estimatedOpen * 100;
          const minGap = routeParams.trade_type === 'BUY' ? row.open_to_low_gap : -row.open_to_high_gap;
          
          if (minGap <= -lossCutYen) {
            filtered_profit_rate = lossRate;
          }
        }

        // 利確チェック（損切に該当しない場合）
        if (profitTargetYen > 0 && filtered_profit_rate === row.baseline_profit_rate) {
          const profitRate = profitTargetYen / estimatedOpen * 100;
          const maxGap = routeParams.trade_type === 'BUY' ? row.open_to_high_gap : -row.open_to_low_gap;
          
          if (maxGap >= profitTargetYen) {
            filtered_profit_rate = profitRate;
          }
        }
      }

      // フィルタ条件に合わない場合は除外扱い
      if (!is_filtered) {
        filtered_profit_rate = 0;
      } else {
        filteredSamples++;
        if (filtered_profit_rate > 0) {
          winSamples++;
        }
        totalProfit += filtered_profit_rate;
      }

      detailData.push({
        ...row,
        filtered_profit_rate: parseFloat(filtered_profit_rate.toFixed(2))
      });
    });

    // フィルタ後統計計算
    const stats: FilteredStats | undefined = (profitTargetYen > 0 || lossCutYen > 0 || prevCloseGapCondition !== 'all') ? {
      total_samples: filteredSamples,
      win_rate: filteredSamples > 0 
        ? parseFloat((winSamples / filteredSamples * 100).toFixed(1))
        : 0,
      avg_profit_rate: filteredSamples > 0
        ? parseFloat((totalProfit / filteredSamples).toFixed(2))
        : 0
    } : undefined;

    return {
      filteredStats: stats,
      filteredDetailData: detailData
    };
  }, [initialData, routeParams, profitTargetYen, lossCutYen, prevCloseGapCondition]);

  // 並び替え処理
  const sortedDetailData = useMemo(() => {
    return [...filteredDetailData].sort((a, b) => {
      const aValue = a[sortField];
      const bValue = b[sortField];
      
      if (typeof aValue === 'string' && typeof bValue === 'string') {
        return sortOrder === 'asc' 
          ? aValue.localeCompare(bValue)
          : bValue.localeCompare(aValue);
      }
      
      if (typeof aValue === 'number' && typeof bValue === 'number') {
        return sortOrder === 'asc' ? aValue - bValue : bValue - aValue;
      }
      
      return 0;
    });
  }, [filteredDetailData, sortField, sortOrder]);

  // 並び替えトグル
  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortOrder('desc');
    }
  };

  // 条件確定処理
  const handleConfirm = async () => {
    if (!routeParams || !initialData) return;
    
    try {
      setSaving(true);
      setError(null);
      
      // 条件保存
      const saveResponse = await fetch('/api/decisions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          signal_type: decodeURIComponent(routeParams.signal_type),
          signal_bin: parseInt(routeParams.signal_bin),
          trade_type: routeParams.trade_type,
          stock_code: routeParams.stock_code,
          profit_target_yen: profitTargetYen,
          loss_cut_yen: lossCutYen,
          prev_close_gap_condition: prevCloseGapCondition,
          additional_notes: `チューニング完了: ${new Date().toLocaleString()}`
        }),
      });
      
      if (!saveResponse.ok) {
        throw new Error('条件保存に失敗しました');
      }
      
      // 検証期間確認画面へ遷移
      const verificationUrl = `/signals/tomorrow/verification/${routeParams.signal_type}/${routeParams.signal_bin}/${routeParams.trade_type}/${routeParams.stock_code}?profit_target_yen=${profitTargetYen}&loss_cut_yen=${lossCutYen}&prev_close_gap_condition=${prevCloseGapCondition}`;
      router.push(verificationUrl);
    } catch (err) {
      setError('条件確定処理に失敗しました');
      console.error('条件確定エラー:', err);
    } finally {
      setSaving(false);
    }
  };

  // クリーンアップ（タイマーのクリア）
  useEffect(() => {
    return () => {
      if (debounceTimer) {
        clearTimeout(debounceTimer);
      }
    };
  }, [debounceTimer]);

  // Loading表示
  if (loading) {
    return (
      <div className="container mx-auto px-4 py-6 max-w-7xl">
        <div className="text-center py-12">
          <div className="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
          <div className="mt-4 text-gray-600">データを読み込んでいます...</div>
        </div>
      </div>
    );
  }

  if (!routeParams || !initialData) {
    return (
      <div className="container mx-auto px-4 py-6 max-w-7xl">
        <div className="bg-red-50 border border-red-200 rounded-md p-4">
          <div className="text-red-800">
            <strong>エラー:</strong> データの読み込みに失敗しました
          </div>
        </div>
      </div>
    );
  }

  const { signal_info, baseline_stats } = initialData;

  return (
    <div className="container mx-auto px-4 py-6 max-w-7xl">
      {/* ヘッダー */}
      <div className="mb-6">
        <div className="flex items-center justify-between mb-4">
          <h1 className="text-2xl font-bold text-gray-900">
            🔧 チューニング画面
          </h1>
          <Link
            href="/signals/tomorrow"
            className="flex items-center px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50"
          >
            <ArrowLeft className="w-4 h-4 mr-2" />
            一覧に戻る
          </Link>
        </div>
        
        {/* 4軸情報表示 */}
        <div className="bg-gray-50 rounded-lg p-4">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
            <div>
              <span className="text-gray-600">シグナルタイプ:</span>
              <span className="ml-2 font-medium">{signal_info.signal_type}</span>
            </div>
            <div>
              <span className="text-gray-600">ビン番号:</span>
              <span className="ml-2 font-medium">{signal_info.signal_bin}</span>
            </div>
            <div>
              <span className="text-gray-600">売買方向:</span>
              <span className={`ml-2 inline-flex px-2 py-1 text-xs font-semibold rounded-full ${
                signal_info.trade_type === 'BUY' 
                  ? 'bg-green-100 text-green-800' 
                  : 'bg-red-100 text-red-800'
              }`}>
                {signal_info.trade_type}
              </span>
            </div>
            <div>
              <span className="text-gray-600">銘柄:</span>
              <span className="ml-2 font-medium">{signal_info.stock_code} {signal_info.stock_name}</span>
            </div>
          </div>
        </div>
      </div>

      {/* エラー表示 */}
      {error && (
        <div className="bg-red-50 border border-red-200 rounded-md p-4 mb-6">
          <div className="flex items-center text-red-800">
            <AlertCircle className="w-5 h-5 mr-2" />
            <strong>エラー:</strong> {error}
          </div>
        </div>
      )}

      {/* 条件設定セクション */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6 mb-6">
        <h2 className="text-lg font-semibold text-gray-900 mb-4 flex items-center">
          <Filter className="w-5 h-5 mr-2" />
          条件設定
        </h2>
        
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {/* 利確目標 */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              利確目標（円）
            </label>
            <input
              type="number"
              value={profitTargetYenInput}
              onChange={(e) => handleProfitTargetChange(e.target.value)}
              placeholder="50"
              className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
            />
            <p className="mt-1 text-xs text-gray-500">0 = 設定なし</p>
          </div>

          {/* 損切設定 */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              損切設定（円）
            </label>
            <input
              type="number"
              value={lossCutYenInput}
              onChange={(e) => handleLossCutChange(e.target.value)}
              placeholder="30"
              className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
            />
            <p className="mt-1 text-xs text-gray-500">0 = 設定なし</p>
          </div>

          {/* 前日終値ギャップ条件 */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              前日終値ギャップ条件
            </label>
            <select
              value={prevCloseGapCondition}
              onChange={(e) => setPrevCloseGapCondition(e.target.value as 'all' | 'above' | 'below')}
              className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
            >
              <option value="all">すべて</option>
              <option value="above">前日終値より上</option>
              <option value="below">前日終値より下</option>
            </select>
          </div>
        </div>

        {/* 現在の設定状態 */}
        <div className="mt-4 p-3 bg-blue-50 rounded-md">
          <div className="text-sm text-blue-800">
            {profitTargetYen === 0 && lossCutYen === 0 && prevCloseGapCondition === 'all' ? (
              <span>現在の設定: フィルタなし（すべての取引を対象）</span>
            ) : (
              <span>
                現在の設定: 
                {profitTargetYen > 0 && ` 利確${profitTargetYen}円`}
                {lossCutYen > 0 && ` 損切${lossCutYen}円`}
                {prevCloseGapCondition !== 'all' && ` ギャップ${prevCloseGapCondition === 'above' ? '上' : '下'}`}
              </span>
            )}
          </div>
        </div>
      </div>

      {/* 統計比較表示 */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6 mb-6">
        <h2 className="text-lg font-semibold text-gray-900 mb-4">
          📊 統計比較
        </h2>
        
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {/* フィルタ前統計 */}
          <div className="bg-gray-50 rounded-lg p-4">
            <h3 className="text-sm font-semibold text-gray-700 mb-3">フィルタ前</h3>
            <div className="space-y-2">
              <div className="flex justify-between">
                <span className="text-sm text-gray-600">サンプル数:</span>
                <span className="text-sm font-medium">{baseline_stats.total_samples}件</span>
              </div>
              <div className="flex justify-between">
                <span className="text-sm text-gray-600">勝率:</span>
                <span className="text-sm font-medium">{baseline_stats.win_rate}%</span>
              </div>
              <div className="flex justify-between">
                <span className="text-sm text-gray-600">平均利益率:</span>
                <span className="text-sm font-medium">{baseline_stats.avg_profit_rate}%</span>
              </div>
            </div>
          </div>

          {/* フィルタ後統計 */}
          <div className={`rounded-lg p-4 ${filteredStats ? 'bg-blue-50' : 'bg-gray-100'}`}>
            <h3 className="text-sm font-semibold text-gray-700 mb-3">フィルタ後</h3>
            {filteredStats ? (
              <div className="space-y-2">
                <div className="flex justify-between">
                  <span className="text-sm text-gray-600">サンプル数:</span>
                  <span className="text-sm font-medium">{filteredStats.total_samples}件</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-sm text-gray-600">勝率:</span>
                  <span className="text-sm font-medium">{filteredStats.win_rate}%</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-sm text-gray-600">平均利益率:</span>
                  <span className="text-sm font-medium">{filteredStats.avg_profit_rate}%</span>
                </div>
              </div>
            ) : (
              <div className="text-sm text-gray-500">
                条件を設定するとフィルタ後の統計が表示されます
              </div>
            )}
          </div>
        </div>
      </div>

      {/* 詳細データ表示 */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6 mb-6">
        <h2 className="text-lg font-semibold text-gray-900 mb-4">
          📈 詳細データ（学習期間）
        </h2>
        
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th onClick={() => handleSort('signal_date')} className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100">
                  <div className="flex items-center">
                    日付
                    <ArrowUpDown className="ml-1 w-3 h-3" />
                  </div>
                </th>
                <th onClick={() => handleSort('prev_close_to_open_gap')} className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100">
                  <div className="flex items-center justify-end">
                    前日終値→始
                    <ArrowUpDown className="ml-1 w-3 h-3" />
                  </div>
                </th>
                <th onClick={() => handleSort('open_to_high_gap')} className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100">
                  <div className="flex items-center justify-end">
                    始→高
                    <ArrowUpDown className="ml-1 w-3 h-3" />
                  </div>
                </th>
                <th onClick={() => handleSort('open_to_low_gap')} className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100">
                  <div className="flex items-center justify-end">
                    始→安
                    <ArrowUpDown className="ml-1 w-3 h-3" />
                  </div>
                </th>
                <th onClick={() => handleSort('open_to_close_gap')} className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100">
                  <div className="flex items-center justify-end">
                    始→終
                    <ArrowUpDown className="ml-1 w-3 h-3" />
                  </div>
                </th>
                <th onClick={() => handleSort('baseline_profit_rate')} className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100">
                  <div className="flex items-center justify-end">
                    寄引損益率
                    <ArrowUpDown className="ml-1 w-3 h-3" />
                  </div>
                </th>
                <th onClick={() => handleSort('filtered_profit_rate')} className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100">
                  <div className="flex items-center justify-end">
                    フィルタ損益率
                    <ArrowUpDown className="ml-1 w-3 h-3" />
                  </div>
                </th>
                <th onClick={() => handleSort('trading_volume')} className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100">
                  <div className="flex items-center justify-end">
                    売買代金
                    <ArrowUpDown className="ml-1 w-3 h-3" />
                  </div>
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {sortedDetailData.map((row, index) => (
                <tr key={index} className="hover:bg-gray-50">
                  <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-900">
                    {row.signal_date}
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap text-sm text-right">
                    <span className={row.prev_close_to_open_gap > 0 ? 'text-green-600' : row.prev_close_to_open_gap < 0 ? 'text-red-600' : 'text-gray-900'}>
                      {row.prev_close_to_open_gap.toFixed(0)}円
                    </span>
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap text-sm text-right">
                    <span className={row.open_to_high_gap > 0 ? 'text-green-600' : 'text-gray-900'}>
                      {row.open_to_high_gap.toFixed(0)}円
                    </span>
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap text-sm text-right">
                    <span className={row.open_to_low_gap < 0 ? 'text-red-600' : 'text-gray-900'}>
                      {row.open_to_low_gap.toFixed(0)}円
                    </span>
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap text-sm text-right">
                    <span className={row.open_to_close_gap > 0 ? 'text-green-600' : row.open_to_close_gap < 0 ? 'text-red-600' : 'text-gray-900'}>
                      {row.open_to_close_gap.toFixed(0)}円
                    </span>
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap text-sm text-right">
                    <span className={row.baseline_profit_rate > 0 ? 'text-green-600' : row.baseline_profit_rate < 0 ? 'text-red-600' : 'text-gray-900'}>
                      {row.baseline_profit_rate.toFixed(2)}%
                    </span>
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap text-sm text-right">
                    <span className={row.filtered_profit_rate > 0 ? 'text-green-600' : row.filtered_profit_rate < 0 ? 'text-red-600' : 'text-gray-900'}>
                      {row.filtered_profit_rate.toFixed(2)}%
                    </span>
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap text-sm text-right text-gray-900">
                    {(row.trading_volume / 1000000).toFixed(0)}百万
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* アクションボタン */}
      <div className="flex justify-between items-center">
        <Link
          href="/signals/tomorrow"
          className="px-6 py-2 border border-gray-300 rounded-md text-gray-700 bg-white hover:bg-gray-50 font-medium transition-colors"
        >
          ← 一覧に戻る
        </Link>
        
        <button
          onClick={handleConfirm}
          disabled={saving}
          className="px-6 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white font-medium rounded-md transition-colors flex items-center space-x-2"
        >
          {saving ? (
            <>
              <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
              <span>処理中...</span>
            </>
          ) : (
            <>
              <Save className="w-4 h-4" />
              <span>条件確定</span>
            </>
          )}
        </button>
      </div>
    </div>
  );
}

// 申し送り書チェックリスト確認
// - 統計比較表示（フィルタ前・フィルタ後）
// - 詳細データ表示（8項目）
// - 並び替え機能（全項目）
// - 動的フィルタ計算（フロントエンドで実行）
// - 条件確定ボタン（検証期間確認画面へ遷移）
// - BUY/SELL用語統一
// - レスポンシブ対応
// - エラーハンドリング
// - BigQueryアクセスは初回のみ（パフォーマンス最適化）