// src/app/signals/tomorrow/[stock_code]/[trade_type]/page.tsx
'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';

interface BinDetail {
  bin: number;
  win_rate: number;
  avg_profit_rate: number;
  sample_count: number;
  sharpe_ratio: number;
  is_excellent: boolean;
  is_tomorrow: boolean;
}

interface SignalTypeBinData {
  signal_type: string;
  signal_category: string;
  description: string;
  bins: BinDetail[];
  tomorrow_bins: number[];
}

interface BinSelectionData {
  stock_code: string;
  stock_name: string;
  trade_type: 'BUY' | 'SELL';
  target_date: string;
  signal_types: SignalTypeBinData[];
}

interface SelectedBin {
  signal_type: string;
  bin: number;
  win_rate: number;
  avg_profit_rate: number;
  sample_count: number;
}

interface PageProps {
  params: Promise<{
    stock_code: string;
    trade_type: string;
  }>;
}

export default function BinSelectionPage({ params }: PageProps) {
  const router = useRouter();
  
  // 状態管理
  const [stockCode, setStockCode] = useState<string>('');
  const [tradeType, setTradeType] = useState<string>('');
  const [data, setData] = useState<BinSelectionData | null>(null);
  const [activeTab, setActiveTab] = useState<string>('');
  const [selectedBins, setSelectedBins] = useState<Set<string>>(new Set());
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // パラメータ取得
  useEffect(() => {
    async function loadParams() {
      const resolvedParams = await params;
      setStockCode(resolvedParams.stock_code);
      setTradeType(resolvedParams.trade_type.toUpperCase());
    }
    loadParams();
  }, [params]);

  // データ取得
  useEffect(() => {
    if (stockCode && tradeType) {
      fetchBinSelectionData();
    }
  }, [stockCode, tradeType]);

  // 初期タブ設定
  useEffect(() => {
    if (data?.signal_types && data.signal_types.length > 0 && !activeTab) {
      // noUncheckedIndexedAccess対応: 明示的な非nullアサーション
      setActiveTab(data.signal_types[0]!.signal_type);
    }
  }, [data, activeTab]);

  const fetchBinSelectionData = async () => {
    try {
      setLoading(true);
      setError(null);
      
      const response = await fetch(`/api/signals/tomorrow/${stockCode}/${tradeType}/details`);
      const result = await response.json();
      
      if (result.success && result.data) {
        setData(result.data);
      } else {
        setError(result.error || 'データの取得に失敗しました');
      }
    } catch (err) {
      setError('ネットワークエラーが発生しました');
      console.error('bin選択データ取得エラー:', err);
    } finally {
      setLoading(false);
    }
  };

  const handleBinToggle = (signalType: string, bin: number) => {
    const binKey = `${signalType}_${bin}`;
    const newSelectedBins = new Set(selectedBins);
    
    if (selectedBins.has(binKey)) {
      newSelectedBins.delete(binKey);
    } else {
      newSelectedBins.add(binKey);
    }
    
    setSelectedBins(newSelectedBins);
  };

  // 型安全なヘルパー関数
  const createSelectedBinsList = (validData: BinSelectionData): SelectedBin[] => {
    const selected: SelectedBin[] = [];
    
    Array.from(selectedBins).forEach(binKey => {
      const parts = binKey.split('_');
      if (parts.length < 2) return;
      
      const signalType = parts[0];
      const binStr = parts[1];
      
      if (!signalType || !binStr) return;
      
      const bin = parseInt(binStr);
      if (isNaN(bin)) return;
      
      const signalTypeData = validData.signal_types.find(st => st.signal_type === signalType);
      const binData = signalTypeData?.bins.find(b => b.bin === bin);
      
      if (binData) {
        selected.push({
          signal_type: signalType,
          bin: bin,
          win_rate: binData.win_rate,
          avg_profit_rate: binData.avg_profit_rate,
          sample_count: binData.sample_count
        });
      }
    });
    
    return selected.sort((a, b) => {
      if (a.signal_type !== b.signal_type) {
        return a.signal_type.localeCompare(b.signal_type);
      }
      return a.bin - b.bin;
    });
  };

  const getSelectedBinsList = (): SelectedBin[] => {
    return data ? createSelectedBinsList(data) : [];
  };

  const handleStartConfiguration = () => {
    const selectedList = getSelectedBinsList();
    if (selectedList.length === 0) {
      alert('bin を選択してください');
      return;
    }
    
    const first = selectedList[0];
    if (!first) {
      alert('選択されたbinが見つかりません');
      return;
    }
    
    const configUrl = `/signals/tomorrow/${stockCode}/${tradeType}/config/${first.signal_type}/${first.bin}`;
    
    sessionStorage.setItem('selectedBins', JSON.stringify(selectedList));
    sessionStorage.setItem('currentBinIndex', '0');
    
    router.push(configUrl);
  };

  const getBinStatusBadge = (bin: BinDetail) => {
    let className = "inline-flex items-center px-2 py-1 rounded text-xs font-medium ";
    
    if (bin.is_tomorrow) {
      className += "bg-yellow-100 text-yellow-800";
    } else if (bin.is_excellent) {
      className += "bg-green-100 text-green-800";
    } else {
      className += "bg-gray-100 text-gray-800";
    }
    
    return className;
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-64">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500"></div>
        <div className="ml-4">
          <p className="text-lg font-medium text-gray-900">読み込み中...</p>
          <p className="text-sm text-gray-500">bin選択データを取得しています</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-4">
        <div className="flex">
          <div className="flex-shrink-0">
            <svg className="h-5 w-5 text-red-400" fill="currentColor" viewBox="0 0 20 20">
              <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" />
            </svg>
          </div>
          <div className="ml-3">
            <h3 className="text-sm font-medium text-red-800">エラーが発生しました</h3>
            <div className="mt-2 text-sm text-red-700">
              <p>{error}</p>
            </div>
            <div className="mt-4 flex space-x-2">
              <button
                onClick={fetchBinSelectionData}
                className="bg-red-100 text-red-800 px-3 py-1 rounded text-sm hover:bg-red-200"
              >
                再試行
              </button>
              <button
                onClick={() => router.push('/signals/tomorrow')}
                className="bg-gray-100 text-gray-800 px-3 py-1 rounded text-sm hover:bg-gray-200"
              >
                一覧に戻る
              </button>
            </div>
          </div>
        </div>
      </div>
    );
  }

  if (!data) {
    return (
      <div className="text-center py-8">
        <p className="text-gray-500">データを読み込んでいます...</p>
      </div>
    );
  }

  const activeSignalType = data.signal_types.find(st => st.signal_type === activeTab);

  return (
    <div className="space-y-6">
      {/* ヘッダー */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold text-gray-900">📊 bin選択</h1>
            <p className="text-gray-600 mt-1">
              {data.stock_code} {data.stock_name} ({data.trade_type}) - {data.target_date}
            </p>
            <p className="text-sm text-blue-600 mt-1">
              明日発火するシグナルタイプ: {data.signal_types.length}個
            </p>
          </div>
          <button
            onClick={() => router.push('/signals/tomorrow')}
            className="px-4 py-2 text-gray-600 bg-gray-100 rounded-md hover:bg-gray-200"
          >
            ← 一覧に戻る
          </button>
        </div>
      </div>

      {/* シグナルタイプタブ（固定ヘッダー） */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 sticky top-0 z-10">
        <div className="border-b border-gray-200">
          <nav className="-mb-px flex space-x-8 px-6">
            {data.signal_types.map((signalType) => (
              <button
                key={signalType.signal_type}
                onClick={() => setActiveTab(signalType.signal_type)}
                className={`py-4 px-1 border-b-2 font-medium text-sm ${
                  activeTab === signalType.signal_type
                    ? 'border-blue-500 text-blue-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                }`}
              >
                {signalType.signal_type}
                {signalType.tomorrow_bins.length > 0 && (
                  <span className="ml-2 inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-yellow-100 text-yellow-800">
                    ★{signalType.tomorrow_bins.length}
                  </span>
                )}
              </button>
            ))}
          </nav>
        </div>

        {/* bin選択エリア */}
        {activeSignalType && (
          <div>
            {/* シグナルタイプ情報（コンパクト） */}
            <div className="px-6 py-3 bg-gray-50 border-b border-gray-200">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-3">
                  <h3 className="text-lg font-semibold text-gray-900">
                    {activeSignalType.signal_type}
                  </h3>
                  <span className="text-sm text-gray-600">
                    {activeSignalType.description}
                  </span>
                  <span className="text-sm text-blue-600 font-medium">
                    明日発火: Bin {activeSignalType.tomorrow_bins.join(', ')}
                  </span>
                </div>
              </div>
            </div>

            {/* bin一覧テーブル（5個表示でスクロール） */}
            <div className="relative">
              <div className="overflow-auto" style={{ height: '320px' }}>
                <table className="min-w-full divide-y divide-gray-200">
                  <thead className="bg-gray-50 sticky top-0 z-20">
                    <tr>
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider bg-gray-50">
                        選択
                      </th>
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider bg-gray-50">
                        Bin
                      </th>
                      <th className="px-3 py-2 text-right text-xs font-medium text-gray-500 uppercase tracking-wider bg-gray-50">
                        勝率 (%)
                      </th>
                      <th className="px-3 py-2 text-right text-xs font-medium text-gray-500 uppercase tracking-wider bg-gray-50">
                        期待値 (%)
                      </th>
                      <th className="px-3 py-2 text-right text-xs font-medium text-gray-500 uppercase tracking-wider bg-gray-50">
                        サンプル数
                      </th>
                      <th className="px-3 py-2 text-center text-xs font-medium text-gray-500 uppercase tracking-wider bg-gray-50">
                        ステータス
                      </th>
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                    {activeSignalType.bins.map((bin) => {
                      const binKey = `${activeSignalType.signal_type}_${bin.bin}`;
                      const isSelected = selectedBins.has(binKey);
                      
                      return (
                        <tr 
                          key={bin.bin} 
                          className={`hover:bg-gray-50 cursor-pointer ${
                            isSelected ? 'bg-blue-50' : ''
                          } ${
                            bin.is_tomorrow ? 'border-l-4 border-yellow-400' : ''
                          }`}
                          onClick={() => handleBinToggle(activeSignalType.signal_type, bin.bin)}
                        >
                          <td className="px-3 py-2 whitespace-nowrap">
                            <input
                              type="checkbox"
                              checked={isSelected}
                              onChange={() => handleBinToggle(activeSignalType.signal_type, bin.bin)}
                              className="h-4 w-4 text-blue-600 rounded border-gray-300"
                              onClick={(e) => e.stopPropagation()}
                            />
                          </td>
                          <td className="px-3 py-2 whitespace-nowrap">
                            <div className="flex items-center">
                              <span className="font-medium text-gray-900">
                                Bin {bin.bin}
                              </span>
                              {bin.is_tomorrow && (
                                <span className="ml-2 text-yellow-600 text-sm font-bold">★</span>
                              )}
                            </div>
                          </td>
                          <td className="px-3 py-2 whitespace-nowrap text-right">
                            <span className={`font-medium ${
                              bin.win_rate >= 55 ? 'text-green-600' : 'text-gray-900'
                            }`}>
                              {bin.win_rate.toFixed(1)}
                            </span>
                          </td>
                          <td className="px-3 py-2 whitespace-nowrap text-right">
                            <span className={`font-medium ${
                              bin.avg_profit_rate >= 0.5 ? 'text-green-600' : 
                              bin.avg_profit_rate >= 0 ? 'text-gray-900' : 'text-red-600'
                            }`}>
                              {bin.avg_profit_rate > 0 ? '+' : ''}{bin.avg_profit_rate.toFixed(2)}
                            </span>
                          </td>
                          <td className="px-3 py-2 whitespace-nowrap text-right">
                            <span className={`text-sm ${
                              bin.sample_count >= 30 ? 'text-gray-900' : 'text-gray-500'
                            }`}>
                              {bin.sample_count}回
                            </span>
                          </td>
                          <td className="px-3 py-2 whitespace-nowrap text-center">
                            <span className={getBinStatusBadge(bin)}>
                              {bin.is_tomorrow ? '明日発火' : bin.is_excellent ? '優秀' : '標準'}
                            </span>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}
      </div>

      {/* 選択中のbin */}
      {selectedBins.size > 0 && (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h3 className="text-lg font-medium text-gray-900 mb-4">
            選択中のbin ({selectedBins.size}個)
          </h3>
          
          <div className="space-y-2">
            {getSelectedBinsList().map((selected) => (
              <div
                key={`${selected.signal_type}_${selected.bin}`}
                className="flex items-center justify-between p-3 bg-gray-50 rounded-lg"
              >
                <div className="flex items-center space-x-4">
                  <span className="font-medium">
                    {selected.signal_type} / Bin{selected.bin}
                  </span>
                  <span className="text-sm text-gray-600">
                    勝率{selected.win_rate}% / 期待値{selected.avg_profit_rate > 0 ? '+' : ''}{selected.avg_profit_rate}%
                  </span>
                </div>
                <button
                  onClick={() => handleBinToggle(selected.signal_type, selected.bin)}
                  className="text-red-600 hover:text-red-800"
                >
                  ✕
                </button>
              </div>
            ))}
          </div>
          
          <div className="mt-6 flex justify-end">
            <button
              onClick={handleStartConfiguration}
              className="bg-blue-600 text-white px-6 py-3 rounded-md hover:bg-blue-700 font-medium"
            >
              選択したbinの条件設定を開始 →
            </button>
          </div>
        </div>
      )}
      {/* 選択中のbin */}
      {selectedBins.size > 0 && (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h3 className="text-lg font-medium text-gray-900 mb-4">
            選択中のbin ({selectedBins.size}個)
          </h3>
          
          <div className="space-y-2">
            {getSelectedBinsList().map((selected) => (
              <div
                key={`${selected.signal_type}_${selected.bin}`}
                className="flex items-center justify-between p-3 bg-gray-50 rounded-lg"
              >
                <div className="flex items-center space-x-4">
                  <span className="font-medium">
                    {selected.signal_type} / Bin{selected.bin}
                  </span>
                  <span className="text-sm text-gray-600">
                    勝率{selected.win_rate}% / 期待値{selected.avg_profit_rate > 0 ? '+' : ''}{selected.avg_profit_rate}%
                  </span>
                </div>
                <button
                  onClick={() => handleBinToggle(selected.signal_type, selected.bin)}
                  className="text-red-600 hover:text-red-800"
                >
                  ✕
                </button>
              </div>
            ))}
          </div>
          
          <div className="mt-6 flex justify-end">
            <button
              onClick={handleStartConfiguration}
              className="bg-blue-600 text-white px-6 py-3 rounded-md hover:bg-blue-700 font-medium"
            >
              選択したbinの条件設定を開始 →
            </button>
          </div>
        </div>
      )}
    </div>
  );
}