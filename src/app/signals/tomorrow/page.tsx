// src/app/signals/tomorrow/[stock_code]/[trade_type]/config/[signal_type]/[bin]/page.tsx
'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';

interface PageProps {
  params: Promise<{
    stock_code: string;
    trade_type: string;
    signal_type: string;
    bin: string;
  }>;
}

interface ConfigData {
  signal_info: {
    signal_type: string;
    signal_bin: number;
    trade_type: string;
    stock_code: string;
    stock_name: string;
  };
  baseline_stats: {
    total_samples: number;
    win_rate: number;
    avg_profit_rate: number;
    sharpe_ratio: number;
    median_profit_rate: number;
    max_profit_rate: number;
    min_profit_rate: number;
  };
  filtered_stats?: {
    total_samples: number;
    win_rate: number;
    avg_profit_rate: number;
    sharpe_ratio: number;
    median_profit_rate: number;
  };
  learning_data: Array<{
    signal_date: string;
    entry_price: number;
    exit_price: number;
    profit_rate: number;
    is_win: boolean;
  }>;
}

export default function ConfigPage({ params }: PageProps) {
  const router = useRouter();
  
  // ルートパラメータ
  const [routeParams, setRouteParams] = useState<{
    stock_code: string;
    trade_type: string;
    signal_type: string;
    bin: string;
  } | null>(null);
  
  // 設定値
  const [profitTarget, setProfitTarget] = useState(0);
  const [lossCut, setLossCut] = useState(0);
  
  // データとUI状態
  const [configData, setConfigData] = useState<ConfigData | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<'stats' | 'data'>('stats');

  // パラメータ解決
  useEffect(() => {
    const resolveParams = async () => {
      try {
        const resolvedParams = await params;
        if (resolvedParams.stock_code && 
            resolvedParams.trade_type && 
            resolvedParams.signal_type && 
            resolvedParams.bin) {
          setRouteParams(resolvedParams);
        } else {
          setError('URLパラメータが不正です');
          setLoading(false);
        }
      } catch (err) {
        setError('パラメータの解決に失敗しました');
        setLoading(false);
      }
    };
    
    resolveParams();
  }, [params]);

  // データ取得
  useEffect(() => {
    if (routeParams) {
      loadConfigData();
    }
  }, [routeParams, profitTarget, lossCut]);

  const loadConfigData = async () => {
    if (!routeParams) return;
    
    const { stock_code, trade_type, signal_type, bin } = routeParams;
    if (!stock_code || !trade_type || !signal_type || !bin) {
      setError('必要なパラメータが不足しています');
      setLoading(false);
      return;
    }
    
    try {
      setError(null);
      
      const queryParams = new URLSearchParams();
      if (profitTarget > 0) queryParams.set('profit_target_yen', profitTarget.toString());
      if (lossCut > 0) queryParams.set('loss_cut_yen', lossCut.toString());
      
      const encodedSignalType = encodeURIComponent(signal_type);
      const url = `/api/signals/tomorrow/${stock_code}/${trade_type}/config/${encodedSignalType}/${bin}?${queryParams}`;
      
      const response = await fetch(url);
      const data = await response.json();
      
      if (data.success && data.data) {
        setConfigData(data.data);
      } else {
        setError(data.error || 'データの取得に失敗しました');
      }
    } catch (err) {
      setError('ネットワークエラーが発生しました');
    } finally {
      setLoading(false);
    }
  };

  const handleSave = async () => {
    if (!routeParams) return;
    
    try {
      setSaving(true);
      
      const response = await fetch('/api/decisions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          signal_type: routeParams.signal_type,
          signal_bin: parseInt(routeParams.bin),
          trade_type: routeParams.trade_type,
          stock_code: routeParams.stock_code,
          profit_target_yen: profitTarget,
          loss_cut_yen: lossCut,
          additional_notes: `設定日時: ${new Date().toLocaleString()}`
        }),
      });
      
      const result = await response.json();
      
      if (result.success) {
        alert('設定が保存されました！');
        router.push('/signals/tomorrow');
      } else {
        setError(result.error || '保存に失敗しました');
      }
    } catch (err) {
      setError('保存中にエラーが発生しました');
      console.error('保存エラー:', err);
    } finally {
      setSaving(false);
    }
  };

  // 安全な数値表示
  const safeNumber = (value: number | undefined, decimals = 2): string => {
    if (typeof value !== 'number' || isNaN(value)) return '0.' + '0'.repeat(decimals);
    return value.toFixed(decimals);
  };

  // ローディング中（より厳密なチェック）
  if (loading || 
      !configData || 
      !configData.baseline_stats || 
      !configData.signal_info || 
      !routeParams ||
      typeof configData.baseline_stats.win_rate !== 'number' ||
      typeof configData.baseline_stats.avg_profit_rate !== 'number') {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-50">
        <div className="text-center">
          <div className="w-8 h-8 border-4 border-blue-600 border-t-transparent rounded-full animate-spin mx-auto mb-4"></div>
          <p className="text-gray-600">読み込み中...</p>
        </div>
      </div>
    );
  }

  // エラー表示
  if (error) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-50">
        <div className="max-w-md w-full bg-white rounded-lg shadow-lg p-6 text-center">
          <h2 className="text-xl font-semibold text-red-600 mb-4">エラー</h2>
          <p className="text-gray-700 mb-6">{error}</p>
          <div className="space-y-3">
            <button
              onClick={() => loadConfigData()}
              className="w-full bg-blue-600 text-white py-2 px-4 rounded hover:bg-blue-700"
            >
              再試行
            </button>
            <button
              onClick={() => router.push('/signals/tomorrow')}
              className="w-full bg-gray-300 text-gray-700 py-2 px-4 rounded hover:bg-gray-400"
            >
              一覧に戻る
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50 p-4">
      <div className="max-w-4xl mx-auto space-y-6">
        
        {/* ヘッダー */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h1 className="text-xl font-bold text-gray-900">4軸設定</h1>
              <div className="flex items-center space-x-2 text-sm text-gray-600 mt-1">
                <span>{configData.signal_info.signal_type}</span>
                <span>•</span>
                <span>Bin {configData.signal_info.signal_bin}</span>
                <span>•</span>
                <span className={`px-2 py-1 rounded text-xs font-medium ${
                  configData.signal_info.trade_type === 'LONG' ? 'bg-blue-100 text-blue-800' : 'bg-red-100 text-red-800'
                }`}>
                  {configData.signal_info.trade_type}
                </span>
                <span>•</span>
                <span>{configData.signal_info.stock_code} {configData.signal_info.stock_name}</span>
              </div>
            </div>
            <div className="flex space-x-2">
              <button
                onClick={() => router.push('/signals/tomorrow')}
                className="px-4 py-2 text-gray-600 bg-gray-100 rounded hover:bg-gray-200"
              >
                戻る
              </button>
              <button
                onClick={handleSave}
                disabled={saving}
                className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50"
              >
                {saving ? '保存中...' : '設定保存'}
              </button>
            </div>
          </div>
        </div>

        {/* 設定フォーム */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
          <h3 className="text-lg font-medium text-gray-900 mb-4">取引条件設定</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                利確目標（円）
              </label>
              <input
                type="number"
                min="0"
                max="1000"
                value={profitTarget}
                onChange={(e) => setProfitTarget(parseInt(e.target.value) || 0)}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                placeholder="0 = 設定なし"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                損切設定（円）
              </label>
              <input
                type="number"
                min="0"
                max="1000"
                value={lossCut}
                onChange={(e) => setLossCut(parseInt(e.target.value) || 0)}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                placeholder="0 = 設定なし"
              />
            </div>
          </div>
          
          <div className="mt-3 p-3 bg-blue-50 rounded text-sm">
            <strong className="text-blue-900">現在の設定: </strong>
            <span className="text-blue-700">
              {profitTarget === 0 && lossCut === 0 ? '純粋な寄り引け取引' :
               profitTarget > 0 && lossCut === 0 ? `利確${profitTarget}円のみ` :
               profitTarget === 0 && lossCut > 0 ? `損切${lossCut}円のみ` :
               `利確${profitTarget}円・損切${lossCut}円`}
            </span>
          </div>
        </div>

        {/* 統計表示 */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
          <h3 className="text-lg font-medium text-gray-900 mb-4">統計情報</h3>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            
            {/* ベースライン統計 */}
            <div className="bg-gray-50 rounded-lg p-4">
              <h4 className="font-medium text-gray-900 mb-3">フィルタ前（全データ）</h4>
              <div className="space-y-2 text-sm">
                <div className="flex justify-between">
                  <span>サンプル数:</span>
                  <span className="font-medium">{configData.baseline_stats.total_samples}件</span>
                </div>
                <div className="flex justify-between">
                  <span>勝率:</span>
                  <span className="font-medium">{safeNumber(configData.baseline_stats.win_rate, 1)}%</span>
                </div>
                <div className="flex justify-between">
                  <span>平均利益率:</span>
                  <span className="font-medium">{configData.baseline_stats.avg_profit_rate >= 0 ? '+' : ''}{safeNumber(configData.baseline_stats.avg_profit_rate)}%</span>
                </div>
                <div className="flex justify-between">
                  <span>シャープレシオ:</span>
                  <span className="font-medium">{safeNumber(configData.baseline_stats.sharpe_ratio, 3)}</span>
                </div>
                <div className="flex justify-between">
                  <span>最大利益:</span>
                  <span className="font-medium text-green-600">+{safeNumber(configData.baseline_stats.max_profit_rate)}%</span>
                </div>
                <div className="flex justify-between">
                  <span>最大損失:</span>
                  <span className="font-medium text-red-600">{safeNumber(configData.baseline_stats.min_profit_rate)}%</span>
                </div>
              </div>
            </div>

            {/* フィルタ後統計 */}
            {configData.filtered_stats && (
              <div className="bg-blue-50 rounded-lg p-4">
                <h4 className="font-medium text-blue-900 mb-3">フィルタ後</h4>
                <div className="space-y-2 text-sm">
                  <div className="flex justify-between">
                    <span>サンプル数:</span>
                    <span className="font-medium">{configData.filtered_stats.total_samples}件</span>
                  </div>
                  <div className="flex justify-between">
                    <span>勝率:</span>
                    <span className="font-medium text-blue-900">{safeNumber(configData.filtered_stats.win_rate, 1)}%</span>
                  </div>
                  <div className="flex justify-between">
                    <span>平均利益率:</span>
                    <span className="font-medium text-blue-900">{configData.filtered_stats.avg_profit_rate >= 0 ? '+' : ''}{safeNumber(configData.filtered_stats.avg_profit_rate)}%</span>
                  </div>
                  <div className="flex justify-between">
                    <span>シャープレシオ:</span>
                    <span className="font-medium text-blue-900">{safeNumber(configData.filtered_stats.sharpe_ratio, 3)}</span>
                  </div>
                  <div className="flex justify-between">
                    <span>差分:</span>
                    <span className={`font-medium ${
                      (configData.filtered_stats.avg_profit_rate - configData.baseline_stats.avg_profit_rate) >= 0 
                        ? 'text-green-600' : 'text-red-600'
                    }`}>
                      {(configData.filtered_stats.avg_profit_rate - configData.baseline_stats.avg_profit_rate) >= 0 ? '+' : ''}
                      {safeNumber(configData.filtered_stats.avg_profit_rate - configData.baseline_stats.avg_profit_rate)}%
                    </span>
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>

        {/* タブ表示 */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200">
          <div className="border-b border-gray-200">
            <nav className="flex space-x-8 px-6">
              <button
                onClick={() => setActiveTab('stats')}
                className={`py-4 px-1 border-b-2 font-medium text-sm ${
                  activeTab === 'stats'
                    ? 'border-blue-500 text-blue-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700'
                }`}
              >
                詳細統計
              </button>
              <button
                onClick={() => setActiveTab('data')}
                className={`py-4 px-1 border-b-2 font-medium text-sm ${
                  activeTab === 'data'
                    ? 'border-blue-500 text-blue-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700'
                }`}
              >
                取引データ（上位50件）
              </button>
            </nav>
          </div>

          <div className="p-6">
            {activeTab === 'stats' && (
              <div className="text-center text-gray-500">
                統計の詳細表示機能は実装予定です
              </div>
            )}

            {activeTab === 'data' && (
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">取引日</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">エントリー</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">エグジット</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">損益率</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">結果</th>
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                    {configData.learning_data.slice(0, 50).map((record, index) => (
                      <tr key={index}>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                          {new Date(record.signal_date).toLocaleDateString()}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                          ¥{Math.round(record.entry_price).toLocaleString()}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                          ¥{Math.round(record.exit_price).toLocaleString()}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm">
                          <span className={record.profit_rate >= 0 ? 'text-green-600' : 'text-red-600'}>
                            {record.profit_rate >= 0 ? '+' : ''}{safeNumber(record.profit_rate)}%
                          </span>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          <span className={`px-2 py-1 text-xs font-semibold rounded-full ${
                            record.is_win ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'
                          }`}>
                            {record.is_win ? '勝' : '負'}
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}