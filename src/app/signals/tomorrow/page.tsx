// src/app/signals/tomorrow/page.tsx
'use client';

import { useState, useEffect } from 'react';

interface TomorrowSignalCandidate {
  stock_code: string;
  stock_name: string;
  trade_type: 'Buy' | 'Sell';
  max_win_rate: number;
  max_expected_return: number;
  excellent_pattern_count: number;
  processing_status: string;
  total_samples: number;
  avg_win_rate: number;
  avg_expected_return: number;
}

interface ApiResponse {
  success: boolean;
  data?: TomorrowSignalCandidate[];
  error?: string;
  pagination?: {
    total: number;
    limit: number;
    offset: number;
    hasMore: boolean;
  };
  metadata?: {
    query_time: string;
    target_date: string;
  };
}

export default function TomorrowSignalsPage() {
  const [signals, setSignals] = useState<TomorrowSignalCandidate[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [metadata, setMetadata] = useState<any>(null);

  useEffect(() => {
    fetchSignals();
  }, []);

  const fetchSignals = async () => {
    try {
      setLoading(true);
      setError(null);
      
      const response = await fetch('/api/signals/tomorrow');
      const data: ApiResponse = await response.json();
      
      if (data.success && data.data) {
        setSignals(data.data);
        setMetadata(data.metadata);
      } else {
        setError(data.error || 'データの取得に失敗しました');
      }
    } catch (err) {
      setError('ネットワークエラーが発生しました');
      console.error('API呼び出しエラー:', err);
    } finally {
      setLoading(false);
    }
  };

  const getStatusBadge = (status: string) => {
    const baseClasses = "inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium";
    
    switch (status) {
      case '未処理':
        return `${baseClasses} bg-yellow-100 text-yellow-800`;
      case '済（対象あり）':
        return `${baseClasses} bg-green-100 text-green-800`;
      case '済（対象なし）':
        return `${baseClasses} bg-gray-100 text-gray-800`;
      default:
        return `${baseClasses} bg-gray-100 text-gray-800`;
    }
  };

  const getTradeTypeBadge = (tradeType: 'Buy' | 'Sell') => {
    const baseClasses = "inline-flex items-center px-2 py-1 rounded text-xs font-medium";
    
    if (tradeType === 'Buy') {
      return `${baseClasses} bg-blue-100 text-blue-800`;
    } else {
      return `${baseClasses} bg-red-100 text-red-800`;
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-64">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500"></div>
        <div className="ml-4">
          <p className="text-lg font-medium text-gray-900">読み込み中...</p>
          <p className="text-sm text-gray-500">明日のシグナル候補を取得しています</p>
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
            <h3 className="text-sm font-medium text-red-800">
              エラーが発生しました
            </h3>
            <div className="mt-2 text-sm text-red-700">
              <p>{error}</p>
            </div>
            <div className="mt-4">
              <button
                onClick={fetchSignals}
                className="bg-red-100 text-red-800 px-3 py-1 rounded text-sm hover:bg-red-200"
              >
                再試行
              </button>
            </div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* サマリー情報 */}
      {metadata && (
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
          <h3 className="text-lg font-medium text-blue-900 mb-2">
            📅 明日のシグナル候補 ({metadata.target_date})
          </h3>
          <p className="text-sm text-blue-700">
            取得件数: <span className="font-medium">{signals.length}件</span>
            {metadata.query_time && (
              <span className="ml-4">
                更新時刻: {new Date(metadata.query_time).toLocaleString('ja-JP')}
              </span>
            )}
          </p>
        </div>
      )}

      {/* データテーブル */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
        <div className="px-6 py-4 border-b border-gray-200">
          <h3 className="text-lg font-medium text-gray-900">
            シグナル候補一覧 (勝率・期待値良い順)
          </h3>
        </div>
        
        {signals.length === 0 ? (
          <div className="px-6 py-8 text-center">
            <p className="text-gray-500">明日のシグナル候補がありません</p>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    銘柄コード
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    銘柄名
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    売買
                  </th>
                  <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                    最高勝率
                  </th>
                  <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                    最高期待値
                  </th>
                  <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                    優秀パターン数
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    処理状況
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    アクション
                  </th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {signals.map((signal, index) => (
                  <tr key={`${signal.stock_code}-${signal.trade_type}`} className="hover:bg-gray-50">
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                      {signal.stock_code}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                      {signal.stock_name}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <span className={getTradeTypeBadge(signal.trade_type)}>
                        {signal.trade_type}
                      </span>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900 text-right">
                      {signal.max_win_rate.toFixed(1)}%
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900 text-right">
                      {signal.max_expected_return > 0 ? '+' : ''}{signal.max_expected_return.toFixed(1)}%
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900 text-right">
                      {signal.excellent_pattern_count}個
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <span className={getStatusBadge(signal.processing_status)}>
                        {signal.processing_status}
                      </span>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium">
                      <button
                        className="text-blue-600 hover:text-blue-900"
                        onClick={() => {
                          // TODO: 個別設定画面への遷移
                          alert(`${signal.stock_code} (${signal.trade_type}) の設定画面に遷移`);
                        }}
                      >
                        設定
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}