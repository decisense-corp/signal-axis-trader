// src/app/signals/tomorrow/page.tsx
'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';

interface TomorrowSignalCandidate {
  stock_code: string;
  stock_name: string;
  trade_type: 'BUY' | 'SELL';
  max_win_rate: number;
  max_expected_value: number;
  excellent_pattern_count: number;
  processing_status: string;
  total_samples: number;
  avg_win_rate: number;
  avg_expected_return: number;
}

interface Pagination {
  total: number;
  limit: number;
  offset: number;
  hasMore: boolean;
}

interface ApiResponse {
  success: boolean;
  data?: TomorrowSignalCandidate[];
  error?: string;
  pagination?: Pagination;
  metadata?: {
    query_time: string;
    source_table?: string;
    optimization?: string;
    description?: string;
    target_date?: string; // 後方互換性のため残す
  };
}

export default function TomorrowSignalsPage() {
  const router = useRouter();
  const [signals, setSignals] = useState<TomorrowSignalCandidate[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [metadata, setMetadata] = useState<any>(null);
  const [pagination, setPagination] = useState<Pagination | null>(null);
  
  // ページネーション設定
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(50);

  useEffect(() => {
    fetchSignals();
  }, [currentPage, pageSize]);

  const fetchSignals = async () => {
    try {
      setLoading(true);
      setError(null);
      
      const offset = (currentPage - 1) * pageSize;
      const url = `/api/signals/tomorrow?limit=${pageSize}&offset=${offset}`;
      
      const response = await fetch(url);
      const data: ApiResponse = await response.json();
      
      if (data.success && data.data) {
        setSignals(data.data);
        setMetadata(data.metadata);
        setPagination(data.pagination || null);
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
    const baseClasses = "inline-flex items-center px-1 py-0.5 rounded text-xs font-medium";
    
    switch (status) {
      case '未処理':
        return `${baseClasses} bg-yellow-100 text-yellow-800`;
      case '未（対象あり）':
        return `${baseClasses} bg-yellow-100 text-yellow-800`;
      case '済（対象あり）':
        return `${baseClasses} bg-green-100 text-green-800`;
      case '済（対象なし）':
        return `${baseClasses} bg-gray-100 text-gray-800`;
      default:
        return `${baseClasses} bg-gray-100 text-gray-800`;
    }
  };

  const getTradeTypeBadge = (tradeType: 'BUY' | 'SELL') => {
    const baseClasses = "inline-flex items-center px-1 py-0.5 rounded text-xs font-medium";
    
    if (tradeType === 'BUY') {
      return `${baseClasses} bg-blue-100 text-blue-800`;
    } else {
      return `${baseClasses} bg-red-100 text-red-800`;
    }
  };

  // ページネーション計算
  const totalPages = pagination ? Math.ceil(pagination.total / pageSize) : 1;
  const startRecord = pagination ? pagination.offset + 1 : 0;
  const endRecord = pagination ? Math.min(pagination.offset + pageSize, pagination.total) : 0;

  const handlePageChange = (newPage: number) => {
    if (newPage >= 1 && newPage <= totalPages) {
      setCurrentPage(newPage);
    }
  };

  const handlePageSizeChange = (newSize: number) => {
    setPageSize(newSize);
    setCurrentPage(1); // ページサイズ変更時は1ページ目に戻る
  };

  const handleConfigClick = (signal: TomorrowSignalCandidate) => {
    router.push(`/signals/tomorrow/${signal.stock_code}/${signal.trade_type}`);
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
    <div className="space-y-3">
      {/* パンくずナビ + 情報バー（コンパクト） */}
      <div className="sticky top-0 z-20 bg-white border-b border-gray-200 shadow-sm">
        <div className="px-4 py-2">
          {/* パンくずリスト */}
          <nav className="flex items-center space-x-2 text-sm text-gray-500 mb-2">
            <span className="text-gray-900 font-medium">明日のシグナル</span>
          </nav>
          
          {/* 情報 + コントロール */}
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-3 text-sm">
              {metadata && (
                <>
                  {/* 日付表示: target_date（旧API）または現在日付 */}
                  <span className="font-medium text-gray-900">
                    {metadata.target_date || new Date().toISOString().split('T')[0]}
                  </span>
                  <span className="text-gray-600">|</span>
                  <span className="text-gray-600">{pagination?.total || 0}件</span>
                  {/* 最適化情報表示 */}
                  {metadata.optimization && (
                    <>
                      <span className="text-gray-600">|</span>
                      <span className="text-green-600 font-medium text-xs">
                        ⚡ {metadata.optimization}
                      </span>
                    </>
                  )}
                  {metadata.query_time && (
                    <>
                      <span className="text-gray-600">|</span>
                      <span className="text-gray-600">
                        更新: {new Date(metadata.query_time).toLocaleTimeString('ja-JP', { hour: '2-digit', minute: '2-digit' })}
                      </span>
                    </>
                  )}
                </>
              )}
            </div>
            
            {/* ページサイズ選択 */}
            <div className="flex items-center space-x-2">
              <label className="text-sm text-gray-600">表示:</label>
              <select 
                value={pageSize} 
                onChange={(e) => handlePageSizeChange(Number(e.target.value))}
                className="text-sm border border-gray-300 rounded px-2 py-1 bg-white"
              >
                <option value={25}>25</option>
                <option value={50}>50</option>
                <option value={100}>100</option>
                <option value={200}>200</option>
              </select>
              {pagination && totalPages > 1 && (
                <>
                  <span className="text-gray-600">|</span>
                  <span className="text-sm text-gray-600">
                    {startRecord}〜{endRecord} / {pagination.total}
                  </span>
                </>
              )}
            </div>
          </div>
        </div>
      </div>

      {/* データテーブル（コンパクト） */}
      <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
        {signals.length === 0 ? (
          <div className="px-6 py-8 text-center">
            <p className="text-gray-500">明日のシグナル候補がありません</p>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">コード</th>
                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">銘柄名</th>
                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">売買</th>
                  <th className="px-3 py-2 text-right text-xs font-medium text-gray-500 uppercase">最高勝率</th>
                  <th className="px-3 py-2 text-right text-xs font-medium text-gray-500 uppercase">最高期待値</th>
                  <th className="px-3 py-2 text-right text-xs font-medium text-gray-500 uppercase">優秀数</th>
                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">状況</th>
                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">アクション</th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {signals.map((signal, index) => (
                  <tr key={`${signal.stock_code}-${signal.trade_type}`} className="hover:bg-gray-50">
                    <td className="px-3 py-2 whitespace-nowrap text-sm font-medium text-gray-900">
                      {signal.stock_code}
                    </td>
                    <td className="px-3 py-2 whitespace-nowrap text-sm text-gray-900">
                      {signal.stock_name}
                    </td>
                    <td className="px-3 py-2 whitespace-nowrap">
                      <span className={getTradeTypeBadge(signal.trade_type)}>
                        {signal.trade_type}
                      </span>
                    </td>
                    <td className="px-3 py-2 whitespace-nowrap text-sm text-gray-900 text-right">
                      {signal.max_win_rate?.toFixed(1) || '0.0'}%
                    </td>
                    <td className="px-3 py-2 whitespace-nowrap text-sm text-gray-900 text-right">
                      {signal.max_expected_value > 0 ? '+' : ''}{signal.max_expected_value?.toFixed(1) || '0.0'}%
                    </td>
                    <td className="px-3 py-2 whitespace-nowrap text-sm text-gray-900 text-right">
                      {signal.excellent_pattern_count || 0}個
                    </td>
                    <td className="px-3 py-2 whitespace-nowrap">
                      <span className={getStatusBadge(signal.processing_status)}>
                        {signal.processing_status}
                      </span>
                    </td>
                    <td className="px-3 py-2 whitespace-nowrap text-sm font-medium">
                      <button
                        className="text-blue-600 hover:text-blue-900 transition-colors"
                        onClick={() => handleConfigClick(signal)}
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

      {/* ページネーション（コンパクト） */}
      {pagination && totalPages > 1 && (
        <div className="bg-white border border-gray-200 rounded-lg px-3 py-2">
          <div className="flex items-center justify-between">
            {/* モバイル用シンプルナビゲーション */}
            <div className="flex sm:hidden">
              <button
                onClick={() => handlePageChange(currentPage - 1)}
                disabled={currentPage <= 1}
                className="px-3 py-1 border border-gray-300 text-sm rounded-md text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                前
              </button>
              <span className="mx-2 text-sm text-gray-700">{currentPage}/{totalPages}</span>
              <button
                onClick={() => handlePageChange(currentPage + 1)}
                disabled={currentPage >= totalPages}
                className="px-3 py-1 border border-gray-300 text-sm rounded-md text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                次
              </button>
            </div>
            
            {/* PC用詳細ナビゲーション */}
            <div className="hidden sm:flex items-center space-x-1">
              <button
                onClick={() => handlePageChange(1)}
                disabled={currentPage <= 1}
                className="px-2 py-1 border border-gray-300 bg-white text-sm text-gray-500 hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed rounded-l"
              >
                最初
              </button>
              
              <button
                onClick={() => handlePageChange(currentPage - 1)}
                disabled={currentPage <= 1}
                className="px-2 py-1 border border-gray-300 bg-white text-sm text-gray-500 hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                ←
              </button>

              {/* ページ番号（コンパクト） */}
              {(() => {
                const pages = [];
                const startPage = Math.max(1, currentPage - 2);
                const endPage = Math.min(totalPages, currentPage + 2);
                
                for (let i = startPage; i <= endPage; i++) {
                  pages.push(
                    <button
                      key={i}
                      onClick={() => handlePageChange(i)}
                      className={`px-2 py-1 border text-sm ${
                        i === currentPage
                          ? 'bg-blue-50 border-blue-500 text-blue-600'
                          : 'bg-white border-gray-300 text-gray-500 hover:bg-gray-50'
                      }`}
                    >
                      {i}
                    </button>
                  );
                }
                
                return pages;
              })()}

              <button
                onClick={() => handlePageChange(currentPage + 1)}
                disabled={currentPage >= totalPages}
                className="px-2 py-1 border border-gray-300 bg-white text-sm text-gray-500 hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                →
              </button>
              
              <button
                onClick={() => handlePageChange(totalPages)}
                disabled={currentPage >= totalPages}
                className="px-2 py-1 border border-gray-300 bg-white text-sm text-gray-500 hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed rounded-r"
              >
                最後
              </button>
            </div>

            <div className="hidden sm:block">
              <span className="text-sm text-gray-700">
                ページ {currentPage} / {totalPages}
              </span>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}