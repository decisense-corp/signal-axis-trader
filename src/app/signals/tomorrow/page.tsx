// src/app/signals/tomorrow/page.tsx
// 申し送り書仕様準拠：10項目表示、フィルタ、ページネーション
// 🆕 4aフィルタ機能追加（4年連続優良シグナル）
'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';

// 申し送り書準拠の型定義（4a追加）
interface TomorrowSignal {
  signal_type: string;
  signal_bin: number;
  trade_type: 'BUY' | 'SELL';  // ✅ 申し送り書仕様：BUY/SELL
  stock_code: string;
  stock_name: string;
  total_samples: number;       // 学習期間サンプル数
  win_rate: number;           // 学習期間勝率（%）
  avg_profit_rate: number;    // 学習期間平均利益率（%）
  decision_status: 'configured' | 'pending' | 'rejected';
  pattern_category: 'PREMIUM' | 'EXCELLENT' | 'GOOD' | 'NORMAL' | 'CAUTION';
  is_excellent_pattern: boolean;
  four_a?: number;  // 🆕 4年連続優良シグナルフラグ（0 or 1）
}

interface ApiResponse {
  signals: TomorrowSignal[];
  total_count: number;
  page: number;
  per_page: number;
}

// 申し送り書推奨：パターンカテゴリ色分けルール
const categoryColors = {
  'PREMIUM': 'bg-purple-100 text-purple-800 border-purple-200',
  'EXCELLENT': 'bg-blue-100 text-blue-800 border-blue-200', 
  'GOOD': 'bg-green-100 text-green-800 border-green-200',
  'NORMAL': 'bg-yellow-100 text-yellow-800 border-yellow-200',
  'CAUTION': 'bg-red-100 text-red-800 border-red-200'
} as const;

const decisionStatusText = {
  'pending': '未設定',
  'configured': '設定済み',
  'rejected': '却下済み'
} as const;

const decisionStatusColors = {
  'pending': 'bg-gray-100 text-gray-800',
  'configured': 'bg-green-100 text-green-800',
  'rejected': 'bg-red-100 text-red-800'
} as const;

export default function TomorrowSignalsPage() {
  const router = useRouter();
  
  // State管理
  const [signals, setSignals] = useState<TomorrowSignal[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [totalCount, setTotalCount] = useState(0);
  
  // 申し送り書仕様：フィルタ状態
  const [page, setPage] = useState(1);
  const [perPage, setPerPage] = useState(15); // 申し送り書仕様：デフォルト15件
  const [decisionFilter, setDecisionFilter] = useState('pending_only'); // 申し送り書仕様：デフォルト未設定のみ
  const [minWinRate, setMinWinRate] = useState('');
  const [fourAFilter, setFourAFilter] = useState('only_4a'); // 🆕 4aフィルタ（デフォルト：4aのみ）
  const [stockCode, setStockCode] = useState(''); // 銘柄コードフィルタ

  // データ取得
  const fetchSignals = async () => {
    try {
      setLoading(true);
      setError(null);
      
      const params = new URLSearchParams({
        page: page.toString(),
        per_page: perPage.toString(),
        decision_filter: decisionFilter,
      });
      
      if (minWinRate) params.set('min_win_rate', minWinRate);
      if (fourAFilter !== 'all') params.set('four_a_filter', fourAFilter); // 🆕 4aフィルタパラメータ
      if (stockCode) params.set('stock_code', stockCode);
      
      const response = await fetch(`/api/signals/tomorrow?${params}`);
      const data: ApiResponse = await response.json();
      
      if (response.ok) {
        setSignals(data.signals);
        setTotalCount(data.total_count);
      } else {
        setError('データの取得に失敗しました');
      }
    } catch (err) {
      setError('ネットワークエラーが発生しました');
      console.error('Fetch error:', err);
    } finally {
      setLoading(false);
    }
  };

  // 初期読み込み・フィルタ変更時
  useEffect(() => {
    fetchSignals();
  }, [page, perPage, decisionFilter, minWinRate, fourAFilter, stockCode]); // 🆕 fourAFilter追加

  // 設定ボタンクリック（申し送り書仕様の画面遷移）
  const handleConfigClick = (signal: TomorrowSignal) => {
    const url = `/signals/tomorrow/config/${encodeURIComponent(signal.signal_type)}/${signal.signal_bin}/${signal.trade_type}/${signal.stock_code}`;
    router.push(url);
  };

  // ページ計算
  const totalPages = Math.ceil(totalCount / perPage);

  return (
    <div className="container mx-auto px-4 py-6 max-w-7xl">
      {/* ヘッダー */}
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-900 mb-2">
          🎯 明日のシグナル一覧（4軸）
        </h1>
        <p className="text-gray-600">
          優秀パターンの条件設定 • 総数: {totalCount.toLocaleString()}件
        </p>
      </div>

      {/* 申し送り書仕様：フィルタエリア */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4 mb-6">
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-6 gap-4">
          {/* 銘柄コード */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              銘柄コード
            </label>
            <input
              type="text"
              value={stockCode}
              onChange={(e) => {
                setStockCode(e.target.value.toUpperCase()); // 大文字に変換
                setPage(1);
              }}
              placeholder="例: 7203"
              className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
            />
          </div>

          {/* 設定状況フィルタ */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              設定状況
            </label>
            <select
              value={decisionFilter}
              onChange={(e) => {
                setDecisionFilter(e.target.value);
                setPage(1);
              }}
              className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
            >
              <option value="pending_only">未設定のみ</option>
              <option value="all">すべて</option>
            </select>
          </div>

          {/* 勝率最低値 */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              勝率最低値（%）
            </label>
            <input
              type="number"
              value={minWinRate}
              onChange={(e) => {
                setMinWinRate(e.target.value);
                setPage(1);
              }}
              placeholder="55"
              className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
            />
          </div>

          {/* 🆕 4aフィルタ（期待値最低値を置き換え） */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              4年連続優良
            </label>
            <select
              value={fourAFilter}
              onChange={(e) => {
                setFourAFilter(e.target.value);
                setPage(1);
              }}
              className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
            >
              <option value="only_4a">4aのみ</option>
              <option value="all">すべて</option>
              <option value="exclude_4a">4a以外</option>
            </select>
          </div>

          {/* 表示件数 */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              表示件数
            </label>
            <select
              value={perPage}
              onChange={(e) => {
                setPerPage(parseInt(e.target.value));
                setPage(1);
              }}
              className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
            >
              <option value={15}>15件</option>
              <option value={30}>30件</option>
              <option value={50}>50件</option>
            </select>
          </div>

          {/* リフレッシュボタン */}
          <div className="flex items-end">
            <button
              onClick={() => fetchSignals()}
              disabled={loading}
              className="w-full bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white font-medium py-2 px-4 rounded-md transition-colors"
            >
              {loading ? '読込中...' : '🔄 更新'}
            </button>
          </div>
        </div>
      </div>

      {/* エラー表示 */}
      {error && (
        <div className="bg-red-50 border border-red-200 rounded-md p-4 mb-6">
          <div className="flex">
            <div className="text-red-800">
              <strong>エラー:</strong> {error}
            </div>
          </div>
        </div>
      )}

      {/* 申し送り書仕様：10項目表示テーブル */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  銘柄コード
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  銘柄名
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  売買方向
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  シグナルタイプ
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  ビン番号
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  サンプル数
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  勝率
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  期待値
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  設定状況
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  設定
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {loading ? (
                <tr>
                  <td colSpan={10} className="px-4 py-8 text-center text-gray-500">
                    データを読み込み中...
                  </td>
                </tr>
              ) : signals.length === 0 ? (
                <tr>
                  <td colSpan={10} className="px-4 py-8 text-center text-gray-500">
                    条件に一致するシグナルがありません
                  </td>
                </tr>
              ) : (
                signals.map((signal, index) => (
                  <tr 
                    key={`${signal.signal_type}_${signal.signal_bin}_${signal.trade_type}_${signal.stock_code}`}
                    className="hover:bg-gray-50"
                  >
                    {/* 銘柄コード（🆕 4aフラグ表示付き） */}
                    <td className="px-4 py-3 whitespace-nowrap text-sm font-medium text-gray-900">
                      <div className="flex items-center">
                        {signal.stock_code}
                        {signal.four_a === 1 && (
                          <span className="ml-2 inline-flex items-center px-2 py-0.5 text-xs font-medium bg-gold-100 text-gold-800 rounded-full border border-gold-200" style={{ backgroundColor: '#fffbeb', color: '#92400e', borderColor: '#fde68a' }}>
                            4A
                          </span>
                        )}
                      </div>
                    </td>
                    
                    {/* 銘柄名 */}
                    <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-900">
                      <div className="max-w-xs truncate">
                        {signal.stock_name}
                      </div>
                    </td>
                    
                    {/* 売買方向 */}
                    <td className="px-4 py-3 whitespace-nowrap text-sm">
                      <span className={`inline-flex px-2 py-1 text-xs font-semibold rounded-full ${
                        signal.trade_type === 'BUY' 
                          ? 'bg-green-100 text-green-800' 
                          : 'bg-red-100 text-red-800'
                      }`}>
                        {signal.trade_type}
                      </span>
                    </td>
                    
                    {/* シグナルタイプ */}
                    <td className="px-4 py-3 text-sm text-gray-900">
                      <div className="max-w-xs truncate" title={signal.signal_type}>
                        {signal.signal_type}
                      </div>
                    </td>
                    
                    {/* ビン番号 */}
                    <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-900">
                      <span className={`inline-flex px-2 py-1 text-xs font-semibold rounded-full border ${categoryColors[signal.pattern_category]}`}>
                        {signal.signal_bin}
                      </span>
                    </td>
                    
                    {/* サンプル数 */}
                    <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-900">
                      {signal.total_samples}
                    </td>
                    
                    {/* 勝率（申し送り書仕様：小数点1桁） */}
                    <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-900">
                      <span className="font-medium">
                        {signal.win_rate.toFixed(1)}%
                      </span>
                    </td>
                    
                    {/* 期待値（申し送り書仕様：小数点2桁） */}
                    <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-900">
                      <span className="font-medium text-blue-600">
                        {signal.avg_profit_rate.toFixed(2)}%
                      </span>
                    </td>
                    
                    {/* 設定状況 */}
                    <td className="px-4 py-3 whitespace-nowrap text-sm">
                      <span className={`inline-flex px-2 py-1 text-xs font-semibold rounded-full ${decisionStatusColors[signal.decision_status]}`}>
                        {decisionStatusText[signal.decision_status]}
                      </span>
                    </td>
                    
                    {/* 設定ボタン */}
                    <td className="px-4 py-3 whitespace-nowrap text-sm">
                      <button
                        onClick={() => handleConfigClick(signal)}
                        className="bg-blue-600 hover:bg-blue-700 text-white text-xs font-medium py-1 px-3 rounded transition-colors"
                      >
                        設定
                      </button>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* 申し送り書仕様：ページネーション */}
      {totalPages > 1 && (
        <div className="bg-white px-4 py-3 flex items-center justify-between border-t border-gray-200 sm:px-6 mt-6 rounded-lg shadow-sm">
          <div className="flex-1 flex justify-between sm:hidden">
            <button
              onClick={() => setPage(Math.max(1, page - 1))}
              disabled={page === 1}
              className="relative inline-flex items-center px-4 py-2 border border-gray-300 text-sm font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-50"
            >
              前へ
            </button>
            <button
              onClick={() => setPage(Math.min(totalPages, page + 1))}
              disabled={page === totalPages}
              className="ml-3 relative inline-flex items-center px-4 py-2 border border-gray-300 text-sm font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-50"
            >
              次へ
            </button>
          </div>
          <div className="hidden sm:flex-1 sm:flex sm:items-center sm:justify-between">
            <div>
              <p className="text-sm text-gray-700">
                <span className="font-medium">{((page - 1) * perPage) + 1}</span>
                〜
                <span className="font-medium">{Math.min(page * perPage, totalCount)}</span>
                件 / 全
                <span className="font-medium">{totalCount.toLocaleString()}</span>
                件
              </p>
            </div>
            <div>
              <nav className="relative z-0 inline-flex rounded-md shadow-sm -space-x-px">
                <button
                  onClick={() => setPage(Math.max(1, page - 1))}
                  disabled={page === 1}
                  className="relative inline-flex items-center px-2 py-2 rounded-l-md border border-gray-300 bg-white text-sm font-medium text-gray-500 hover:bg-gray-50 disabled:opacity-50"
                >
                  ←
                </button>
                
                {/* ページ番号表示 */}
                {(() => {
                  const startPage = Math.max(1, page - 2);
                  const endPage = Math.min(totalPages, startPage + 4);
                  const pages = [];
                  
                  for (let i = startPage; i <= endPage; i++) {
                    pages.push(
                      <button
                        key={`page-${i}`}
                        onClick={() => setPage(i)}
                        className={`relative inline-flex items-center px-4 py-2 border text-sm font-medium ${
                          i === page
                            ? 'z-10 bg-blue-50 border-blue-500 text-blue-600'
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
                  onClick={() => setPage(Math.min(totalPages, page + 1))}
                  disabled={page === totalPages}
                  className="relative inline-flex items-center px-2 py-2 rounded-r-md border border-gray-300 bg-white text-sm font-medium text-gray-500 hover:bg-gray-50 disabled:opacity-50"
                >
                  →
                </button>
              </nav>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// ✅ 申し送り書チェックリスト確認
// - 10項目表示テーブル ✅
// - フィルタ機能（設定状況・勝率） ✅
// - 🆕 4aフィルタ追加（期待値最低値を置き換え） ✅
// - 🆕 4aフラグ表示（銘柄コード欄） ✅
// - 銘柄コードフィルタ ✅
// - ページネーション（15件/30件/50件） ✅
// - BUY/SELL用語統一 ✅
// - パターンカテゴリ色分け ✅
// - 申し送り書推奨の画面遷移URL ✅
// - レスポンシブ対応 ✅
// - 高速レスポンス対応（loading状態） ✅