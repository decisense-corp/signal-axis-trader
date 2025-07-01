// src/app/signals/tomorrow/[stock_code]/[trade_type]/config/[signal_type]/[bin]/page.tsx
'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';

interface LearningPeriodData {
  signal_date: string;
  signal_value: number;
  entry_price: number;
  exit_price: number;
  profit_rate: number;
  is_win: boolean;
  trading_volume: number;
  reference_date: string;
  day_open: number;
  day_high: number;
  day_low: number;
  day_close: number;
  prev_close: number;
}

interface ConfigStats {
  total_samples: number;
  win_rate: number;
  avg_profit_rate: number;
  total_profit_rate: number;
  max_profit_rate: number;
  min_profit_rate: number;
  std_deviation: number;
  sharpe_ratio: number;
  median_profit_rate: number;
}

interface ConfigResponse {
  learning_data: LearningPeriodData[];
  baseline_stats: ConfigStats;
  filtered_stats?: ConfigStats;
  signal_info: {
    signal_type: string;
    signal_bin: number;
    trade_type: string;
    stock_code: string;
    stock_name: string;
    signal_description: string;
  };
}

interface PageProps {
  params: Promise<{
    stock_code: string;
    trade_type: string;
    signal_type: string;
    bin: string;
  }>;
}

// 安全な日付フォーマット関数
const formatDate = (dateValue: any): string => {
  try {
    if (!dateValue) return 'N/A';
    
    let dateObj: Date;
    
    if (dateValue instanceof Date) {
      dateObj = dateValue;
    } else if (typeof dateValue === 'string') {
      dateObj = new Date(dateValue);
    } else if (typeof dateValue === 'object' && dateValue.value) {
      dateObj = new Date(dateValue.value);
    } else {
      dateObj = new Date(String(dateValue));
    }
    
    if (isNaN(dateObj.getTime())) {
      console.warn('Invalid date value:', dateValue);
      return 'N/A';
    }
    
    return dateObj.toLocaleDateString('ja-JP', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit'
    });
  } catch (error) {
    console.error('Date formatting error:', error, 'for value:', dateValue);
    return 'N/A';
  }
};

export default function ConfigPage({ params }: PageProps) {
  const router = useRouter();
  
  // パラメータ状態
  const [pageParams, setPageParams] = useState<{
    stock_code: string;
    trade_type: string;
    signal_type: string;
    bin: string;
  } | null>(null);
  
  // データ状態
  const [configData, setConfigData] = useState<ConfigResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  
  // 🔧 修正: フィルタ条件の初期値を0に変更（寄り引けモード）
  const [profitTargetYen, setProfitTargetYen] = useState<number>(0);  // 100 → 0
  const [lossCutYen, setLossCutYen] = useState<number>(0);            // 100 → 0
  const [prevCloseGapCondition, setPrevCloseGapCondition] = useState<'all' | 'above' | 'below'>('all');
  const [prevCloseGapThreshold, setPrevCloseGapThreshold] = useState<number>(0);
  
  // UI状態
  const [activeTab, setActiveTab] = useState<'stats' | 'data'>('stats');
  const [updating, setUpdating] = useState(false);
  const [saving, setSaving] = useState(false);
  
  // sessionStorage状態
  const [selectedBins, setSelectedBins] = useState<number[]>([]);
  const [currentBinIndex, setCurrentBinIndex] = useState<number>(0);

  // パラメータ取得
  useEffect(() => {
    async function loadParams() {
      const resolvedParams = await params;
      setPageParams(resolvedParams);
    }
    loadParams();
  }, [params]);

  // sessionStorageから選択bin情報を復元
  useEffect(() => {
    if (typeof window !== 'undefined') {
      const storedBins = sessionStorage.getItem('selectedBins');
      const storedIndex = sessionStorage.getItem('currentBinIndex');
      
      if (storedBins) {
        setSelectedBins(JSON.parse(storedBins));
      }
      if (storedIndex) {
        setCurrentBinIndex(parseInt(storedIndex));
      }
    }
  }, []);

  // 初期データ取得
  useEffect(() => {
    if (pageParams) {
      fetchConfigData();
    }
  }, [pageParams]);

  // フィルタ条件変更時のデータ更新
  useEffect(() => {
    if (pageParams) {
      const timeoutId = setTimeout(() => {
        fetchConfigData();
      }, 500); // 500ms遅延でAPI呼び出し

      return () => clearTimeout(timeoutId);
    }
    // pageParamsがnullの場合は何も返さない（undefinedを明示的に返す）
    return undefined;
  }, [profitTargetYen, lossCutYen, prevCloseGapCondition, prevCloseGapThreshold]);

  const fetchConfigData = async () => {
    if (!pageParams) return;
    
    try {
      setUpdating(true);
      setError(null);
      
      const queryParams = new URLSearchParams();
      // 🔧 修正: 0より大きい場合のみパラメータ追加
      if (profitTargetYen > 0) queryParams.set('profit_target_yen', profitTargetYen.toString());
      if (lossCutYen > 0) queryParams.set('loss_cut_yen', lossCutYen.toString());
      if (prevCloseGapCondition !== 'all') {
        queryParams.set('prev_close_gap_condition', prevCloseGapCondition);
        queryParams.set('prev_close_gap_threshold', prevCloseGapThreshold.toString());
      }
      
      const url = `/api/signals/tomorrow/${pageParams.stock_code}/${pageParams.trade_type}/config/${pageParams.signal_type}/${pageParams.bin}?${queryParams}`;
      
      const response = await fetch(url);
      const data = await response.json();
      
      if (data.success && data.data) {
        setConfigData(data.data);
      } else {
        setError(data.error || 'データの取得に失敗しました');
      }
    } catch (err) {
      setError('ネットワークエラーが発生しました');
      console.error('API呼び出しエラー:', err);
    } finally {
      setUpdating(false);
      setLoading(false);
    }
  };

  // 🔧 修正: 設定状態を取得する関数
  const getCurrentSettingDescription = () => {
    if (profitTargetYen === 0 && lossCutYen === 0) {
      return "純粋な寄り引け取引";
    } else if (profitTargetYen > 0 && lossCutYen === 0) {
      return `利確${profitTargetYen}円のみ設定`;
    } else if (profitTargetYen === 0 && lossCutYen > 0) {
      return `損切${lossCutYen}円のみ設定`;
    } else {
      return `利確${profitTargetYen}円・損切${lossCutYen}円`;
    }
  };

  const handleSaveConfig = async () => {
    if (!pageParams) return;
    
    try {
      setSaving(true);
      
      const payload = {
        profit_target_yen: profitTargetYen,
        loss_cut_yen: lossCutYen,
        prev_close_gap_condition: prevCloseGapCondition,
        prev_close_gap_threshold: prevCloseGapThreshold,
        additional_notes: `学習期間での条件最適化: ${getCurrentSettingDescription()}`
      };
      
      const response = await fetch(
        `/api/signals/tomorrow/${pageParams.stock_code}/${pageParams.trade_type}/config/${pageParams.signal_type}/${pageParams.bin}`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        }
      );
      
      const data = await response.json();
      
      if (data.success) {
        // 次のbinまたは完了処理
        handleNextBin();
      } else {
        alert(`保存に失敗しました: ${data.error}`);
      }
    } catch (err) {
      alert('ネットワークエラーが発生しました');
    } finally {
      setSaving(false);
    }
  };

  const handleNextBin = () => {
    if (selectedBins.length > 0 && currentBinIndex < selectedBins.length - 1) {
      // 次のbinに進む
      const nextIndex = currentBinIndex + 1;
      const nextBin = selectedBins[nextIndex];
      
      sessionStorage.setItem('currentBinIndex', nextIndex.toString());
      
      router.push(`/signals/tomorrow/${pageParams!.stock_code}/${pageParams!.trade_type}/config/${pageParams!.signal_type}/${nextBin}`);
    } else {
      // 全bin完了 → 検算画面へ
      router.push(`/signals/tomorrow/${pageParams!.stock_code}/${pageParams!.trade_type}/verify`);
    }
  };

  const getStatsBadge = (stats: ConfigStats) => {
    if (stats.win_rate >= 70 && stats.avg_profit_rate >= 1.0) {
      return 'bg-purple-100 text-purple-800';
    } else if (stats.win_rate >= 60 && stats.avg_profit_rate >= 0.8) {
      return 'bg-green-100 text-green-800';
    } else if (stats.win_rate >= 55 && stats.avg_profit_rate >= 0.5) {
      return 'bg-blue-100 text-blue-800';
    } else {
      return 'bg-yellow-100 text-yellow-800';
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-64">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500"></div>
        <div className="ml-4">
          <p className="text-lg font-medium text-gray-900">読み込み中...</p>
          <p className="text-sm text-gray-500">学習期間データを分析しています</p>
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
                onClick={fetchConfigData}
                className="bg-red-100 text-red-800 px-3 py-1 rounded text-sm hover:bg-red-200"
              >
                再試行
              </button>
              <button
                onClick={() => router.back()}
                className="bg-gray-100 text-gray-800 px-3 py-1 rounded text-sm hover:bg-gray-200"
              >
                戻る
              </button>
            </div>
          </div>
        </div>
      </div>
    );
  }

  if (!configData) return null;

  const currentBinProgress = selectedBins.length > 0 ? `${currentBinIndex + 1}/${selectedBins.length}` : '1/1';

  return (
    <div className="space-y-3">
      {/* パンくずナビ + 最小限ヘッダー */}
      <div className="sticky top-0 z-20 bg-white border-b border-gray-200 shadow-sm">
        <div className="px-4 py-2">
          {/* パンくずリスト */}
          <nav className="flex items-center space-x-2 text-sm text-gray-500 mb-2">
            <a href="/signals/tomorrow" className="hover:text-gray-700">明日のシグナル</a>
            <span>›</span>
            <a href="#" onClick={() => router.back()} className="hover:text-gray-700">bin選択</a>
            <span>›</span>
            <span className="text-gray-900 font-medium">条件設定 ({currentBinProgress})</span>
          </nav>
          
          {/* 4軸情報 + アクション */}
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-3">
              <span className="font-medium text-gray-900">
                {configData.signal_info.signal_type}
              </span>
              <span className="text-gray-600">|</span>
              <span className="text-gray-600">Bin{configData.signal_info.signal_bin}</span>
              <span className="text-gray-600">|</span>
              <span className={`px-2 py-0.5 rounded text-xs font-medium ${
                configData.signal_info.trade_type === 'BUY' ? 'bg-blue-100 text-blue-800' : 'bg-red-100 text-red-800'
              }`}>
                {configData.signal_info.trade_type}
              </span>
              <span className="text-gray-600">|</span>
              <span className="text-gray-600">{configData.signal_info.stock_code}</span>
            </div>
            <div className="flex space-x-2">
              <button
                onClick={() => router.back()}
                className="px-3 py-1 text-gray-600 bg-gray-100 rounded text-sm hover:bg-gray-200"
              >
                ← 戻る
              </button>
              <button
                onClick={handleSaveConfig}
                disabled={saving}
                className="px-3 py-1 bg-blue-600 text-white rounded text-sm hover:bg-blue-700 disabled:opacity-50"
              >
                {saving ? '保存中...' : '💾 確定'}
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* 🔧 修正: フィルタ条件設定（初期値0・呼び値対応） */}
      <div className="bg-white rounded-lg border border-gray-200 p-3">
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-3">
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">
              利確目標（円）
              <span className="text-gray-500 ml-1">※0=設定なし</span>
            </label>
            <input
              type="number"
              min="0"        // 🔧 修正: 最小値を0に
              max="1000"
              step="1"       // 🔧 修正: 1円刻みに変更
              value={profitTargetYen}
              onChange={(e) => setProfitTargetYen(parseInt(e.target.value) || 0)}  // 🔧 修正: デフォルト0
              placeholder="0"
              className="w-full px-2 py-1 border border-gray-300 rounded text-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
            />
          </div>
          
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">
              損切設定（円）
              <span className="text-gray-500 ml-1">※0=設定なし</span>
            </label>
            <input
              type="number"
              min="0"        // 🔧 修正: 最小値を0に
              max="1000"
              step="1"       // 🔧 修正: 1円刻みに変更
              value={lossCutYen}
              onChange={(e) => setLossCutYen(parseInt(e.target.value) || 0)}      // 🔧 修正: デフォルト0
              placeholder="0"
              className="w-full px-2 py-1 border border-gray-300 rounded text-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
            />
          </div>
          
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">前日終値ギャップ</label>
            <select
              value={prevCloseGapCondition}
              onChange={(e) => setPrevCloseGapCondition(e.target.value as 'all' | 'above' | 'below')}
              className="w-full px-2 py-1 border border-gray-300 rounded text-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
            >
              <option value="all">全て</option>
              <option value="above">ギャップアップ以上</option>
              <option value="below">ギャップダウン未満</option>
            </select>
          </div>
          
          {prevCloseGapCondition !== 'all' && (
            <div>
              <label className="block text-xs font-medium text-gray-700 mb-1">ギャップ閾値（%）</label>
              <input
                type="number"
                min="-10"
                max="10"
                step="0.1"
                value={prevCloseGapThreshold}
                onChange={(e) => setPrevCloseGapThreshold(parseFloat(e.target.value) || 0)}
                className="w-full px-2 py-1 border border-gray-300 rounded text-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
              />
            </div>
          )}
        </div>
        
        {/* 🔧 追加: 現在の設定状態を表示 */}
        <div className="mt-2 p-2 bg-blue-50 rounded text-xs">
          <strong className="text-blue-900">現在の設定:</strong> 
          <span className="text-blue-700 ml-1">{getCurrentSettingDescription()}</span>
        </div>
      </div>

      {/* 統計比較（インライン・コンパクト） */}
      <div className="bg-white rounded-lg border border-gray-200 p-3">
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
          {/* ベースライン */}
          <div className="bg-gray-50 rounded p-2">
            <div className="flex items-center justify-between mb-2">
              <span className="text-sm font-medium text-gray-900">フィルタ前</span>
              <span className="text-xs text-gray-600">{configData.baseline_stats.total_samples}件</span>
              {updating && <div className="animate-spin rounded-full h-3 w-3 border-b-2 border-blue-600"></div>}
            </div>
            <div className="grid grid-cols-4 gap-2 text-xs">
              <div>
                <div className="text-gray-500">勝率</div>
                <div className="font-semibold">{configData.baseline_stats.win_rate.toFixed(1)}%</div>
              </div>
              <div>
                <div className="text-gray-500">平均</div>
                <div className="font-semibold">{configData.baseline_stats.avg_profit_rate > 0 ? '+' : ''}{configData.baseline_stats.avg_profit_rate.toFixed(2)}%</div>
              </div>
              <div>
                <div className="text-gray-500">シャープ</div>
                <div className="font-semibold">{configData.baseline_stats.sharpe_ratio.toFixed(2)}</div>
              </div>
              <div>
                <div className="text-gray-500">中央値</div>
                <div className="font-semibold">{configData.baseline_stats.median_profit_rate > 0 ? '+' : ''}{configData.baseline_stats.median_profit_rate.toFixed(2)}%</div>
              </div>
            </div>
          </div>
          
          {/* フィルタ後 */}
          {configData.filtered_stats && (
            <div className="bg-blue-50 rounded p-2">
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm font-medium text-blue-900">フィルタ後</span>
                <span className="text-xs text-blue-700">{configData.filtered_stats.total_samples}件</span>
              </div>
              <div className="grid grid-cols-4 gap-2 text-xs">
                <div>
                  <div className="text-blue-600">勝率</div>
                  <div className="font-semibold text-blue-900">{configData.filtered_stats.win_rate.toFixed(1)}%</div>
                  <div className="text-xs text-blue-600">
                    {(configData.filtered_stats.win_rate - configData.baseline_stats.win_rate) >= 0 ? '+' : ''}{(configData.filtered_stats.win_rate - configData.baseline_stats.win_rate).toFixed(1)}
                  </div>
                </div>
                <div>
                  <div className="text-blue-600">平均</div>
                  <div className="font-semibold text-blue-900">{configData.filtered_stats.avg_profit_rate > 0 ? '+' : ''}{configData.filtered_stats.avg_profit_rate.toFixed(2)}%</div>
                  <div className="text-xs text-blue-600">
                    {(configData.filtered_stats.avg_profit_rate - configData.baseline_stats.avg_profit_rate) >= 0 ? '+' : ''}{(configData.filtered_stats.avg_profit_rate - configData.baseline_stats.avg_profit_rate).toFixed(2)}
                  </div>
                </div>
                <div>
                  <div className="text-blue-600">シャープ</div>
                  <div className="font-semibold text-blue-900">{configData.filtered_stats.sharpe_ratio.toFixed(2)}</div>
                </div>
                <div>
                  <div className="text-blue-600">中央値</div>
                  <div className="font-semibold text-blue-900">{configData.filtered_stats.median_profit_rate > 0 ? '+' : ''}{configData.filtered_stats.median_profit_rate.toFixed(2)}%</div>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* タブコンテンツ */}
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
              📈 詳細統計
            </button>
            <button
              onClick={() => setActiveTab('data')}
              className={`py-4 px-1 border-b-2 font-medium text-sm ${
                activeTab === 'data'
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700'
              }`}
            >
              📋 取引詳細 ({configData.learning_data.length}件)
            </button>
          </nav>
        </div>
        
        <div className="p-4">
          {activeTab === 'stats' && (
            <div className="space-y-4">
              <h3 className="font-medium text-gray-900">学習期間での詳細分析</h3>
              
              <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
                <div className="bg-gray-50 rounded-lg p-4">
                  <h4 className="font-medium text-gray-900 mb-2">📊 利益分布</h4>
                  <div className="space-y-2 text-sm">
                    <div className="flex justify-between">
                      <span>最大利益:</span>
                      <span className="font-medium text-green-600">+{configData.baseline_stats.max_profit_rate.toFixed(2)}%</span>
                    </div>
                    <div className="flex justify-between">
                      <span>最大損失:</span>
                      <span className="font-medium text-red-600">{configData.baseline_stats.min_profit_rate.toFixed(2)}%</span>
                    </div>
                    <div className="flex justify-between">
                      <span>標準偏差:</span>
                      <span className="font-medium">{configData.baseline_stats.std_deviation.toFixed(2)}%</span>
                    </div>
                  </div>
                </div>
                
                <div className="bg-gray-50 rounded-lg p-4">
                  <h4 className="font-medium text-gray-900 mb-2">💰 累積収益</h4>
                  <div className="space-y-2 text-sm">
                    <div className="flex justify-between">
                      <span>総収益率:</span>
                      <span className="font-medium">{configData.baseline_stats.total_profit_rate > 0 ? '+' : ''}{configData.baseline_stats.total_profit_rate.toFixed(2)}%</span>
                    </div>
                    <div className="flex justify-between">
                      <span>平均リターン:</span>
                      <span className="font-medium">{configData.baseline_stats.avg_profit_rate > 0 ? '+' : ''}{configData.baseline_stats.avg_profit_rate.toFixed(2)}%</span>
                    </div>
                    <div className="flex justify-between">
                      <span>取引回数:</span>
                      <span className="font-medium">{configData.baseline_stats.total_samples}回</span>
                    </div>
                  </div>
                </div>
                
                <div className="bg-gray-50 rounded-lg p-4">
                  <h4 className="font-medium text-gray-900 mb-2">⚡ リスク指標</h4>
                  <div className="space-y-2 text-sm">
                    <div className="flex justify-between">
                      <span>シャープレシオ:</span>
                      <span className="font-medium">{configData.baseline_stats.sharpe_ratio.toFixed(3)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span>勝率:</span>
                      <span className="font-medium">{configData.baseline_stats.win_rate.toFixed(1)}%</span>
                    </div>
                    <div className="flex justify-between">
                      <span>中央値:</span>
                      <span className="font-medium">{configData.baseline_stats.median_profit_rate > 0 ? '+' : ''}{configData.baseline_stats.median_profit_rate.toFixed(2)}%</span>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          )}
          
          {activeTab === 'data' && (
            <div className="space-y-4">
              <h3 className="font-medium text-gray-900">学習期間の取引詳細（最新20件）</h3>
              
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200 text-xs">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-2 py-2 text-left font-medium text-gray-500 uppercase">日付</th>
                      <th className="px-2 py-2 text-right font-medium text-gray-500 uppercase">前日終値</th>
                      <th className="px-2 py-2 text-right font-medium text-gray-500 uppercase">始値</th>
                      <th className="px-2 py-2 text-right font-medium text-gray-500 uppercase">高値</th>
                      <th className="px-2 py-2 text-right font-medium text-gray-500 uppercase">安値</th>
                      <th className="px-2 py-2 text-right font-medium text-gray-500 uppercase">終値</th>
                      <th className="px-2 py-2 text-right font-medium text-gray-500 uppercase">終値→始値</th>
                      <th className="px-2 py-2 text-right font-medium text-gray-500 uppercase">始値→高値</th>
                      <th className="px-2 py-2 text-right font-medium text-gray-500 uppercase">始値→安値</th>
                      <th className="px-2 py-2 text-right font-medium text-gray-500 uppercase">始値→終値</th>
                      <th className="px-2 py-2 text-right font-medium text-gray-500 uppercase">利益率</th>
                      <th className="px-2 py-2 text-center font-medium text-gray-500 uppercase">勝敗</th>
                      <th className="px-2 py-2 text-right font-medium text-gray-500 uppercase">出来高</th>
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                    {configData.learning_data.slice(0, 20).map((row, index) => {
                      // 値幅計算（円単位）
                      const gapRange = row.prev_close ? (row.day_open - row.prev_close) : 0;
                      const openToHighRange = row.day_open ? (row.day_high - row.day_open) : 0;
                      const openToLowRange = row.day_open ? (row.day_low - row.day_open) : 0;
                      const openToCloseRange = row.day_open ? (row.day_close - row.day_open) : 0;
                      
                      return (
                        <tr key={index} className="hover:bg-gray-50">
                          <td className="px-2 py-2 text-gray-900">
                            {formatDate(row.signal_date)}
                          </td>
                          <td className="px-2 py-2 text-gray-900 text-right">
                            ¥{row.prev_close?.toLocaleString() || 'N/A'}
                          </td>
                          <td className="px-2 py-2 text-gray-900 text-right">
                            ¥{row.day_open?.toLocaleString() || 'N/A'}
                          </td>
                          <td className="px-2 py-2 text-gray-900 text-right">
                            ¥{row.day_high?.toLocaleString() || 'N/A'}
                          </td>
                          <td className="px-2 py-2 text-gray-900 text-right">
                            ¥{row.day_low?.toLocaleString() || 'N/A'}
                          </td>
                          <td className="px-2 py-2 text-gray-900 text-right">
                            ¥{row.day_close?.toLocaleString() || 'N/A'}
                          </td>
                          <td className="px-2 py-2 text-right">
                            <span className={gapRange >= 0 ? 'text-green-600' : 'text-red-600'}>
                              {gapRange >= 0 ? '+' : ''}¥{gapRange.toFixed(0)}
                            </span>
                          </td>
                          <td className="px-2 py-2 text-right">
                            <span className={openToHighRange >= 0 ? 'text-green-600' : 'text-red-600'}>
                              {openToHighRange >= 0 ? '+' : ''}¥{openToHighRange.toFixed(0)}
                            </span>
                          </td>
                          <td className="px-2 py-2 text-right">
                            <span className={openToLowRange >= 0 ? 'text-green-600' : 'text-red-600'}>
                              {openToLowRange >= 0 ? '+' : ''}¥{openToLowRange.toFixed(0)}
                            </span>
                          </td>
                          <td className="px-2 py-2 text-right">
                            <span className={openToCloseRange >= 0 ? 'text-green-600' : 'text-red-600'}>
                              {openToCloseRange >= 0 ? '+' : ''}¥{openToCloseRange.toFixed(0)}
                            </span>
                          </td>
                          <td className="px-2 py-2 text-right">
                            <span className={row.profit_rate >= 0 ? 'text-green-600 font-medium' : 'text-red-600 font-medium'}>
                              {row.profit_rate >= 0 ? '+' : ''}{row.profit_rate.toFixed(2)}%
                            </span>
                          </td>
                          <td className="px-2 py-2 text-center">
                            <span className={`inline-flex items-center px-1.5 py-0.5 rounded-full text-xs font-medium ${
                              row.is_win ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'
                            }`}>
                              {row.is_win ? '勝' : '負'}
                            </span>
                          </td>
                          <td className="px-2 py-2 text-gray-900 text-right">
                            {row.trading_volume?.toLocaleString() || 'N/A'}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
              
              {configData.learning_data.length > 20 && (
                <p className="text-sm text-gray-500 text-center">
                  表示件数: 20/{configData.learning_data.length}件（最新順）
                </p>
              )}
            </div>
          )}
        </div>
      </div>

      {/* 進行状況・次のアクション（コンパクト化） */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-3">
        <div className="flex items-center justify-between">
          <div>
            <h3 className="text-sm font-medium text-blue-900">進行状況</h3>
            <p className="text-sm text-blue-700">
              Bin {currentBinIndex + 1}/{selectedBins.length} の条件設定中
              {selectedBins.length > 1 && currentBinIndex < selectedBins.length - 1 && 
                ` | 次は Bin${selectedBins[currentBinIndex + 1]} です`
              }
            </p>
          </div>
          <div className="flex space-x-2">
            {selectedBins.length > 1 && currentBinIndex < selectedBins.length - 1 && (
              <button
                onClick={handleSaveConfig}
                disabled={saving}
                className="px-3 py-1 bg-blue-600 text-white rounded text-sm hover:bg-blue-700 disabled:opacity-50"
              >
                {saving ? '保存中...' : '次のBinへ →'}
              </button>
            )}
            {(selectedBins.length === 1 || currentBinIndex === selectedBins.length - 1) && (
              <button
                onClick={handleSaveConfig}
                disabled={saving}
                className="px-3 py-1 bg-green-600 text-white rounded text-sm hover:bg-green-700 disabled:opacity-50"
              >
                {saving ? '保存中...' : '✅ 検算画面へ'}
              </button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}