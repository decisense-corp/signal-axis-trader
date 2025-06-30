// src/app/signals/tomorrow/[stock_code]/[trade_type]/page.tsx
'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';

interface SignalTypeDetail {
  signal_type: string;
  signal_category: string;
  description: string;
  excellent_bins: number[];
  max_win_rate: number;
  max_expected_value: number;
  best_bin: number;
  best_bin_win_rate: number;
  best_bin_expected_value: number;
}

interface AxisDetailData {
  signal_type: string;
  signal_bin: number;
  trade_type: string;
  stock_code: string;
  stock_name: string;
  learning_win_rate: number;
  learning_avg_profit: number;
  learning_samples: number;
  learning_sharpe_ratio: number;
  pattern_category: string;
  signal_description: string;
}

interface DecisionConfig {
  signal_type: string;
  signal_bin: number;
  trade_type: 'BUY' | 'SELL';
  stock_code: string;
  profit_target_rate: number;
  loss_cut_rate: number;
  max_hold_days: number;
  position_size_rate: number;
  min_signal_strength: number;
  excluded_months: number[];
  additional_notes: string;
}

interface PageProps {
  params: Promise<{
    stock_code: string;
    trade_type: string;
  }>;
}

export default function IndividualConfigPage({ params }: PageProps) {
  const router = useRouter();
  
  // 状態管理
  const [stockCode, setStockCode] = useState<string>('');
  const [tradeType, setTradeType] = useState<string>('');
  const [signalTypes, setSignalTypes] = useState<SignalTypeDetail[]>([]);
  const [selectedSignalType, setSelectedSignalType] = useState<string>('');
  const [selectedBin, setSelectedBin] = useState<number>(0);
  const [axisDetail, setAxisDetail] = useState<AxisDetailData | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  
  // フォーム状態
  const [config, setConfig] = useState<Partial<DecisionConfig>>({
    profit_target_rate: 2.0,
    loss_cut_rate: 2.0,
    max_hold_days: 5,
    position_size_rate: 10.0,
    min_signal_strength: 0,
    excluded_months: [],
    additional_notes: ''
  });

  // パラメータ取得
  useEffect(() => {
    async function loadParams() {
      const resolvedParams = await params;
      setStockCode(resolvedParams.stock_code);
      setTradeType(resolvedParams.trade_type.toUpperCase());
    }
    loadParams();
  }, [params]);

  // シグナルタイプ一覧取得
  useEffect(() => {
    if (stockCode && tradeType) {
      fetchSignalTypes();
    }
  }, [stockCode, tradeType]);

  // 4軸詳細取得
  useEffect(() => {
    if (selectedSignalType && selectedBin && stockCode && tradeType) {
      fetchAxisDetail();
    }
  }, [selectedSignalType, selectedBin, stockCode, tradeType]);

  const fetchSignalTypes = async () => {
    try {
      setLoading(true);
      setError(null);
      
      const response = await fetch(`/api/axis/signal-types/${stockCode}/${tradeType}`);
      const data = await response.json();
      
      if (data.success && data.data) {
        setSignalTypes(data.data);
        if (data.data.length > 0) {
          const firstType = data.data[0];
          setSelectedSignalType(firstType.signal_type);
          setSelectedBin(firstType.best_bin);
        }
      } else {
        setError(data.error || 'シグナルタイプの取得に失敗しました');
      }
    } catch (err) {
      setError('ネットワークエラーが発生しました');
    } finally {
      setLoading(false);
    }
  };

  const fetchAxisDetail = async () => {
    try {
      const response = await fetch(`/api/axis/${selectedSignalType}/${selectedBin}/${tradeType}/${stockCode}`);
      const data = await response.json();
      
      if (data.success && data.data) {
        setAxisDetail(data.data);
      }
    } catch (err) {
      console.error('4軸詳細取得エラー:', err);
    }
  };

  const handleSignalTypeChange = (signalType: string) => {
    setSelectedSignalType(signalType);
    const typeDetail = signalTypes.find(t => t.signal_type === signalType);
    if (typeDetail) {
      setSelectedBin(typeDetail.best_bin);
    }
  };

  const handleConfigChange = (key: keyof DecisionConfig, value: any) => {
    setConfig(prev => ({ ...prev, [key]: value }));
  };

  const handleSave = async () => {
    if (!axisDetail) return;
    
    try {
      setSaving(true);
      
      const payload: DecisionConfig = {
        signal_type: axisDetail.signal_type,
        signal_bin: axisDetail.signal_bin,
        trade_type: axisDetail.trade_type as 'BUY' | 'SELL',
        stock_code: axisDetail.stock_code,
        profit_target_rate: config.profit_target_rate || 2.0,
        loss_cut_rate: config.loss_cut_rate || 2.0,
        max_hold_days: config.max_hold_days || 5,
        position_size_rate: config.position_size_rate || 10.0,
        min_signal_strength: config.min_signal_strength || 0,
        excluded_months: config.excluded_months || [],
        additional_notes: config.additional_notes || ''
      };
      
      const response = await fetch('/api/decisions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      
      const data = await response.json();
      
      if (data.success) {
        alert('条件設定が保存されました！');
        router.push('/signals/tomorrow');
      } else {
        alert(`保存に失敗しました: ${data.error}`);
      }
    } catch (err) {
      alert('ネットワークエラーが発生しました');
    } finally {
      setSaving(false);
    }
  };

  const getPatternBadge = (category: string) => {
    const baseClasses = "inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium";
    
    switch (category) {
      case '超優秀': return `${baseClasses} bg-purple-100 text-purple-800`;
      case '優秀': return `${baseClasses} bg-green-100 text-green-800`;
      case '良好': return `${baseClasses} bg-blue-100 text-blue-800`;
      case '標準': return `${baseClasses} bg-yellow-100 text-yellow-800`;
      default: return `${baseClasses} bg-red-100 text-red-800`;
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-64">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500"></div>
        <div className="ml-4">
          <p className="text-lg font-medium text-gray-900">読み込み中...</p>
          <p className="text-sm text-gray-500">シグナル分析データを取得しています</p>
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
                onClick={fetchSignalTypes}
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

  return (
    <div className="space-y-6">
      {/* ヘッダー */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold text-gray-900">📊 個別条件設定</h1>
            <p className="text-gray-600 mt-1">
              {stockCode} ({tradeType}) の取引条件を設定してください
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

      {/* Step 1: シグナルタイプ選択 */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <h2 className="text-lg font-semibold text-gray-900 mb-4">
          Step 1: 優秀シグナルタイプ選択
        </h2>
        
        {signalTypes.length === 0 ? (
          <div className="text-center py-8">
            <p className="text-gray-500">優秀なシグナルタイプが見つかりませんでした</p>
          </div>
        ) : (
          <div className="space-y-4">
            {signalTypes.map((type) => (
              <div
                key={type.signal_type}
                className={`border rounded-lg p-4 cursor-pointer transition-colors ${
                  selectedSignalType === type.signal_type
                    ? 'border-blue-500 bg-blue-50'
                    : 'border-gray-200 hover:border-gray-300'
                }`}
                onClick={() => handleSignalTypeChange(type.signal_type)}
              >
                <div className="flex items-center justify-between">
                  <div className="flex-1">
                    <div className="flex items-center space-x-3">
                      <input
                        type="radio"
                        checked={selectedSignalType === type.signal_type}
                        onChange={() => handleSignalTypeChange(type.signal_type)}
                        className="h-4 w-4 text-blue-600"
                      />
                      <div>
                        <h3 className="font-medium text-gray-900">{type.signal_type}</h3>
                        <p className="text-sm text-gray-500">{type.description}</p>
                      </div>
                    </div>
                  </div>
                  <div className="text-right">
                    <div className="text-sm font-medium text-gray-900">
                      最高勝率: {type.max_win_rate?.toFixed(1)}%
                    </div>
                    <div className="text-sm font-medium text-gray-900">
                      最高期待値: +{type.max_expected_value?.toFixed(2)}%
                    </div>
                  </div>
                </div>
                
                {selectedSignalType === type.signal_type && (
                  <div className="mt-4 pt-4 border-t border-gray-200">
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      シグナル区分 (bin) を選択:
                    </label>
                    <div className="flex flex-wrap gap-2">
                      {type.excellent_bins.map((bin) => (
                        <button
                          key={bin}
                          onClick={(e) => {
                            e.stopPropagation();
                            setSelectedBin(bin);
                          }}
                          className={`px-3 py-1 rounded text-sm font-medium ${
                            selectedBin === bin
                              ? 'bg-blue-600 text-white'
                              : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                          }`}
                        >
                          Bin {bin}
                          {bin === type.best_bin && <span className="ml-1 text-xs">★</span>}
                        </button>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Step 2: 4軸詳細情報 */}
      {axisDetail && (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">
            Step 2: 選択した4軸の詳細統計
          </h2>
          
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div className="space-y-4">
              <div>
                <h3 className="font-medium text-gray-900 mb-2">📋 基本情報</h3>
                <div className="space-y-2 text-sm">
                  <div className="flex justify-between">
                    <span className="text-gray-500">4軸組み合わせ:</span>
                    <span className="font-medium">
                      {axisDetail.signal_type} / Bin{axisDetail.signal_bin} / {axisDetail.trade_type} / {axisDetail.stock_code}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-gray-500">銘柄名:</span>
                    <span className="font-medium">{axisDetail.stock_name}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-gray-500">パターン評価:</span>
                    <span className={getPatternBadge(axisDetail.pattern_category)}>
                      {axisDetail.pattern_category}
                    </span>
                  </div>
                </div>
              </div>
              
              <div>
                <h3 className="font-medium text-gray-900 mb-2">📈 学習期間統計</h3>
                <div className="bg-gray-50 rounded-lg p-4">
                  <div className="grid grid-cols-2 gap-4 text-sm">
                    <div>
                      <div className="text-gray-500">勝率</div>
                      <div className="font-semibold text-lg text-green-600">
                        {axisDetail.learning_win_rate?.toFixed(1)}%
                      </div>
                    </div>
                    <div>
                      <div className="text-gray-500">期待値</div>
                      <div className="font-semibold text-lg text-blue-600">
                        +{axisDetail.learning_avg_profit?.toFixed(2)}%
                      </div>
                    </div>
                    <div>
                      <div className="text-gray-500">サンプル数</div>
                      <div className="font-medium">{axisDetail.learning_samples}回</div>
                    </div>
                    <div>
                      <div className="text-gray-500">シャープレシオ</div>
                      <div className="font-medium">{axisDetail.learning_sharpe_ratio?.toFixed(3)}</div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
            
            <div className="space-y-4">
              <div>
                <h3 className="font-medium text-gray-900 mb-2">💡 設定アドバイス</h3>
                <div className="bg-blue-50 rounded-lg p-4">
                  <div className="text-sm text-blue-800 space-y-2">
                    <p>
                      <strong>推奨利確率:</strong> {((axisDetail?.learning_avg_profit || 1.0) * 1.5).toFixed(1)}%
                      <span className="text-xs ml-2">(期待値の1.5倍)</span>
                    </p>
                    <p>
                      <strong>推奨損切率:</strong> {Math.min(3.0, axisDetail?.learning_avg_profit || 2.0).toFixed(1)}%
                      <span className="text-xs ml-2">(期待値以下で設定)</span>
                    </p>
                    <p>
                      <strong>推奨保有期間:</strong> 3-7日
                      <span className="text-xs ml-2">(デイトレード〜スイング)</span>
                    </p>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Step 3: 条件設定 */}
      {axisDetail && (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">
            Step 3: 取引条件設定
          </h2>
          
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div className="space-y-4">
              <h3 className="font-medium text-gray-900">🎯 基本取引条件</h3>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  利確目標率 (%)
                </label>
                <input
                  type="number"
                  step="0.1"
                  min="0.1"
                  max="50"
                  value={config.profit_target_rate || 2.0}
                  onChange={(e) => handleConfigChange('profit_target_rate', parseFloat(e.target.value))}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                />
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  損切率 (%)
                </label>
                <input
                  type="number"
                  step="0.1"
                  min="0.1"
                  max="20"
                  value={config.loss_cut_rate || 2.0}
                  onChange={(e) => handleConfigChange('loss_cut_rate', parseFloat(e.target.value))}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                />
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  最大保有日数
                </label>
                <input
                  type="number"
                  min="1"
                  max="30"
                  value={config.max_hold_days || 5}
                  onChange={(e) => handleConfigChange('max_hold_days', parseInt(e.target.value))}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                />
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  ポジションサイズ率 (%)
                </label>
                <input
                  type="number"
                  step="0.1"
                  min="0.1"
                  max="100"
                  value={config.position_size_rate || 10.0}
                  onChange={(e) => handleConfigChange('position_size_rate', parseFloat(e.target.value))}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                />
              </div>
            </div>
            
            <div className="space-y-4">
              <h3 className="font-medium text-gray-900">⚙️ 詳細条件</h3>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  追加メモ
                </label>
                <textarea
                  rows={4}
                  value={config.additional_notes || ''}
                  onChange={(e) => handleConfigChange('additional_notes', e.target.value)}
                  placeholder="この設定に関するメモや注意事項..."
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                />
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  除外月 (複数選択可)
                </label>
                <div className="grid grid-cols-4 gap-2">
                  {[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12].map(month => (
                    <label key={month} className="flex items-center">
                      <input
                        type="checkbox"
                        checked={config.excluded_months?.includes(month) || false}
                        onChange={(e) => {
                          const currentExcluded = config.excluded_months || [];
                          if (e.target.checked) {
                            handleConfigChange('excluded_months', [...currentExcluded, month]);
                          } else {
                            handleConfigChange('excluded_months', currentExcluded.filter(m => m !== month));
                          }
                        }}
                        className="h-4 w-4 text-blue-600 rounded border-gray-300"
                      />
                      <span className="ml-1 text-sm">{month}月</span>
                    </label>
                  ))}
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Step 4: 確認・保存 */}
      {axisDetail && (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">
            Step 4: 設定確認・保存
          </h2>
          
          <div className="bg-gray-50 rounded-lg p-4 mb-6">
            <h3 className="font-medium text-gray-900 mb-3">📋 設定サマリー</h3>
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 text-sm">
              <div>
                <div className="text-gray-500">対象4軸</div>
                <div className="font-medium">
                  {axisDetail.signal_type}<br/>
                  Bin{axisDetail.signal_bin} / {axisDetail.trade_type}<br/>
                  {axisDetail.stock_code}
                </div>
              </div>
              <div>
                <div className="text-gray-500">利確・損切</div>
                <div className="font-medium">
                  利確: +{config.profit_target_rate || 2.0}%<br/>
                  損切: -{config.loss_cut_rate || 2.0}%
                </div>
              </div>
              <div>
                <div className="text-gray-500">保有・サイズ</div>
                <div className="font-medium">
                  最大: {config.max_hold_days || 5}日<br/>
                  サイズ: {config.position_size_rate || 10.0}%
                </div>
              </div>
              <div>
                <div className="text-gray-500">リスク・リターン予想</div>
                <div className="font-medium">
                  勝率: {axisDetail.learning_win_rate?.toFixed(1)}%<br/>
                  期待値: +{axisDetail.learning_avg_profit?.toFixed(2)}%
                </div>
              </div>
            </div>
          </div>
          
          <div className="flex space-x-4">
            <button
              onClick={handleSave}
              disabled={saving}
              className="flex-1 bg-blue-600 text-white py-3 px-6 rounded-md hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed font-medium"
            >
              {saving ? (
                <div className="flex items-center justify-center">
                  <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white mr-2"></div>
                  保存中...
                </div>
              ) : (
                '💾 条件設定を保存'
              )}
            </button>
            
            <button
              onClick={() => router.push('/signals/tomorrow')}
              className="px-6 py-3 text-gray-600 bg-gray-100 rounded-md hover:bg-gray-200 font-medium"
            >
              ⬅️ 一覧に戻る
            </button>
          </div>
          
          <div className="mt-4 p-4 bg-yellow-50 border border-yellow-200 rounded-lg">
            <div className="flex">
              <div className="flex-shrink-0">
                <svg className="h-5 w-5 text-yellow-400" fill="currentColor" viewBox="0 0 20 20">
                  <path fillRule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clipRule="evenodd" />
                </svg>
              </div>
              <div className="ml-3">
                <h3 className="text-sm font-medium text-yellow-800">
                  ⚠️ 重要な注意事項
                </h3>
                <div className="mt-2 text-sm text-yellow-700 space-y-1">
                  <p>• 一度保存した条件は後から変更できません（ゼロリセットのみ可能）</p>
                  <p>• 利確・損切の両方に到達した場合は<strong>損切が優先</strong>されます</p>
                  <p>• 設定条件は明日以降のシグナルから適用されます</p>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}