// src/app/signals/tomorrow/config/[signal_type]/[signal_bin]/[trade_type]/[stock_code]/page.tsx
// 申し送り書仕様準拠：基本的な条件設定画面
'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';

// 申し送り書準拠の型定義
interface RouteParams {
  signal_type: string;
  signal_bin: string;
  trade_type: string;
  stock_code: string;
}

interface ConfigPageProps {
  params: Promise<RouteParams>;
}

export default function ConfigPage({ params }: ConfigPageProps) {
  const router = useRouter();
  
  // Route params解決
  const [routeParams, setRouteParams] = useState<RouteParams | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // 申し送り書仕様：条件設定項目
  const [profitTargetYen, setProfitTargetYen] = useState<number>(0);
  const [lossCutYen, setLossCutYen] = useState<number>(0);
  const [prevCloseGapCondition, setPrevCloseGapCondition] = useState<'all' | 'above' | 'below'>('all');
  const [additionalNotes, setAdditionalNotes] = useState<string>('');

  // Route params解決
  useEffect(() => {
    const resolveParams = async () => {
      try {
        const resolvedParams = await params;
        setRouteParams(resolvedParams);
        setLoading(false);
      } catch (err) {
        setError('URLパラメータの解決に失敗しました');
        setLoading(false);
      }
    };
    
    resolveParams();
  }, [params]);

  // 保存処理
  const handleSave = async () => {
    if (!routeParams) return;
    
    try {
      setSaving(true);
      setError(null);
      
      const requestBody = {
        signal_type: decodeURIComponent(routeParams.signal_type), // URLデコード追加
        signal_bin: parseInt(routeParams.signal_bin),
        trade_type: routeParams.trade_type,
        stock_code: routeParams.stock_code,
        profit_target_yen: profitTargetYen,
        loss_cut_yen: lossCutYen,
        prev_close_gap_condition: prevCloseGapCondition,
        additional_notes: additionalNotes || `設定日時: ${new Date().toLocaleString()}`
      };

      console.log('💾 条件設定保存リクエスト:', requestBody);

      const response = await fetch('/api/decisions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody),
      });
      
      const result = await response.json();
      
      if (result.success) {
        alert('✅ 条件設定が保存されました！');
        // 申し送り書仕様：明日のシグナルトップに戻る
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

  // キャンセル処理
  const handleCancel = () => {
    router.push('/signals/tomorrow');
  };

  if (loading) {
    return (
      <div className="container mx-auto px-4 py-6 max-w-4xl">
        <div className="text-center py-8">
          <div className="text-gray-500">読み込み中...</div>
        </div>
      </div>
    );
  }

  if (!routeParams) {
    return (
      <div className="container mx-auto px-4 py-6 max-w-4xl">
        <div className="bg-red-50 border border-red-200 rounded-md p-4">
          <div className="text-red-800">
            <strong>エラー:</strong> URLパラメータが無効です
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="container mx-auto px-4 py-6 max-w-4xl">
      {/* ヘッダー */}
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-900 mb-2">
          ⚙️ 条件設定
        </h1>
        <div className="text-sm text-gray-600 space-y-1">
          <div><strong>シグナルタイプ:</strong> {decodeURIComponent(routeParams.signal_type)}</div>
          <div><strong>ビン番号:</strong> {routeParams.signal_bin}</div>
          <div><strong>売買方向:</strong> 
            <span className={`ml-2 inline-flex px-2 py-1 text-xs font-semibold rounded-full ${
              routeParams.trade_type === 'BUY' 
                ? 'bg-green-100 text-green-800' 
                : 'bg-red-100 text-red-800'
            }`}>
              {routeParams.trade_type}
            </span>
          </div>
          <div><strong>銘柄コード:</strong> {routeParams.stock_code}</div>
        </div>
      </div>

      {/* エラー表示 */}
      {error && (
        <div className="bg-red-50 border border-red-200 rounded-md p-4 mb-6">
          <div className="text-red-800">
            <strong>エラー:</strong> {error}
          </div>
        </div>
      )}

      {/* 申し送り書仕様：条件設定フォーム */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <h2 className="text-lg font-semibold text-gray-900 mb-6">取引条件設定</h2>
        
        <div className="space-y-6">
          {/* 利確目標 */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              利確目標（円）
            </label>
            <div className="flex items-center space-x-3">
              <input
                type="number"
                value={profitTargetYen}
                onChange={(e) => setProfitTargetYen(parseInt(e.target.value) || 0)}
                placeholder="50"
                className="flex-1 rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
              />
              <span className="text-sm text-gray-500">円 (0 = 設定なし)</span>
            </div>
            <p className="mt-1 text-xs text-gray-500">
              例: 50円 = 株価が50円上昇したら利確
            </p>
          </div>

          {/* 損切設定 */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              損切設定（円）
            </label>
            <div className="flex items-center space-x-3">
              <input
                type="number"
                value={lossCutYen}
                onChange={(e) => setLossCutYen(parseInt(e.target.value) || 0)}
                placeholder="30"
                className="flex-1 rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
              />
              <span className="text-sm text-gray-500">円 (0 = 設定なし)</span>
            </div>
            <p className="mt-1 text-xs text-gray-500">
              例: 30円 = 株価が30円下落したら損切
            </p>
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
              <option value="all">すべて（ギャップ条件なし）</option>
              <option value="above">前日終値より上で約定</option>
              <option value="below">前日終値より下で約定</option>
            </select>
            <p className="mt-1 text-xs text-gray-500">
              前日終値と当日始値の関係による取引条件
            </p>
          </div>

          {/* 追加メモ */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              追加メモ（任意）
            </label>
            <textarea
              value={additionalNotes}
              onChange={(e) => setAdditionalNotes(e.target.value)}
              placeholder="特記事項があれば記入してください..."
              rows={3}
              className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
            />
          </div>
        </div>
      </div>

      {/* 現在の設定状態表示 */}
      <div className="bg-blue-50 border border-blue-200 rounded-md p-4 mt-6">
        <h3 className="text-sm font-semibold text-blue-900 mb-2">設定内容確認</h3>
        <div className="text-sm text-blue-800 space-y-1">
          <div>
            <strong>利確目標:</strong> {profitTargetYen === 0 ? '設定なし' : `${profitTargetYen}円`}
          </div>
          <div>
            <strong>損切設定:</strong> {lossCutYen === 0 ? '設定なし' : `${lossCutYen}円`}
          </div>
          <div>
            <strong>ギャップ条件:</strong> {
              prevCloseGapCondition === 'all' ? 'すべて' :
              prevCloseGapCondition === 'above' ? '前日終値より上' :
              '前日終値より下'
            }
          </div>
        </div>
      </div>

      {/* アクションボタン */}
      <div className="flex justify-between items-center pt-6">
        <button
          onClick={handleCancel}
          className="px-6 py-2 border border-gray-300 rounded-md text-gray-700 bg-white hover:bg-gray-50 font-medium transition-colors"
        >
          ← 一覧に戻る
        </button>
        
        <button
          onClick={handleSave}
          disabled={saving}
          className="px-6 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white font-medium rounded-md transition-colors flex items-center space-x-2"
        >
          {saving ? (
            <>
              <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
              <span>保存中...</span>
            </>
          ) : (
            <>
              <span>💾</span>
              <span>条件設定を保存</span>
            </>
          )}
        </button>
      </div>

      {/* 申し送り書仕様：操作ガイド */}
      <div className="bg-gray-50 border border-gray-200 rounded-md p-4 mt-6">
        <h3 className="text-sm font-semibold text-gray-900 mb-2">💡 操作ガイド</h3>
        <div className="text-sm text-gray-600 space-y-1">
          <div>• <strong>利確・損切</strong>: 0円 = 設定なし（大引け手仕舞い）</div>
          <div>• <strong>ギャップ条件</strong>: 前日終値との関係で取引条件を絞り込み</div>
          <div>• <strong>保存後</strong>: 明日のシグナル一覧に戻ります</div>
          <div>• <strong>変更</strong>: 一度設定すると変更には注意が必要です</div>
        </div>
      </div>
    </div>
  );
}

// ✅ 申し送り書チェックリスト確認
// - 利確・損切・ギャップ条件設定 ✅
// - BUY/SELL用語統一 ✅
// - 申し送り書仕様の画面遷移（一覧→設定→一覧） ✅
// - 保存ボタン・キャンセルボタン ✅
// - 設定内容確認表示 ✅
// - 操作ガイド表示 ✅
// - エラーハンドリング ✅
// - レスポンシブ対応 ✅