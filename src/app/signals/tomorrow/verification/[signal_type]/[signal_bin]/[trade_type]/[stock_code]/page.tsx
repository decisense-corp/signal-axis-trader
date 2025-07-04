// src/app/signals/tomorrow/verification/[signal_type]/[signal_bin]/[trade_type]/[stock_code]/page.tsx
'use client';

import { useEffect, useState } from 'react';
import { useParams, useRouter, useSearchParams } from 'next/navigation';

interface SignalInfo {
  signal_type: string;
  signal_bin: number;
  trade_type: 'BUY' | 'SELL';
  stock_code: string;
  stock_name: string;
}

interface StatsSummary {
  total_samples: number;
  win_rate: number;
  avg_profit_rate: number;
}

interface ComparisonStats {
  learning_period: {
    baseline: StatsSummary;
    filtered: StatsSummary;
  };
  verification_period: {
    baseline: StatsSummary;
    filtered: StatsSummary;
  };
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

interface VerificationData {
  signal_info: SignalInfo;
  filter_conditions: {
    profit_target_yen: number | null;
    loss_cut_yen: number | null;
    prev_close_gap_condition: string;
  };
  comparison_stats: ComparisonStats;
  verification_detail_data: DetailData[];
}

export default function VerificationPage() {
  const params = useParams();
  const router = useRouter();
  const searchParams = useSearchParams();
  
  const [data, setData] = useState<VerificationData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [sortField, setSortField] = useState<string>('signal_date');
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('desc');
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    fetchVerificationData();
  }, [params, searchParams]);

  const fetchVerificationData = async () => {
    try {
      setLoading(true);
      setError(null);

      const queryParams = new URLSearchParams({
        signal_type: decodeURIComponent(params.signal_type as string),
        signal_bin: params.signal_bin as string,
        trade_type: params.trade_type as string,
        stock_code: params.stock_code as string,
        profit_target_yen: searchParams.get('profit_target_yen') || '0',
        loss_cut_yen: searchParams.get('loss_cut_yen') || '0',
        prev_close_gap_condition: searchParams.get('prev_close_gap_condition') || 'all'
      });

      const response = await fetch(`/api/signals/verification?${queryParams}`);
      
      if (!response.ok) {
        throw new Error('データの取得に失敗しました');
      }

      const result = await response.json();
      setData(result);
    } catch (err) {
      console.error('取得エラー:', err);
      setError(err instanceof Error ? err.message : '不明なエラーが発生しました');
    } finally {
      setLoading(false);
    }
  };

  const handleSort = (field: string) => {
    if (sortField === field) {
      setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortOrder('desc');
    }
  };

  const sortedDetailData = data?.verification_detail_data.sort((a, b) => {
    const aValue = a[sortField as keyof DetailData];
    const bValue = b[sortField as keyof DetailData];
    
    if (sortOrder === 'asc') {
      return aValue > bValue ? 1 : -1;
    } else {
      return aValue < bValue ? 1 : -1;
    }
  });

  const formatConditions = (conditions: VerificationData['filter_conditions']) => {
    const parts = [];
    if (conditions.profit_target_yen) {
      parts.push(`利確目標: ${conditions.profit_target_yen}円`);
    }
    if (conditions.loss_cut_yen) {
      parts.push(`損切設定: ${conditions.loss_cut_yen}円`);
    }
    if (conditions.prev_close_gap_condition !== 'all') {
      parts.push(`前日終値ギャップ: ${conditions.prev_close_gap_condition === 'above' ? 'プラスのみ' : 'マイナスのみ'}`);
    }
    return parts.length > 0 ? parts.join(' / ') : '条件なし';
  };

  const calculateChange = (learning: number, verification: number) => {
    return verification - learning;
  };

  const getChangeColor = (metric: string, change: number) => {
    if (Math.abs(change) < 0.1) return 'text-yellow-600';
    
    if (metric === 'samples') {
      return 'text-gray-600'; // サンプル数の変化は色分けしない
    }
    
    // 勝率と利益率は増加が良い
    return change > 0 ? 'text-green-600' : 'text-red-600';
  };

  const handleDecision = async (decision: 'configured' | 'rejected' | 'pending') => {
    if (!data) return;
    
    try {
      setSaving(true);
      
      const body = {
        signal_type: data.signal_info.signal_type,
        signal_bin: data.signal_info.signal_bin,
        trade_type: data.signal_info.trade_type,
        stock_code: data.signal_info.stock_code,
        decision_status: decision,
        profit_target_yen: data.filter_conditions.profit_target_yen,
        loss_cut_yen: data.filter_conditions.loss_cut_yen,
        prev_close_gap_condition: data.filter_conditions.prev_close_gap_condition,
        additional_notes: `検証期間確認画面から${decision}に設定`
      };

      const response = await fetch('/api/decisions', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(body)
      });

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.error || 'エラーが発生しました');
      }

      // 4軸一覧画面に戻る
      router.push('/signals/tomorrow');
    } catch (err) {
      console.error('保存エラー:', err);
      alert(err instanceof Error ? err.message : 'エラーが発生しました');
    } finally {
      setSaving(false);
    }
  };

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-xl">読み込み中...</div>
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-xl text-red-600">{error || 'データがありません'}</div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-7xl mx-auto p-6">
        {/* ヘッダー */}
        <div className="bg-white rounded-lg shadow-md p-6 mb-6">
          <h1 className="text-2xl font-bold mb-4">検証期間確認画面</h1>
          
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4">
            <div>
              <div className="text-sm text-gray-600">シグナルタイプ</div>
              <div className="font-semibold">{data.signal_info.signal_type}</div>
            </div>
            <div>
              <div className="text-sm text-gray-600">ビン番号</div>
              <div className="font-semibold">{data.signal_info.signal_bin}</div>
            </div>
            <div>
              <div className="text-sm text-gray-600">売買方向</div>
              <div className="font-semibold">{data.signal_info.trade_type}</div>
            </div>
            <div>
              <div className="text-sm text-gray-600">銘柄</div>
              <div className="font-semibold">{data.signal_info.stock_code} {data.signal_info.stock_name}</div>
            </div>
          </div>

          <div className="bg-blue-50 p-3 rounded">
            <div className="text-sm text-gray-600">設定条件</div>
            <div className="font-semibold">{formatConditions(data.filter_conditions)}</div>
          </div>
        </div>

        {/* 比較統計表示 */}
        <div className="bg-white rounded-lg shadow-md p-6 mb-6">
          <h2 className="text-xl font-bold mb-4">学習期間 vs 検証期間 比較</h2>
          
          <div className="overflow-x-auto">
            <table className="w-full border-collapse">
              <thead>
                <tr>
                  <th className="border p-2 text-left bg-gray-50">項目</th>
                  <th className="border p-2 text-center bg-gray-50">学習期間</th>
                  <th className="border p-2 text-center bg-gray-50">検証期間</th>
                  <th className="border p-2 text-center bg-gray-50">変化</th>
                </tr>
              </thead>
              <tbody>
                {/* 寄引統計 */}
                <tr className="bg-blue-50">
                  <td className="border p-2 font-semibold" colSpan={4}>【寄引】</td>
                </tr>
                <tr>
                  <td className="border p-2">サンプル数</td>
                  <td className="border p-2 text-center">{data.comparison_stats.learning_period.baseline.total_samples}件</td>
                  <td className="border p-2 text-center">{data.comparison_stats.verification_period.baseline.total_samples}件</td>
                  <td className={`border p-2 text-center ${getChangeColor('samples', 
                    calculateChange(data.comparison_stats.learning_period.baseline.total_samples, 
                                  data.comparison_stats.verification_period.baseline.total_samples))}`}>
                    {data.comparison_stats.verification_period.baseline.total_samples - 
                     data.comparison_stats.learning_period.baseline.total_samples}件
                  </td>
                </tr>
                <tr>
                  <td className="border p-2">勝率</td>
                  <td className="border p-2 text-center">{data.comparison_stats.learning_period.baseline.win_rate}%</td>
                  <td className="border p-2 text-center">{data.comparison_stats.verification_period.baseline.win_rate}%</td>
                  <td className={`border p-2 text-center ${getChangeColor('win_rate',
                    calculateChange(data.comparison_stats.learning_period.baseline.win_rate,
                                  data.comparison_stats.verification_period.baseline.win_rate))}`}>
                    {(data.comparison_stats.verification_period.baseline.win_rate - 
                      data.comparison_stats.learning_period.baseline.win_rate).toFixed(1)}%
                  </td>
                </tr>
                <tr>
                  <td className="border p-2">平均利益率</td>
                  <td className="border p-2 text-center">{data.comparison_stats.learning_period.baseline.avg_profit_rate}%</td>
                  <td className="border p-2 text-center">{data.comparison_stats.verification_period.baseline.avg_profit_rate}%</td>
                  <td className={`border p-2 text-center ${getChangeColor('avg_profit_rate',
                    calculateChange(data.comparison_stats.learning_period.baseline.avg_profit_rate,
                                  data.comparison_stats.verification_period.baseline.avg_profit_rate))}`}>
                    {(data.comparison_stats.verification_period.baseline.avg_profit_rate - 
                      data.comparison_stats.learning_period.baseline.avg_profit_rate).toFixed(2)}%
                  </td>
                </tr>

                {/* フィルタ統計（条件設定時のみ表示） */}
                {(data.filter_conditions.profit_target_yen || 
                  data.filter_conditions.loss_cut_yen || 
                  data.filter_conditions.prev_close_gap_condition !== 'all') && (
                  <>
                    <tr className="bg-green-50">
                      <td className="border p-2 font-semibold" colSpan={4}>【フィルタ】</td>
                    </tr>
                    <tr>
                      <td className="border p-2">サンプル数</td>
                      <td className="border p-2 text-center">{data.comparison_stats.learning_period.filtered.total_samples}件</td>
                      <td className="border p-2 text-center">{data.comparison_stats.verification_period.filtered.total_samples}件</td>
                      <td className={`border p-2 text-center ${getChangeColor('samples',
                        calculateChange(data.comparison_stats.learning_period.filtered.total_samples,
                                      data.comparison_stats.verification_period.filtered.total_samples))}`}>
                        {data.comparison_stats.verification_period.filtered.total_samples - 
                         data.comparison_stats.learning_period.filtered.total_samples}件
                      </td>
                    </tr>
                    <tr>
                      <td className="border p-2">勝率</td>
                      <td className="border p-2 text-center">{data.comparison_stats.learning_period.filtered.win_rate}%</td>
                      <td className="border p-2 text-center">{data.comparison_stats.verification_period.filtered.win_rate}%</td>
                      <td className={`border p-2 text-center ${getChangeColor('win_rate',
                        calculateChange(data.comparison_stats.learning_period.filtered.win_rate,
                                      data.comparison_stats.verification_period.filtered.win_rate))}`}>
                        {(data.comparison_stats.verification_period.filtered.win_rate - 
                          data.comparison_stats.learning_period.filtered.win_rate).toFixed(1)}%
                      </td>
                    </tr>
                    <tr>
                      <td className="border p-2">平均利益率</td>
                      <td className="border p-2 text-center">{data.comparison_stats.learning_period.filtered.avg_profit_rate}%</td>
                      <td className="border p-2 text-center">{data.comparison_stats.verification_period.filtered.avg_profit_rate}%</td>
                      <td className={`border p-2 text-center ${getChangeColor('avg_profit_rate',
                        calculateChange(data.comparison_stats.learning_period.filtered.avg_profit_rate,
                                      data.comparison_stats.verification_period.filtered.avg_profit_rate))}`}>
                        {(data.comparison_stats.verification_period.filtered.avg_profit_rate - 
                          data.comparison_stats.learning_period.filtered.avg_profit_rate).toFixed(2)}%
                      </td>
                    </tr>
                  </>
                )}
              </tbody>
            </table>
          </div>
        </div>

        {/* 検証期間詳細データ */}
        <div className="bg-white rounded-lg shadow-md p-6 mb-6">
          <h2 className="text-xl font-bold mb-4">検証期間詳細データ</h2>
          
          <div className="overflow-x-auto">
            <table className="w-full border-collapse text-sm">
              <thead>
                <tr>
                  <th 
                    className="border p-2 bg-gray-50 cursor-pointer hover:bg-gray-100"
                    onClick={() => handleSort('signal_date')}
                  >
                    日付 {sortField === 'signal_date' && (sortOrder === 'asc' ? '▲' : '▼')}
                  </th>
                  <th 
                    className="border p-2 bg-gray-50 cursor-pointer hover:bg-gray-100"
                    onClick={() => handleSort('prev_close_to_open_gap')}
                  >
                    前日終値→始 {sortField === 'prev_close_to_open_gap' && (sortOrder === 'asc' ? '▲' : '▼')}
                  </th>
                  <th 
                    className="border p-2 bg-gray-50 cursor-pointer hover:bg-gray-100"
                    onClick={() => handleSort('open_to_high_gap')}
                  >
                    始→高 {sortField === 'open_to_high_gap' && (sortOrder === 'asc' ? '▲' : '▼')}
                  </th>
                  <th 
                    className="border p-2 bg-gray-50 cursor-pointer hover:bg-gray-100"
                    onClick={() => handleSort('open_to_low_gap')}
                  >
                    始→安 {sortField === 'open_to_low_gap' && (sortOrder === 'asc' ? '▲' : '▼')}
                  </th>
                  <th 
                    className="border p-2 bg-gray-50 cursor-pointer hover:bg-gray-100"
                    onClick={() => handleSort('open_to_close_gap')}
                  >
                    始→終 {sortField === 'open_to_close_gap' && (sortOrder === 'asc' ? '▲' : '▼')}
                  </th>
                  <th 
                    className="border p-2 bg-gray-50 cursor-pointer hover:bg-gray-100"
                    onClick={() => handleSort('baseline_profit_rate')}
                  >
                    寄引損益率 {sortField === 'baseline_profit_rate' && (sortOrder === 'asc' ? '▲' : '▼')}
                  </th>
                  <th 
                    className="border p-2 bg-gray-50 cursor-pointer hover:bg-gray-100"
                    onClick={() => handleSort('filtered_profit_rate')}
                  >
                    フィルタ損益率 {sortField === 'filtered_profit_rate' && (sortOrder === 'asc' ? '▲' : '▼')}
                  </th>
                  <th 
                    className="border p-2 bg-gray-50 cursor-pointer hover:bg-gray-100"
                    onClick={() => handleSort('trading_volume')}
                  >
                    売買代金 {sortField === 'trading_volume' && (sortOrder === 'asc' ? '▲' : '▼')}
                  </th>
                </tr>
              </thead>
              <tbody>
                {sortedDetailData?.map((row, idx) => (
                  <tr key={idx} className={idx % 2 === 0 ? 'bg-gray-50' : ''}>
                    <td className="border p-2 text-center">{row.signal_date}</td>
                    <td className="border p-2 text-right">{row.prev_close_to_open_gap.toFixed(2)}</td>
                    <td className="border p-2 text-right">{row.open_to_high_gap.toFixed(2)}</td>
                    <td className="border p-2 text-right">{row.open_to_low_gap.toFixed(2)}</td>
                    <td className="border p-2 text-right">{row.open_to_close_gap.toFixed(2)}</td>
                    <td className={`border p-2 text-right ${row.baseline_profit_rate > 0 ? 'text-green-600' : 'text-red-600'}`}>
                      {row.baseline_profit_rate.toFixed(2)}%
                    </td>
                    <td className={`border p-2 text-right ${row.filtered_profit_rate > 0 ? 'text-green-600' : 'text-red-600'}`}>
                      {row.filtered_profit_rate.toFixed(2)}%
                    </td>
                    <td className="border p-2 text-right">{row.trading_volume.toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* アクションボタン */}
        <div className="bg-white rounded-lg shadow-md p-6">
          <div className="flex justify-center space-x-4">
            <button
              onClick={() => handleDecision('configured')}
              disabled={saving}
              className="px-8 py-3 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50 disabled:cursor-not-allowed font-semibold"
            >
              やる
            </button>
            <button
              onClick={() => handleDecision('rejected')}
              disabled={saving}
              className="px-8 py-3 bg-red-600 text-white rounded-lg hover:bg-red-700 disabled:opacity-50 disabled:cursor-not-allowed font-semibold"
            >
              やらない
            </button>
            <button
              onClick={() => handleDecision('pending')}
              disabled={saving}
              className="px-8 py-3 bg-yellow-600 text-white rounded-lg hover:bg-yellow-700 disabled:opacity-50 disabled:cursor-not-allowed font-semibold"
            >
              次回再検討
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}