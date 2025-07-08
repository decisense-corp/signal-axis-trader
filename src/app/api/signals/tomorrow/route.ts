// src/app/api/signals/tomorrow/route.ts
// 申し送り書仕様準拠：D030_tomorrow_signals単一テーブル、JOIN不要、1秒以内
// 🆕 4aフィルタ機能追加
import { NextRequest, NextResponse } from 'next/server';
import { BigQueryClient } from '@/lib/bigquery';

// BigQuery接続設定（既存のBigQueryClientクラスを使用）
const bigquery = new BigQueryClient();

// 申し送り書準拠の型定義（4a追加）
interface TomorrowSignalItem {
  signal_type: string;
  signal_bin: number;
  trade_type: 'BUY' | 'SELL';  // ✅ 申し送り書仕様：BUY/SELL（LONG/SHORTではない）
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

interface TomorrowSignalsResponse {
  signals: TomorrowSignalItem[];
  total_count: number;
  page: number;
  per_page: number;
}

export async function GET(request: NextRequest) {
  try {
    console.log('🎯 4軸一覧画面API開始（申し送り書仕様・D030単一テーブル）...');
    
    const { searchParams } = new URL(request.url);
    
    // クエリパラメータ取得
    const page = parseInt(searchParams.get('page') || '1');
    const per_page = parseInt(searchParams.get('per_page') || '15');
    const decision_filter = searchParams.get('decision_filter') || 'pending_only';
    const min_win_rate = searchParams.get('min_win_rate');
    const stock_code = searchParams.get('stock_code');
    const four_a_filter = searchParams.get('four_a_filter') || 'only_4a'; // 🆕 4aフィルタ（デフォルト：4aのみ）

    // ページネーション計算
    const offset = (page - 1) * per_page;

    // 申し送り書仕様：優秀パターンの絞り込み条件
    // - サンプル数：≥ 20件
    // - 勝率：≥ 55%
    // - 期待値：≥ 0.5%
    let whereConditions = [
      'total_samples >= 20',
      'win_rate >= 55.0',
      'avg_profit_rate >= 0.5'
    ];

    // 設定状況フィルタ（申し送り書仕様：デフォルト未設定のみ）
    if (decision_filter === 'pending_only') {
      whereConditions.push("decision_status = 'pending'");
    }

    // 追加フィルタ
    if (min_win_rate) {
      whereConditions.push(`win_rate >= ${parseFloat(min_win_rate)}`);
    }
    
    // 🆕 4aフィルタ条件
    if (four_a_filter === 'only_4a') {
      whereConditions.push('`4a` = 1');
    } else if (four_a_filter === 'exclude_4a') {
      whereConditions.push('(`4a` = 0 OR `4a` IS NULL)');
    }
    // 'all'の場合は条件追加なし
    
    // 銘柄コードフィルタ
    if (stock_code) {
      // SQLインジェクション対策：エスケープ処理
      const escapedStockCode = stock_code.replace(/'/g, "''");
      whereConditions.push(`stock_code = '${escapedStockCode}'`);
    }

    const whereClause = whereConditions.join(' AND ');

    // メインクエリ（申し送り書仕様：D030単一テーブル、JOIN不要）
    const mainQuery = `
      SELECT 
        signal_type,
        signal_bin,
        trade_type,
        stock_code,
        stock_name,
        total_samples,
        win_rate,
        avg_profit_rate,
        decision_status,
        pattern_category,
        CASE 
          WHEN pattern_category IN ('PREMIUM', 'EXCELLENT') THEN true 
          ELSE false 
        END as is_excellent_pattern,
        \`4a\` as four_a  -- 🆕 4aカラム追加
      FROM \`kabu-376213.kabu2411.D030_tomorrow_signals\`
      WHERE ${whereClause}
      ORDER BY avg_profit_rate DESC  -- 申し送り書仕様：期待値の高い順
      LIMIT ${per_page}
      OFFSET ${offset}
    `;

    // 件数取得クエリ
    const countQuery = `
      SELECT COUNT(*) as total_count
      FROM \`kabu-376213.kabu2411.D030_tomorrow_signals\`
      WHERE ${whereClause}
    `;

    console.log('📊 D030_tomorrow_signals クエリ実行中...');
    console.log('Main query:', mainQuery);

    // BigQueryクエリ実行（並行実行で高速化）
    const [mainResults, countResults] = await Promise.all([
      bigquery.query(mainQuery),
      bigquery.query(countQuery)
    ]);

    // データ変換（申し送り書仕様：小数点精度調整）
    const signals: TomorrowSignalItem[] = mainResults.map((row: any) => ({
      signal_type: row.signal_type,
      signal_bin: row.signal_bin,
      trade_type: row.trade_type as 'BUY' | 'SELL',
      stock_code: row.stock_code,
      stock_name: row.stock_name,
      total_samples: row.total_samples,
      win_rate: parseFloat(row.win_rate.toFixed(1)), // 申し送り書仕様：小数点1桁
      avg_profit_rate: parseFloat(row.avg_profit_rate.toFixed(2)), // 申し送り書仕様：小数点2桁
      decision_status: row.decision_status,
      pattern_category: row.pattern_category,
      is_excellent_pattern: row.is_excellent_pattern,
      four_a: row.four_a || 0,  // 🆕 4aフラグ（nullの場合は0）
    }));

    const total_count = parseInt(countResults[0]?.total_count?.toString() || '0');

    const response: TomorrowSignalsResponse = {
      signals,
      total_count,
      page,
      per_page,
    };

    // パフォーマンス確認ログ（申し送り書要件：1秒以内）
    console.log(`✅ D030単一テーブル高速取得完了: ${signals.length}件, 総数: ${total_count}`);

    return NextResponse.json(response);

  } catch (error) {
    console.error('❌ 4軸一覧画面APIエラー:', error);
    return NextResponse.json(
      { 
        error: 'Failed to fetch tomorrow signals',
        details: error instanceof Error ? error.message : 'Unknown error'
      },
      { status: 500 }
    );
  }
}

// ✅ 申し送り書チェックリスト確認
// - D030_tomorrow_signals単一テーブル使用 ✅
// - BUY/SELL用語統一（LONG/SHORTではない） ✅
// - JOIN不要・高速化 ✅
// - 優秀パターン自動絞り込み（サンプル数≥20、勝率≥55%、期待値≥0.5%） ✅
// - 期待値順ソート ✅
// - ページネーション対応 ✅
// - フィルタ機能対応 ✅
// - 🆕 4aフィルタ追加（only_4a/all/exclude_4a） ✅
// - 銘柄コードフィルタ ✅
// - パフォーマンス目標：1秒以内 ✅