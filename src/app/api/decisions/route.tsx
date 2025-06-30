// src/app/api/decisions/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { BigQueryClient } from '@/lib/bigquery';

interface DecisionConfig {
  signal_type: string;
  signal_bin: number;
  trade_type: 'BUY' | 'SELL';
  stock_code: string;
  profit_target_rate: number;    // 利確目標率（%）
  loss_cut_rate: number;         // 損切率（%）
  max_hold_days: number;         // 最大保有日数
  position_size_rate: number;    // ポジションサイズ率（%）
  min_signal_strength?: number;  // 最小シグナル強度（オプション）
  excluded_months?: number[];    // 除外月配列（オプション）
  additional_notes?: string;     // 追加メモ（オプション）
}

interface DecisionResponse {
  decision_id: string;
  created_at: string;
  success: boolean;
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    
    console.log('💾 条件設定保存処理開始...');
    
    // 基本バリデーション
    const {
      signal_type,
      signal_bin,
      trade_type,
      stock_code,
      profit_target_rate,
      loss_cut_rate,
      max_hold_days,
      position_size_rate,
      min_signal_strength = 0,
      excluded_months = [],
      additional_notes = ''
    } = body as DecisionConfig;
    
    // 必須フィールドのバリデーション
    if (!signal_type || !signal_bin || !trade_type || !stock_code) {
      return NextResponse.json({
        success: false,
        error: '必須フィールドが不足しています: signal_type, signal_bin, trade_type, stock_code'
      }, { status: 400 });
    }
    
    if (!profit_target_rate || !loss_cut_rate || !max_hold_days || !position_size_rate) {
      return NextResponse.json({
        success: false,
        error: '条件設定が不足しています: profit_target_rate, loss_cut_rate, max_hold_days, position_size_rate'
      }, { status: 400 });
    }
    
    // 値の範囲チェック
    if (signal_bin < 1 || signal_bin > 20) {
      return NextResponse.json({
        success: false,
        error: 'signal_binは1-20の範囲である必要があります'
      }, { status: 400 });
    }
    
    if (profit_target_rate <= 0 || profit_target_rate > 50) {
      return NextResponse.json({
        success: false,
        error: '利確目標率は0より大きく50%以下である必要があります'
      }, { status: 400 });
    }
    
    if (loss_cut_rate <= 0 || loss_cut_rate > 20) {
      return NextResponse.json({
        success: false,
        error: '損切率は0より大きく20%以下である必要があります'
      }, { status: 400 });
    }
    
    if (max_hold_days < 1 || max_hold_days > 30) {
      return NextResponse.json({
        success: false,
        error: '最大保有日数は1-30日の範囲である必要があります'
      }, { status: 400 });
    }
    
    if (position_size_rate <= 0 || position_size_rate > 100) {
      return NextResponse.json({
        success: false,
        error: 'ポジションサイズ率は0より大きく100%以下である必要があります'
      }, { status: 400 });
    }
    
    // trade_typeの正規化
    const normalizedTradeType = trade_type.toUpperCase();
    if (!['BUY', 'SELL'].includes(normalizedTradeType)) {
      return NextResponse.json({
        success: false,
        error: 'trade_type は BUY または SELL である必要があります'
      }, { status: 400 });
    }

    const bigquery = new BigQueryClient();
    
    // decision_idを生成（4軸の組み合わせをハッシュ化）
    const decision_id = `${signal_type}_${signal_bin}_${normalizedTradeType}_${stock_code}`;
    const timestamp = new Date().toISOString();
    
    // 既存データの確認
    const checkQuery = `
      SELECT decision_id, decision_status
      FROM \`kabu-376213.kabu2411.u10_user_decisions\`
      WHERE decision_id = '${decision_id}'
    `;
    
    const existingData = await bigquery.query(checkQuery);
    
    let operation = 'INSERT';
    if (existingData.length > 0) {
      const status = existingData[0].decision_status;
      if (status === 'configured') {
        return NextResponse.json({
          success: false,
          error: 'この4軸の条件は既に確定済みです。変更するにはゼロリセットが必要です。',
          details: `decision_id: ${decision_id}`
        }, { status: 409 });
      }
      operation = 'UPDATE';
    }
    
    // 参照データを取得（学習期間統計）
    const refDataQuery = `
      SELECT 
        win_rate as ref_learning_win_rate,
        avg_profit_rate as ref_learning_avg_profit,
        total_signals as ref_learning_total_signals,
        stock_name
      FROM \`kabu-376213.kabu2411.d30_learning_period_snapshot\`
      WHERE signal_type = '${signal_type}'
        AND signal_bin = ${signal_bin}
        AND trade_type = '${normalizedTradeType}'
        AND stock_code = '${stock_code}'
    `;
    
    const refData = await bigquery.query(refDataQuery);
    
    if (refData.length === 0) {
      return NextResponse.json({
        success: false,
        error: '指定された4軸パターンの学習データが見つかりません',
        details: `${signal_type}/${signal_bin}/${normalizedTradeType}/${stock_code}`
      }, { status: 404 });
    }
    
    const ref = refData[0];
    
    // INSERT/UPDATE クエリの構築
    let query: string;
    
    if (operation === 'INSERT') {
      query = `
        INSERT INTO \`kabu-376213.kabu2411.u10_user_decisions\`
        (
          decision_id,
          signal_type,
          signal_bin,
          trade_type,
          stock_code,
          stock_name,
          is_active,
          decision_status,
          profit_target_rate,
          loss_cut_rate,
          max_hold_days,
          position_size_rate,
          min_signal_strength,
          excluded_months,
          additional_notes,
          ref_learning_win_rate,
          ref_learning_avg_profit,
          ref_learning_total_signals,
          ref_pattern_category,
          created_by,
          created_at,
          updated_at
        )
        VALUES
        (
          '${decision_id}',
          '${signal_type}',
          ${signal_bin},
          '${normalizedTradeType}',
          '${stock_code}',
          '${ref.stock_name}',
          true,
          'pending',
          ${profit_target_rate},
          ${loss_cut_rate},
          ${max_hold_days},
          ${position_size_rate},
          ${min_signal_strength},
          [${excluded_months.join(',')}],
          '${additional_notes.replace(/'/g, "''")}',
          ${ref.ref_learning_win_rate},
          ${ref.ref_learning_avg_profit},
          ${ref.ref_learning_total_signals},
          CASE 
            WHEN ${ref.ref_learning_win_rate} >= 70.0 AND ${ref.ref_learning_avg_profit} >= 1.0 THEN '超優秀'
            WHEN ${ref.ref_learning_win_rate} >= 65.0 AND ${ref.ref_learning_avg_profit} >= 0.8 THEN '優秀'
            WHEN ${ref.ref_learning_win_rate} >= 60.0 AND ${ref.ref_learning_avg_profit} >= 0.6 THEN '良好'
            WHEN ${ref.ref_learning_win_rate} >= 55.0 AND ${ref.ref_learning_avg_profit} >= 0.5 THEN '標準'
            ELSE '要注意'
          END,
          'system',
          TIMESTAMP('${timestamp}'),
          TIMESTAMP('${timestamp}')
        )
      `;
    } else {
      query = `
        UPDATE \`kabu-376213.kabu2411.u10_user_decisions\`
        SET
          profit_target_rate = ${profit_target_rate},
          loss_cut_rate = ${loss_cut_rate},
          max_hold_days = ${max_hold_days},
          position_size_rate = ${position_size_rate},
          min_signal_strength = ${min_signal_strength},
          excluded_months = [${excluded_months.join(',')}],
          additional_notes = '${additional_notes.replace(/'/g, "''")}',
          updated_at = TIMESTAMP('${timestamp}')
        WHERE decision_id = '${decision_id}'
      `;
    }

    console.log(`📊 条件設定${operation}実行中...`);
    
    await bigquery.query(query);
    
    const response: DecisionResponse = {
      decision_id,
      created_at: timestamp,
      success: true
    };

    console.log(`✅ 条件設定${operation}完了: ${decision_id}`);

    return NextResponse.json({
      success: true,
      data: response,
      metadata: {
        operation,
        decision_id,
        signal_type,
        signal_bin,
        trade_type: normalizedTradeType,
        stock_code,
        timestamp,
        description: `4軸条件設定${operation === 'INSERT' ? '新規作成' : '更新'}完了`
      }
    });

  } catch (error) {
    console.error('❌ 条件設定保存エラー:', error);
    
    return NextResponse.json({
      success: false,
      error: '条件設定の保存に失敗しました',
      details: error instanceof Error ? error.message : '不明なエラー'
    }, { status: 500 });
  }
}

export async function GET(request: NextRequest) {
  try {
    console.log('📋 条件設定一覧取得開始...');
    
    const { searchParams } = new URL(request.url);
    const stock_code = searchParams.get('stock_code');
    const trade_type = searchParams.get('trade_type');
    const status = searchParams.get('status');
    
    const bigquery = new BigQueryClient();
    
    let whereConditions = ['1=1'];
    
    if (stock_code) {
      whereConditions.push(`stock_code = '${stock_code}'`);
    }
    
    if (trade_type) {
      const normalizedTradeType = trade_type.toUpperCase();
      whereConditions.push(`trade_type = '${normalizedTradeType}'`);
    }
    
    if (status) {
      whereConditions.push(`decision_status = '${status}'`);
    }
    
    const query = `
      SELECT
        decision_id,
        signal_type,
        signal_bin,
        trade_type,
        stock_code,
        stock_name,
        decision_status,
        profit_target_rate,
        loss_cut_rate,
        max_hold_days,
        position_size_rate,
        ref_learning_win_rate,
        ref_learning_avg_profit,
        ref_learning_total_signals,
        ref_pattern_category,
        created_at,
        updated_at
      FROM \`kabu-376213.kabu2411.u10_user_decisions\`
      WHERE ${whereConditions.join(' AND ')}
      ORDER BY updated_at DESC
    `;
    
    const results = await bigquery.query(query);
    
    console.log(`✅ 条件設定一覧 ${results.length}件を取得`);
    
    return NextResponse.json({
      success: true,
      data: results,
      metadata: {
        count: results.length,
        filters: { stock_code, trade_type, status },
        query_time: new Date().toISOString()
      }
    });
    
  } catch (error) {
    console.error('❌ 条件設定一覧取得エラー:', error);
    
    return NextResponse.json({
      success: false,
      error: '条件設定一覧の取得に失敗しました',
      details: error instanceof Error ? error.message : '不明なエラー'
    }, { status: 500 });
  }
}