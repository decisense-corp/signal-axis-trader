// src/lib/bigquery.ts
import { BigQuery } from '@google-cloud/bigquery';

export class BigQueryClient {
  private client: BigQuery;
  private projectId: string;
  private datasetId: string;

  constructor() {
    this.projectId = process.env.GOOGLE_CLOUD_PROJECT_ID || 'kabu-376213';
    this.datasetId = process.env.GOOGLE_CLOUD_DATASET_ID || 'kabu2411';
    
    this.client = new BigQuery({
      projectId: this.projectId,
      keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS || './service-account-key.json',
    });
  }

  /**
   * SQLクエリを実行
   */
  async query(sql: string): Promise<any[]> {
    try {
      console.log('🔍 BigQuery実行中:', sql.substring(0, 100) + '...');
      
      const [rows] = await this.client.query({
        query: sql,
        useLegacySql: false,
        location: 'asia-northeast2',
      });

      console.log(`✅ クエリ完了: ${rows.length}件のデータを取得`);
      return rows;
    } catch (error) {
      console.error('❌ BigQueryエラー:', error);
      throw new Error(`BigQuery実行エラー: ${error instanceof Error ? error.message : '不明なエラー'}`);
    }
  }

  /**
   * 取引対象銘柄マスタを取得
   */
  async getTradingStockMaster(): Promise<any[]> {
    const sql = `
      SELECT 
        stock_code,
        company_name,
        market_code,
        market_name,
        sector17_code,
        sector17_name,
        trading_days,
        avg_trading_value_million,
        avg_intraday_volatility,
        volatility_decile
      FROM \`${this.projectId}.${this.datasetId}.master_trading_stocks\`
      ORDER BY stock_code
    `;
    
    return this.query(sql);
  }

  /**
   * 4軸パフォーマンス統計を取得
   */
  async getSignalPerformance4Axis(filters?: {
    stockCode?: string;
    tradeType?: 'Buy' | 'Sell';
    minWinRate?: number;
    minSampleCount?: number;
  }): Promise<any[]> {
    let whereConditions = ['1=1'];
    
    if (filters?.stockCode) {
      whereConditions.push(`stock_code = '${filters.stockCode}'`);
    }
    
    if (filters?.tradeType) {
      whereConditions.push(`trade_type = '${filters.tradeType}'`);
    }
    
    if (filters?.minWinRate) {
      whereConditions.push(`win_rate >= ${filters.minWinRate}`);
    }
    
    if (filters?.minSampleCount) {
      whereConditions.push(`sample_count >= ${filters.minSampleCount}`);
    }

    const sql = `
      SELECT 
        signal_type,
        signal_bin,
        trade_type,
        stock_code,
        stock_name,
        sample_count,
        win_count,
        win_rate,
        expected_return,
        avg_profit_rate,
        median_profit_rate,
        std_profit_rate,
        sharpe_ratio,
        max_profit_rate,
        min_profit_rate,
        recent_30d_count,
        recent_30d_win_rate,
        recent_30d_avg_profit,
        last_signal_date,
        calculation_date
      FROM \`${this.projectId}.${this.datasetId}.d02_signal_performance_4axis\`
      WHERE ${whereConditions.join(' AND ')}
      ORDER BY win_rate DESC, expected_return DESC
    `;
    
    return this.query(sql);
  }

  /**
   * 明日のシグナル候補を取得
   */
  async getTomorrowSignalCandidates(): Promise<any[]> {
    const sql = `
      WITH tomorrow_signals AS (
        SELECT DISTINCT
          sr.stock_code,
          tsm.company_name as stock_name,
          CASE 
            WHEN sr.signal_value > 0 THEN 'Buy'
            ELSE 'Sell'
          END as trade_type
        FROM \`${this.projectId}.${this.datasetId}.d01_signals_raw\` sr
        INNER JOIN \`${this.projectId}.${this.datasetId}.master_trading_stocks\` tsm
          ON sr.stock_code = tsm.stock_code
        WHERE sr.signal_date = DATE_ADD(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 DAY)
          AND sr.signal_value IS NOT NULL
      )
      
      SELECT
        ts.stock_code,
        ts.stock_name,
        ts.trade_type,
        COUNT(*) as signal_count
      FROM tomorrow_signals ts
      GROUP BY ts.stock_code, ts.stock_name, ts.trade_type
      ORDER BY ts.stock_code, ts.trade_type
    `;
    
    return this.query(sql);
  }

  /**
   * シグナルタイプマスタを取得
   */
  async getSignalTypes(): Promise<any[]> {
    const sql = `
      SELECT 
        signal_type,
        signal_category,
        description,
        calculation_method,
        priority_rank,
        is_active,
        is_score_type
      FROM \`${this.projectId}.${this.datasetId}.m01_signal_types\`
      WHERE is_active = true
      ORDER BY priority_rank, signal_type
    `;
    
    return this.query(sql);
  }

  /**
   * 接続テスト（テーブル存在確認なし）
   */
  async testConnectionSimple(): Promise<{ success: boolean; message: string; tableCount?: number; tables?: string[] }> {
    try {
      console.log('🔍 BigQuery接続テスト開始...');
      
      const [datasets] = await this.client.getDatasets();
      console.log(`📊 データセット数: ${datasets.length}`);
      
      const targetDataset = datasets.find(d => d.id === this.datasetId);
      
      if (!targetDataset) {
        return {
          success: false,
          message: `Dataset '${this.datasetId}' が見つかりません`
        };
      }

      const [tables] = await targetDataset.getTables();
      const tableNames = tables.map(t => t.id).filter((id): id is string => id !== undefined);
      
      console.log(`✅ テーブル確認完了: ${tables.length}個`);
      
      return {
        success: true,
        message: `BigQuery接続成功: ${tables.length}個のテーブルを確認`,
        tableCount: tables.length,
        tables: tableNames
      };
    } catch (error) {
      console.error('❌ BigQuery接続エラー:', error);
      return {
        success: false,
        message: `BigQuery接続エラー: ${error instanceof Error ? error.message : '不明なエラー'}`
      };
    }
  }

  /**
   * テーブルの存在確認
   */
  async checkTableExists(tableName: string): Promise<boolean> {
    try {
      const table = this.client.dataset(this.datasetId).table(tableName);
      const [exists] = await table.exists();
      return exists;
    } catch (error) {
      console.warn(`テーブル存在確認エラー (${tableName}):`, error);
      return false;
    }
  }

  /**
   * データセット・プロジェクト情報を取得
   */
  getConnectionInfo() {
    return {
      projectId: this.projectId,
      datasetId: this.datasetId,
      fullDatasetId: `${this.projectId}.${this.datasetId}`
    };
  }
}