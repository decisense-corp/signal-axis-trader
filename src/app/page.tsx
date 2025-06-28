 // src/app/page.tsx
export default function HomePage() {
  return (
    <div className="space-y-8">
      <div className="text-center">
        <h1 className="text-3xl font-bold text-gray-900 mb-4">
          Signal Axis Trader
        </h1>
        <p className="text-lg text-gray-600">
          株式取引システム - 4軸分析による個別判断システム
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h2 className="text-xl font-semibold text-gray-900 mb-4">
            機能1: 明日のシグナル処理
          </h2>
          <p className="text-gray-600 mb-4">
            明日発生するシグナルの条件設定
          </p>
          <a 
            href="/signals/tomorrow"
            className="bg-blue-600 hover:bg-blue-700 text-white font-medium py-2 px-4 rounded-md transition-colors w-full text-center block"
          >
            明日のシグナル一覧
          </a>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h2 className="text-xl font-semibold text-gray-900 mb-4">
            機能2: 未処理4軸処理
          </h2>
          <p className="text-gray-600 mb-4">
            優秀パフォーマンス + 未設定4軸の事前準備
          </p>
          <a 
            href="/signals/unprocessed"
            className="bg-gray-200 hover:bg-gray-300 text-gray-900 font-medium py-2 px-4 rounded-md transition-colors w-full text-center block"
          >
            未処理4軸一覧
          </a>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h2 className="text-xl font-semibold text-gray-900 mb-4">
            機能3: 振り返り処理
          </h2>
          <p className="text-gray-600 mb-4">
            設定済みルールの見直し
          </p>
          <a 
            href="/signals/review"
            className="bg-gray-200 hover:bg-gray-300 text-gray-900 font-medium py-2 px-4 rounded-md transition-colors w-full text-center block"
          >
            振り返り一覧
          </a>
        </div>
      </div>

      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">接続テスト</h3>
        <div className="flex space-x-4">
          <a 
            href="/api/test-bigquery" 
            target="_blank"
            className="bg-gray-200 hover:bg-gray-300 text-gray-900 font-medium py-2 px-4 rounded-md transition-colors"
          >
            BigQuery接続確認
          </a>
          <a 
            href="/api/signals/tomorrow" 
            target="_blank"
            className="bg-gray-200 hover:bg-gray-300 text-gray-900 font-medium py-2 px-4 rounded-md transition-colors"
          >
            明日のシグナルAPI
          </a>
        </div>
      </div>
    </div>
  );
}