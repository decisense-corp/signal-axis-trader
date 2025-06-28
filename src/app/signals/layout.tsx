 // src/app/signals/layout.tsx
export default function SignalsLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <div className="space-y-6">
      {/* シグナル機能共通ヘッダー */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold text-gray-900">
              シグナル分析システム
            </h1>
            <p className="text-gray-600 mt-1">
              4軸分析による個別判断型取引システム
            </p>
          </div>
          
          {/* ナビゲーション */}
          <nav className="flex space-x-4">
            <a
              href="/signals/tomorrow"
              className="px-4 py-2 text-sm font-medium text-blue-600 bg-blue-50 rounded-md hover:bg-blue-100"
            >
              明日のシグナル
            </a>
            <a
              href="/signals/unprocessed"
              className="px-4 py-2 text-sm font-medium text-gray-600 bg-gray-50 rounded-md hover:bg-gray-100"
            >
              未処理4軸
            </a>
            <a
              href="/signals/review"
              className="px-4 py-2 text-sm font-medium text-gray-600 bg-gray-50 rounded-md hover:bg-gray-100"
            >
              振り返り
            </a>
          </nav>
        </div>
      </div>

      {/* ページコンテンツ */}
      <div className="space-y-6">
        {children}
      </div>
    </div>
  );
}