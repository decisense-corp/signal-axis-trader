 // src/app/signals/tomorrow/layout.tsx
export default function TomorrowSignalsLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <div className="space-y-4">
      {/* ページヘッダー */}
      <div className="bg-green-50 border border-green-200 rounded-lg p-4">
        <div className="flex items-center">
          <div className="flex-shrink-0">
            <svg
              className="h-5 w-5 text-green-400"
              fill="currentColor"
              viewBox="0 0 20 20"
            >
              <path
                fillRule="evenodd"
                d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z"
                clipRule="evenodd"
              />
            </svg>
          </div>
          <div className="ml-3">
            <h2 className="text-lg font-medium text-green-900">
              機能1: 明日のシグナル処理
            </h2>
            <p className="text-sm text-green-700">
              明日発生するシグナルの条件設定 - 新規条件設定（寄り条件、利確、損切）
            </p>
          </div>
        </div>
      </div>

      {/* ページコンテンツ */}
      {children}
    </div>
  );
}