 // src/app/not-found.tsx
import Link from 'next/link';

export default function NotFound() {
  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-50">
      <div className="max-w-md w-full bg-white rounded-lg shadow-lg p-6 text-center">
        <div className="flex items-center justify-center w-16 h-16 mx-auto bg-gray-100 rounded-full mb-4">
          <svg
            className="w-8 h-8 text-gray-600"
            fill="none"
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth="2"
            viewBox="0 0 24 24"
            stroke="currentColor"
          >
            <path d="M9.172 16.172a4 4 0 015.656 0M9 12h6m-6-4h6m2 5.291A7.962 7.962 0 0112 15c-2.34 0-4.29-1.004-5.824-2.412M15 9.75a3 3 0 11-6 0 3 3 0 016 0z" />
          </svg>
        </div>
        
        <h1 className="text-2xl font-bold text-gray-900 mb-2">
          404
        </h1>
        <h2 className="text-lg font-medium text-gray-700 mb-4">
          ページが見つかりません
        </h2>
        <p className="text-gray-500 mb-6">
          お探しのページは存在しないか、移動された可能性があります。
        </p>

        <div className="space-y-3">
          <Link
            href="/"
            className="block w-full bg-blue-600 text-white py-3 px-4 rounded-md hover:bg-blue-700 transition-colors"
          >
            ホームに戻る
          </Link>
          <Link
            href="/signals/tomorrow"
            className="block w-full bg-gray-200 text-gray-900 py-3 px-4 rounded-md hover:bg-gray-300 transition-colors"
          >
            明日のシグナル
          </Link>
        </div>
      </div>
    </div>
  );
}