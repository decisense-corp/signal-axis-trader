// src/app/layout.tsx
import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'Signal Axis Trader',
  description: '株式取引システム - 4軸分析による個別判断システム',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="ja">
      <body className="bg-gray-50 text-gray-900">
        {/* 最小限ヘッダー */}
        <header className="bg-white border-b border-gray-200">
          <div className="max-w-7xl mx-auto px-4">
            <div className="flex justify-between items-center h-10">
              <div className="flex items-center">
                <h1 className="text-sm font-medium text-gray-900">
                  Signal Axis Trader
                </h1>
              </div>
              <nav className="flex space-x-2">
                <a
                  href="/signals/tomorrow"
                  className="text-gray-600 hover:text-gray-900 px-2 py-1 text-xs font-medium"
                >
                  明日のシグナル
                </a>
                <a
                  href="/signals/unprocessed"
                  className="text-gray-600 hover:text-gray-900 px-2 py-1 text-xs font-medium"
                >
                  未処理4軸
                </a>
                <a
                  href="/signals/review"
                  className="text-gray-600 hover:text-gray-900 px-2 py-1 text-xs font-medium"
                >
                  振り返り
                </a>
              </nav>
            </div>
          </div>
        </header>
        
        {/* メインコンテンツ */}
        <main className="max-w-7xl mx-auto px-4 py-3">
          {children}
        </main>
      </body>
    </html>
  );
}