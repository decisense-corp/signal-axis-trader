// src/app/loading.tsx
export default function Loading() {
  return (
    <div className="flex items-center justify-center min-h-screen">
      <div className="animate-spin rounded-full h-32 w-32 border-b-2 border-blue-500"></div>
      <div className="ml-4">
        <p className="text-lg font-medium text-gray-900">読み込み中...</p>
        <p className="text-sm text-gray-500">しばらくお待ちください</p>
      </div>
    </div>
  );
}