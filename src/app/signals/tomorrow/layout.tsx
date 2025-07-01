// src/app/signals/tomorrow/layout.tsx - 冗長ヘッダーを完全削除
export default function TomorrowSignalsLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <div>
      {children}
    </div>
  );
}