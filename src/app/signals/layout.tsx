// src/app/signals/layout.tsx - 冗長ヘッダーを完全削除
export default function SignalsLayout({
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