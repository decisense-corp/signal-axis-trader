 
/** @type {import('next').NextConfig} */
const nextConfig = {
  experimental: {
    // App Routerの最適化
    appDir: true,
  },
  // TypeScript厳密設定
  typescript: {
    ignoreBuildErrors: false,
  },
  // ESLint設定
  eslint: {
    ignoreDuringBuilds: false,
  },
  // 環境変数設定
  env: {
    CUSTOM_KEY: process.env.CUSTOM_KEY,
  },
  // 画像最適化
  images: {
    domains: [],
    formats: ['image/webp', 'image/avif'],
  },
  // セキュリティヘッダー
  async headers() {
    return [
      {
        source: '/(.*)',
        headers: [
          {
            key: 'X-Frame-Options',
            value: 'DENY',
          },
          {
            key: 'X-Content-Type-Options',
            value: 'nosniff',
          },
          {
            key: 'Referrer-Policy',
            value: 'strict-origin-when-cross-origin',
          },
        ],
      },
    ];
  },
  // BigQuery API用のrewrite設定（将来的に必要になる可能性）
  async rewrites() {
    return [
      {
        source: '/api/bigquery/:path*',
        destination: '/api/bigquery/:path*',
      },
    ];
  },
};

module.exports = nextConfig;