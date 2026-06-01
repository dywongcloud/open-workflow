import type { ReactNode } from 'react';

export const metadata = {
  title: 'open-workflow · Next.js example',
  description: 'Durable workflows on Redis + 307, vendor-agnostic.',
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
