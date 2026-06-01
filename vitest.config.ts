import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['packages/**/src/**/*.test.ts'],
    exclude: ['**/node_modules/**', '**/dist/**', 'examples/**'],
    testTimeout: 30_000,
    hookTimeout: 30_000,
  },
});
