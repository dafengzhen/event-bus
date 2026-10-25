import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    clearMocks: true,
    coverage: {
      exclude: ['src/**/*.test.ts'],
      include: ['src/**/*.ts'],
      provider: 'v8',
      reporter: ['text', 'html', 'lcov'],
      thresholds: {
        branches: 85,
        functions: 90,
        lines: 90,
        statements: 90,
      },
    },
    environment: 'node',
    include: ['src/**/*.test.ts', 'examples/**/*.test.ts'],
  },
});
