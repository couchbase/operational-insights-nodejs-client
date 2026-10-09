import { defineConfig } from 'tsup'

export default defineConfig({
  entry: ['lib/operationalinsights.ts'],
  format: ['cjs', 'esm'],
  dts: {
    compilerOptions: {
      // tsup always sets `baseUrl` for the declaration build, which TypeScript 6
      // deprecates. Our tsconfig.json does not use it.
      ignoreDeprecations: '6.0',
    },
  },
  clean: true,
  sourcemap: true,
})
