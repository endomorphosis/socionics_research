import { defineConfig } from 'vite';

export default defineConfig({
  root: '.',
  server: {
    port: 5173,
    open: true,
  },
  optimizeDeps: {
  include: ['hnswlib-wasm', './hnswlib_loader.js'],
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    rollupOptions: {
      input: {
        main: 'index.html',
  worker: 'vec_worker.js',
      },
      output: {
        // Preserve readable names without hashes so worker relative imports resolve
        entryFileNames: 'assets/[name].js',
        chunkFileNames: 'assets/[name].js',
        assetFileNames: 'assets/[name][extname]'
      }
    },
  },
});
