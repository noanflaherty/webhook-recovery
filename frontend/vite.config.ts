import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// In production the FastAPI process serves dist/ itself, so there is no proxy
// in play. This proxy exists only so `npm run dev` talks to a locally running
// api on :8000 and the frontend uses the same relative /api/* URLs in both.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': {
        target: process.env.VITE_API_TARGET ?? 'http://localhost:8000',
        changeOrigin: true,
      },
    },
  },
})
