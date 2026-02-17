import { defineConfig } from 'vite'
import { federation } from "@module-federation/vite";
import vue from '@vitejs/plugin-vue'

// https://vite.dev/config/
export default defineConfig({
  build: {
    outDir: "../src/main/resources/plugin-ui",
  },
  plugins: [
    federation({
        filename: "plugin-ui.js",
        name: "plugin-redis",
        exposes: {
          "./topology-details": "./src/components/TopologyDetails.vue",
        },
        shared: {
          vue: {
            singleton: true,
            requiredVersion: "^3"
          },
        }
      }),
    vue()
  ],
})
