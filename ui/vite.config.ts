import { defineConfig } from 'vite'
import fede from "./fede";
import vue from '@vitejs/plugin-vue'

// https://vite.dev/config/
export default defineConfig({
  build: {
    outDir: "../src/main/resources/plugin-ui",
  },
  plugins: [
    fede({
        exposes: {
          "./topology-details": {
            path:"./src/components/TopologyDetails.vue",
            additionalProperties: {
              "height": "400px",
              "width": "600px"
            }
          },
          "./log-details": {
            path:"./src/components/TopologyDetails.vue",
          },
        },
      }),
    vue()
  ],
})
