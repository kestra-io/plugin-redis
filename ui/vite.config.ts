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
            "io.kestra.plugin.redis.list.ListPop": [
                {

                    uiModule: "topology-details",
                    path: "./src/components/TopologyDetails.vue",
                    additionalProperties: {
                        "height": 80,
                    }
                },
                {
                    uiModule: "log-details",
                    path: "./src/components/LogDetails.vue",
                }
            ]

        }
      }),
    vue()
  ],
})
