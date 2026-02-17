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
        plugin: "io.kestra.plugin.redis",
        exposes: {
            "list.ListPop": [
                {
                    uiModule: "topology-details",
                    path: "./src/components/TopologyDetailsPop.vue",
                    additionalProperties: {
                        "height": 80,
                    }
                },
                {
                    uiModule: "log-details",
                    path: "./src/components/LogDetails.vue",
                }
            ],
            "list.ListPush": [
                {
                    uiModule: "topology-details",
                    path: "./src/components/TopologyDetailsPush.vue",
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
