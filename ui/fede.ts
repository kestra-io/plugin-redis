import { federation } from "@module-federation/vite";
import * as fs from "fs";

function getNameFromGradleSettings() {
    const lines = fs.readFileSync("../settings.gradle").toString().split("\n");
    for (const line of lines) {
        if (line.trim().startsWith("rootProject.name")) {
            const name = line.split("=").pop()?.trim().slice(1, -1)
            if(name) {
                return name;
            }
        }
    }
    throw new Error("Could not extract project name from settings.gradle");
}

const KNOWN_KEYS = ["topology-details", "log-details"];

export default ({ exposes, plugin }: {
    plugin: string,
    exposes: Record<string, Array<{
        uiModule:string,
        path: string,
        additionalProperties?: Record<string, any>
    }>> }) => {
    const manifest: Record<string, Array<{ uiModule: string, staticInfo?: Record<string, any> }>> = {}
    for (const task in exposes) {
        const completeTaskName = `${plugin}.${task}`;
        const manifestTask = manifest[completeTaskName] ?? [];
        for (const module of exposes[task]) {
            if (!KNOWN_KEYS.includes(module.uiModule)) {
                throw new Error(
                    `The key "${module.uiModule}" is unknown. Allowed keys are: ${KNOWN_KEYS.join(", ")}`,
                );
            }
            manifestTask.push({
                uiModule: module.uiModule,
                staticInfo: module.additionalProperties,
            });
        }
        manifest[completeTaskName] = manifestTask;
    }

    // create the directory ../src/main/resources/plugin-ui/ if it doesn't exist
    if (!fs.existsSync("../src/main/resources/plugin-ui/")) {
        fs.mkdirSync("../src/main/resources/plugin-ui/", { recursive: true });
    }
    fs.writeFileSync("../src/main/resources/plugin-ui/manifest.json", JSON.stringify(manifest, null, 2));

    return federation({
        filename: "plugin-ui.js",
        name: getNameFromGradleSettings(),
        exposes: Object.entries(exposes).reduce((acc, [task, modules]) => {
            for (const { uiModule, path } of modules) {
                // "./list.ListPop/topology-details" for example
                acc[`./${task}/${uiModule}`] = path;
            }
            return acc;
        }, {} as Record<string, string>),
        shared: {
            vue: {
                singleton: true,
                requiredVersion: "^3",
            },
        },
    });
};
