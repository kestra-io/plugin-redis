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

export default ({ exposes }: { exposes: Record<string, { path: string, additionalProperties?: Record<string, any> }> }) => {
    for (const key in exposes) {
        const shortKey = key.split("/").pop();
        if(shortKey === undefined) {
            throw new Error(`Invalid key "${key}". It should contain at least one "/".`);
        }

        if (!KNOWN_KEYS.includes(shortKey)) {
            throw new Error(
                `The key "${key}" is unknown. Allowed keys are: ${KNOWN_KEYS.join(", ")}`,
            );
        }
    }
    const manifest =
        Object.entries(exposes).map(([key, value]) => (
            {
                path: key,
                staticInfo: value.additionalProperties
            })
    );
    // create the directory ../src/main/resources/plugin-ui/ if it doesn't exist
    if (!fs.existsSync("../src/main/resources/plugin-ui/")) {
        fs.mkdirSync("../src/main/resources/plugin-ui/", { recursive: true });
    }
    fs.writeFileSync("../src/main/resources/plugin-ui/manifest.json", JSON.stringify(manifest, null, 2));

    return federation({
        filename: "plugin-ui.js",
        name: getNameFromGradleSettings(),
        exposes: Object.fromEntries(
            Object.entries(exposes).map(([key, value]) => [key, value.path])
        ),
        shared: {
            vue: {
                singleton: true,
                requiredVersion: "^3",
            },
        },
    });
};
