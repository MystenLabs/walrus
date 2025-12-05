// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

const fs = require("fs");
const path = require("path");
const yaml = require("js-yaml");

// Define source and destination directories
const dataDir = path.join(__dirname, "../../../data");
const staticDir = path.join(__dirname, "../../static");

// Ensure static directory exists
if (!fs.existsSync(staticDir)) {
    fs.mkdirSync(staticDir, { recursive: true });
}

// Get all YAML files from data directory
function getYamlFiles(dir) {
    const files = [];
    const items = fs.readdirSync(dir);

    for (const item of items) {
        const fullPath = path.join(dir, item);
        const stat = fs.statSync(fullPath);

        if (stat.isDirectory()) {
            // Recursively get YAML files from subdirectories
            files.push(...getYamlFiles(fullPath));
        } else if (item.endsWith(".yaml") || item.endsWith(".yml")) {
            files.push(fullPath);
        }
    }

    return files;
}

// Convert YAML files to JSON and copy to static directory
function convertYamlToJson() {
    try {
        const yamlFiles = getYamlFiles(dataDir);

        if (yamlFiles.length === 0) {
            console.log("No YAML files found in data directory");
            return;
        }

        console.log(`Found ${yamlFiles.length} YAML file(s):`);

        for (const yamlFile of yamlFiles) {
            const relativePath = path.relative(dataDir, yamlFile);
            const fileNameWithoutExt = path.basename(yamlFile, path.extname(yamlFile));
            const destPath = path.join(staticDir, `${fileNameWithoutExt}.json`);

            // Read and parse YAML file
            const yamlContent = fs.readFileSync(yamlFile, "utf8");
            const jsonData = yaml.load(yamlContent);

            // Convert to JSON and write to static directory
            const jsonContent = JSON.stringify(jsonData, null, 2);
            fs.writeFileSync(destPath, jsonContent);

            console.log(`  Converted: ${relativePath} -> ${fileNameWithoutExt}.json`);
        }

        console.log("All YAML files converted to JSON successfully!");
    } catch (error) {
        console.error("Error converting YAML files:", error.message);
        process.exit(1);
    }
}

// Run the script
convertYamlToJson();
