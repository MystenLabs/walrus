// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//
// Convenience wrapper: runs fetch + transform for Walrus Memory docs.
// Passes through CLI args (e.g. --force) to the fetch script.

const { execSync } = require("child_process");
const path = require("path");

const scriptDir = __dirname;
const args = process.argv.slice(2).join(" ");

execSync(`node ${path.join(scriptDir, "fetch-walrus-memory-docs.js")} ${args}`, {
  stdio: "inherit",
});
execSync(`node ${path.join(scriptDir, "transform-walrus-memory-docs.js")}`, {
  stdio: "inherit",
});
