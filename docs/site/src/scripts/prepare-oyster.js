// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//
// Convenience wrapper: runs fetch + transform + sidebar generation for Oyster docs.
// Passes through CLI args (e.g. --force) to the fetch script.

const { execSync } = require("child_process");
const path = require("path");

const scriptDir = __dirname;
const args = process.argv.slice(2).join(" ");

execSync(`node ${path.join(scriptDir, "fetch-oyster-docs.js")} ${args}`, {
  stdio: "inherit",
});
execSync(`node ${path.join(scriptDir, "transform-oyster-docs.js")}`, {
  stdio: "inherit",
});
execSync(`node ${path.join(scriptDir, "generate-oyster-sidebar.js")}`, {
  stdio: "inherit",
});
