// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

module.exports = function tailwindPlugin() {
    return {
        name: "tailwind-plugin",
        configurePostCss(postcssOptions) {
            postcssOptions.plugins.push(require("@tailwindcss/postcss"));
            return postcssOptions;
        },
    };
};
