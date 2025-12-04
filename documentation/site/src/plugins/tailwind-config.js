module.exports = function tailwindPlugin() {
  return {
    name: "tailwind-plugin",
    configurePostCss(postcssOptions) {
    postcssOptions.plugins.push(require("@tailwindcss/postcss"));
    return postcssOptions;
    },
  };
};
