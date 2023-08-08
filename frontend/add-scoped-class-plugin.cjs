module.exports = (opts = { class: "the-networks-of-war" }) => {
  return {
    postcssPlugin: "add-scoped-class",
    Rule(rule) {
      rule.selectors = rule.selectors.map(selector => `.${opts.class} ${selector}`)
    },
  }
}

module.exports.postcss = true
