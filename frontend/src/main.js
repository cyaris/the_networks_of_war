import App from "./lib/components/App.svelte"

import "../node_modules/svelte-lib/src/lib/static/styles/root.css"
import "../node_modules/svelte-lib/src/lib/static/styles/app.css"

let div = document.createElement("div")
div.classList.add("mastermind")

let script = document.currentScript
script.parentNode.insertBefore(div, script)

new App({
  target: div,
})