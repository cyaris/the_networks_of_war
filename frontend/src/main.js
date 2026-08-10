import "svelte-lib/styles/app.css"
import "svelte-lib/styles/root.css"

import { mountEmbeddedRoot } from "svelte-lib/functions"

import Router from "./lib/components/Router.svelte"

let div = mountEmbeddedRoot({ classes: ["the-networks-of-war"], dataset: { svelteLibTooltipRoot: "true" } })

new Router({ target: div })
