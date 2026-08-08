import { createViteConfig } from "svelte-lib/vite.config.js"

/** @type {import('vite').UserConfig} */
export const config = createViteConfig({ ssr: { noExternal: ["chart.js"] } })

export default config
