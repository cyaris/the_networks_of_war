<script>
  import * as d3 from "d3"
  import Pluralize from "pluralize"

  import { tooltip } from "svelte-lib/functions"
  import { Select, Text } from "svelte-lib/components"

  const moduleNames = ['static/json/war_num_1_0.json']

let modules = {}

for (const moduleName of moduleNames) {
    import module from './' + moduleName
    modules.moduleName = module
}

// export modules
console.log(modules)

</script>
