<script>
  import { Route, Router } from "svelte-routing"

  import Home from "../../routes/+page.svelte"
  import Development from "../../routes/development/+page.svelte"
  import Documentation from "../../routes/documentation/+page.svelte"
  import Tool from "../../routes/tool/+page.svelte"

  let projectRouteBase = "/the_networks_of_war"

  function shellPaths(path = "") {
    let routePath = `${projectRouteBase}${path}`

    return path ? [routePath, `${routePath}.html`] : [routePath, `${routePath}/`, `${routePath}/index.html`]
  }

  let routes = [
    { paths: shellPaths(), component: Home },
    { paths: shellPaths("/development"), component: Development },
    { paths: shellPaths("/documentation"), component: Documentation },
    { paths: shellPaths("/tool"), component: Tool }
  ]
</script>

<main>
  <Router base="/">
    {#each routes as { paths, component }}
      {#each paths as path}
        <Route {path} {component} />
      {/each}
    {/each}
  </Router>
</main>
