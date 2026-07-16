<script>
  import { Route, Router } from "svelte-routing"

  import Home from "../../routes/+page.svelte"
  import Development from "../../routes/development/+page.svelte"
  import Documentation from "../../routes/documentation/+page.svelte"
  import Tool from "../../routes/tool/+page.svelte"
</script>

<main>
  <Router base="/">
    <Route path="/the_networks_of_war" component={Home} />
    <Route path="/the_networks_of_war/development" component={Development} />
    <Route path="/the_networks_of_war/documentation" component={Documentation} />
    <Route path="/the_networks_of_war/tool" component={Tool} />
  </Router>
</main>
