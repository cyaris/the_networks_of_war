<script>
  import { Route, Router } from "svelte-routing"

  import Browser from "../../routes/+page.svelte"
  import Data from "../../routes/data/+page.svelte"
  import Development from "../../routes/development/+page.svelte"
</script>

<main>
  <Router base="/">
    <Route path="/the_networks_of_war" component={Browser} />
    <Route path="/the_networks_of_war/data" component={Data} />
    <Route path="/the_networks_of_war/development" component={Development} />
  </Router>
</main>