<script>
  import { LinkButton } from "svelte-lib/components"

  import minardNapoleonCampaign from "../../lib/static/minardNapoleonCampaign.webp"
</script>

<div class="flex flex-col items-center">
  <LinkButton classes="w-56" href="/the_networks_of_war/tool" label="Begin Analysis" />
</div>
<div class="mx-auto mt-8 flex max-w-3xl flex-col items-center gap-2">
  <img
    src={minardNapoleonCampaign}
    alt="Charles Joseph Minard's 1869 flow map of Napoleon's 1812 Russian campaign. A tan band tracks the army advancing east to Moscow and a black band tracks the retreat back west, the width of each falling from 422,000 men to 10,000, above a line chart of the sub-zero temperatures recorded during the withdrawal."
    class="w-full rounded"
  />
  <p class="text-sm italic">Charles Joseph Minard's 1869 map of Napoleon's Russian campaign</p>
</div>
<p>
  I came across Minard's map in Edward R. Tufte's <em>The Visual Display of Quantitative Information</em> around the same
  time I started experimenting with D3.js. It got me hooked on the idea of a long-term project built on historical data that
  was actually historical. More than 150 years later, it still does something most charts can't. Four variables share a single
  band, and the losses aren't reported anywhere—you just watch the band narrow until there's almost nothing left.
</p>
<p>
  So I went looking for a historical dataset of my own, and after a lot of Google searches I found the
  <a href="https://correlatesofwar.org/" target="_blank" rel="noopener noreferrer">Correlates of War Project</a>. The
  data itself is modern and still actively maintained, but its coverage starts in 1816, a few years after Napoleon's
  retreat from Russia. It's the most interesting dataset I've ever worked with, and this project is my attempt to make
  two centuries of conflict as legible as Minard's line.
</p>
