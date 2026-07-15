<script>
  import { Button } from "svelte-lib/components"

  export let active = "browser"

  let navItems = [
    { key: "browser", label: "Browser", href: "/the_networks_of_war" },
    { key: "data", label: "Data", href: "/the_networks_of_war/data" },
    { key: "development", label: "Development", href: "/the_networks_of_war/development" },
  ]
</script>

<div class="min-h-screen">
  <header class="mx-auto flex w-full max-w-7xl flex-col items-center gap-3 px-4 py-6 text-center sm:px-6">
    <div>
      <h1 class="text-3xl font-extrabold">The Networks of War</h1>
      <p class="mt-1 font-semibold text-themegray-11">War Network Browser</p>
    </div>
    <div class="flex flex-wrap justify-center gap-2">
      {#each navItems as item (item.key)}
        <Button
          href={item.href}
          label={item.label}
          classes="w-[150px] {active == item.key ? 'border-[#c44536]' : ''}"
        />
      {/each}
    </div>
  </header>

  <slot />
</div>
