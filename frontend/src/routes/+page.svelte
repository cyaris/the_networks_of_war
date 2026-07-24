<script>
  import { Button } from "svelte-lib/components"
</script>

<div class="mb-8 flex flex-col items-center">
  <Button classes="w-56" href="tool" label="Begin Analysis" />
  <div class="mt-2 grid gap-x-1 gap-y-2 sm:grid-cols-2">
    <Button classes="w-56" href="development" label="Development" />
    <Button classes="w-56" href="documentation" label="Documentation" />
  </div>
</div>
