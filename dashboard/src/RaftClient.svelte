<script>
  import GetEntry from "./GetEntry.svelte";
  import SetEntry from "./SetEntry.svelte";
  import RemoveEntry from "./RemoveEntry.svelte";
  import Lifecycle from "./Lifecycle.svelte";
  export let client;
  let key = "key";
</script>

<div class="card">
  <div class="card-header">
    <p class="card-header-title">{client.url}</p>
  </div>
  <div class="card-content">
    <div class="content">
      <SetEntry {client} />
    </div>
    <div class="content">
      <GetEntry {client} />
    </div>
    <div class="content">
      <RemoveEntry {client} />
    </div>
    <div class="content">
      <Lifecycle {client} />
    </div>
  </div>
</div>
