<script>
  //   const {
  //     Entry,
  //     GetStatus,
  //     Key,
  //     RemoveStatus,
  //     SetStatus
  //   } = require("./control_pb.js");
  //   const { ControlClient } = require("./control_grpc_web_pb.js");
  import GetEntry from "./GetEntry.svelte";
  import SetEntry from "./SetEntry.svelte";
  import RemoveEntry from "./RemoveEntry.svelte";
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
  </div>
</div>
