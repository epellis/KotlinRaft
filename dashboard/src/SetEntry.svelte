<script>
  export let client;
  const { Entry, SetStatus } = require("./control_pb.js");
  const { ControlPromiseClient } = require("./control_grpc_web_pb.js");
  const stub = new ControlPromiseClient(client.url, null, null);

  let entryKey = "";
  let entryValue = "";

  async function setEntry() {
    console.log(`Setting Key=${entryKey}, Value=${entryValue}`);
    const request = new Entry();
    request.setKey(entryKey);
    request.setValue(entryValue);
    const res = await stub.setEntry(request, {});
    const status = res.getStatus() == SetStatus.Status.OK;
    console.log(`Status: ${status}`);
  }
</script>

<div class="container">
  <div class="field">
    <label class="label">Key</label>
    <input class="input" type="text" bind:value={entryKey} />
  </div>
  <div class="field">
    <label class="label">Value</label>
    <input class="input" type="text" bind:value={entryValue} />
  </div>
  <div class="field is-grouped">
    <button class="button" on:click={setEntry}>Set Entry</button>
  </div>
</div>
