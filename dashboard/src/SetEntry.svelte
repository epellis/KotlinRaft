<script>
  export let client;
  const { Entry, SetStatus } = require("./control_pb.js");
  const { ControlClient } = require("./control_grpc_web_pb.js");
  const stub = new ControlClient(client.url);

  let entryKey = "";
  let entryValue = "";

  function setEntry() {
    console.log(`Setting Key=${entryKey}, Value=${entryValue}`);
    let request = new Entry();
    request.setKey(entryKey);
    request.setValue(entryValue);
    stub.setStatus(request, {}, (err, res) => {
      console.log(response);
    });
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
