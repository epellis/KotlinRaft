<script>
  export let client;
  import { getNotificationsContext } from "svelte-notifications";
  const { Key, Entry, GetStatus } = require("./control_pb.js");
  const { ControlPromiseClient } = require("./control_grpc_web_pb.js");

  const stub = new ControlPromiseClient(client.url, null, null);
  const { addNotification } = getNotificationsContext();

  let entryKey = "";

  async function setEntry() {
    try {
      const request = new Key();
      request.setKey(entryKey);
      const res = await stub.getEntry(request, {});
      //   const status = res.getStatus() == GetStatus.Status.OK;
      if (res.getStatus() == GetStatus.Status.NOT_FOUND) {
        addNotification({
          text: "Not Found",
          position: "bottom-center",
          type: "warning",
          removeAfter: 1000
        });
      }
      if (res.getStatus() == GetStatus.Status.OK) {
        addNotification({
          text: `${entryKey}\t=>\t${res.getValue()}`,
          position: "bottom-center",
          type: "success",
          removeAfter: 3000
        });
      }
    } catch (e) {
      console.log(e);
      addNotification({
        text: e.message,
        position: "bottom-center",
        type: "danger"
      });
    }
  }
</script>

<div class="container">
  <div class="field">
    <label class="label">Key</label>
    <input class="input" type="text" bind:value={entryKey} />
  </div>
  <div class="field is-grouped">
    <button class="button" on:click={setEntry}>Get Entry</button>
  </div>
</div>
