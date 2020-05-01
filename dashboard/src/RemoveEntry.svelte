<script>
  export let client;
  import { getNotificationsContext } from "svelte-notifications";
  const { Entry, UpdateStatus } = require("./control_pb.js");
  const { ControlPromiseClient } = require("./control_grpc_web_pb.js");

  const stub = new ControlPromiseClient(client.url, null, null);
  const { addNotification } = getNotificationsContext();

  let entryKey = "";

  async function setEntry() {
    try {
      const request = new Entry();
      request.setKey(entryKey);
      request.setAction(Entry.Action.REMOVE);
      const res = await stub.updateEntry(request, {});
      if (res.getStatus() == UpdateStatus.Status.OK) {
        addNotification({
          text: "removed",
          position: "bottom-center",
          type: "success",
          removeAfter: 1000
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
    <button class="button" on:click={setEntry}>Remove Entry</button>
  </div>
</div>
