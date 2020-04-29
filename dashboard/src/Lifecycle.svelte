<script>
  export let client;
  const { Nothing } = require("./control_pb.js");
  const { ControlPromiseClient } = require("./control_grpc_web_pb.js");

  const stub = new ControlPromiseClient(client.url, null, null);

  async function doIdle() {
    const request = new Nothing();
    const res = await stub.idleClient(request, {});
  }
  async function doWake() {
    const request = new Nothing();
    const res = await stub.wakeClient(request, {});
  }
</script>

<div class="container">
  <button class="button" on:click={doIdle}>Idle</button>
  <button class="button" on:click={doWake}>Wake</button>
</div>
