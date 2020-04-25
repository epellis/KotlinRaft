<script>
  const {
    Entry,
    GetStatus,
    Key,
    RemoveStatus,
    SetStatus
  } = require("./control_pb.js");
  const { ControlClient } = require("./control_grpc_web_pb.js");
  export let client;
</script>

<div class="card">
  <div class="card-content">
    <div class="content">
      <h1>{client.url}</h1>
      <br />
      <time datetime="2016-1-1">11:09 PM - 1 Jan 2016</time>
    </div>
  </div>
</div>
