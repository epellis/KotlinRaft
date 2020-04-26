<script>
  import RaftClient from "./RaftClient.svelte";
  const title = "Raft";
</script>

<section class="section">
  <div class="container">
    <h1 class="title">Raft Dashboard</h1>
    <h2 class="subtitle">Monitor Cluster Status</h2>
  </div>
  <div class="container">
    <div class="columns">
      <div class="column">
        <RaftClient client={{ url: 'http://localhost:8080' }} />
      </div>
      <div class="column">
        <RaftClient client={{ url: 'http://localhost:8001' }} />
      </div>
      <div class="column">
        <RaftClient client={{ url: 'http://localhost:8002' }} />
      </div>
      <div class="column">
        <RaftClient client={{ url: 'http://localhost:8003' }} />
      </div>
    </div>
  </div>
</section>
