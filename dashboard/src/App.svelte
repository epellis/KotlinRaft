<script>
  import RaftClient from "./RaftClient.svelte";
  import Notifications from "svelte-notifications";
  const title = "Raft";
</script>

<Notifications>
  <section class="section">
    <div class="container">
      <h1 class="title">Raft Dashboard</h1>
      <h2 class="subtitle">Monitor Cluster Status</h2>
    </div>
    <div class="container">
      <div class="columns">
        <div class="column">
          <RaftClient client={{ url: 'http://localhost:7070' }} />
        </div>
        <div class="column">
          <RaftClient client={{ url: 'http://localhost:7071' }} />
        </div>
        <div class="column">
          <RaftClient client={{ url: 'http://localhost:7072' }} />
        </div>
        <div class="column">
          <RaftClient client={{ url: 'http://localhost:7073' }} />
        </div>
      </div>
    </div>
  </section>
</Notifications>
