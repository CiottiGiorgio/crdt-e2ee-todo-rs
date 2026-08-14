<script lang="ts">
  import { onMount } from "svelte";
  import { commands, type SyncStatus, type TodoItem, type TodoStatus } from "$lib/bindings";
  import { listen } from "@tauri-apps/api/event";

  let syncStatus = $state<SyncStatus>({ status: "connecting" });
  let todos = $state<TodoItem[]>([]);

  async function loadTodos() {
    const res = await commands.getTodos();
    if (res.status === "ok") {
      todos = res.data;
    } else {
      console.error("Failed to load todos:", res.error);
    }
  }

  async function loadSyncStatus() {
    const res = await commands.getSyncStatus();
    if (res.status === "ok") {
      syncStatus = res.data;
    }
  }

  onMount(() => {
    loadTodos();
    loadSyncStatus();
    const unlistenTodos = listen("todos-updated", () => {
      loadTodos();
    });
    const unlistenSync = listen<SyncStatus>("sync-status", (event) => {
      syncStatus = event.payload;
    });
    return () => {
      unlistenTodos.then((u) => u());
      unlistenSync.then((u) => u());
    };
  });

  let isBacklogOpen = $state(false);
  let isCompletedOpen = $state(false);

  let workingSetTodos = $derived(todos.filter((t) => t.status === "workingSet"));
  let backlogTodos = $derived(todos.filter((t) => t.status === "backlog"));
  let completedTodos = $derived(todos.filter((t) => t.status === "completed"));

  let newTodoText = $state("");

  async function addTodo(event: Event) {
    event.preventDefault();
    const text = newTodoText.trim();
    if (!text) return;
    const res = await commands.addTodo(text);
    if (res.status === "ok") {
      todos.push(res.data);
      newTodoText = "";
    } else {
      console.error("Failed to add todo:", res.error);
    }
  }

  async function updateStatus(id: string, newStatus: TodoStatus) {
    const todo = todos.find((t) => t.id === id);
    if (!todo) return;
    const oldStatus = todo.status;
    todo.status = newStatus;
    const res = await commands.updateTodoStatus(id, newStatus);
    if (res.status !== "ok") {
      todo.status = oldStatus;
      console.error("Failed to update todo status:", res.error);
    }
  }

  async function deleteTodo(id: string) {
    const original = [...todos];
    todos = todos.filter((t) => t.id !== id);
    const res = await commands.deleteTodo(id);
    if (res.status !== "ok") {
      todos = original;
      console.error("Failed to delete todo:", res.error);
    }
  }
</script>

<main class="container">
  <div class="header-row">
    <h1>Todo List</h1>
    <div class="sync-badge {syncStatus.status}">
      <span class="sync-dot"></span>
      {#if syncStatus.status === "connected"}
        <span>Synced</span>
      {:else if syncStatus.status === "connecting"}
        <span>Connecting...</span>
      {:else if syncStatus.status === "disconnected"}
        <span>Local</span>
      {:else if syncStatus.status === "error"}
        <span>Sync Error: {syncStatus.message}</span>
      {/if}
    </div>
  </div>

  <form onsubmit={addTodo} class="todo-form">
    <input
      type="text"
      placeholder="Add a new task..."
      bind:value={newTodoText}
    />
    <button type="submit">Add</button>
  </form>

  <!-- 1. Working Set (First List) -->
  <section class="todo-section">
    <h2>Working Set</h2>
    {#if workingSetTodos.length === 0}
      <p class="empty-msg">No tasks in working set.</p>
    {:else}
      <ul class="todo-list">
        {#each workingSetTodos as todo (todo.id)}
          <li class="todo-item">
            <label>
              <input
                type="checkbox"
                checked={todo.status === "completed"}
                onchange={() => updateStatus(todo.id, todo.status === "completed" ? "workingSet" : "completed")}
              />
              <span>{todo.text}</span>
            </label>
            <div class="item-actions">
              <button
                type="button"
                class="action-btn"
                onclick={() => updateStatus(todo.id, "backlog")}
              >
                - Remove
              </button>
              <button
                type="button"
                class="delete-btn"
                onclick={() => deleteTodo(todo.id)}
              >
                Delete
              </button>
            </div>
          </li>
        {/each}
      </ul>
    {/if}
  </section>

  <!-- 2. Inactive Tasks (Collapsible, default collapsed) -->
  <section class="todo-section">
    <button
      type="button"
      class="collapse-header"
      onclick={() => (isBacklogOpen = !isBacklogOpen)}
    >
      <span>Tasks ({backlogTodos.length})</span>
      <svg
        class="arrow-icon"
        class:open={isBacklogOpen}
        width="16"
        height="16"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        stroke-width="2.5"
        stroke-linecap="round"
        stroke-linejoin="round"
      >
        <polyline points="6 9 12 15 18 9"></polyline>
      </svg>
    </button>
    {#if isBacklogOpen}
      {#if backlogTodos.length === 0}
        <p class="empty-msg">No active tasks.</p>
      {:else}
        <ul class="todo-list">
          {#each backlogTodos as todo (todo.id)}
            <li class="todo-item">
              <label>
                <input
                  type="checkbox"
                  checked={todo.status === "completed"}
                  onchange={() => updateStatus(todo.id, todo.status === "completed" ? "backlog" : "completed")}
                />
                <span>{todo.text}</span>
              </label>
              <div class="item-actions">
                <button
                  type="button"
                  class="action-btn"
                  onclick={() => updateStatus(todo.id, "workingSet")}
                >
                  + Working Set
                </button>
                <button
                  type="button"
                  class="delete-btn"
                  onclick={() => deleteTodo(todo.id)}
                >
                  Delete
                </button>
              </div>
            </li>
          {/each}
        </ul>
      {/if}
    {/if}
  </section>

  <!-- 3. Completed Tasks (Collapsible, default collapsed) -->
  <section class="todo-section">
    <button
      type="button"
      class="collapse-header"
      onclick={() => (isCompletedOpen = !isCompletedOpen)}
    >
      <span>Completed Tasks ({completedTodos.length})</span>
      <svg
        class="arrow-icon"
        class:open={isCompletedOpen}
        width="16"
        height="16"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        stroke-width="2.5"
        stroke-linecap="round"
        stroke-linejoin="round"
      >
        <polyline points="6 9 12 15 18 9"></polyline>
      </svg>
    </button>
    {#if isCompletedOpen}
      {#if completedTodos.length === 0}
        <p class="empty-msg">No completed tasks.</p>
      {:else}
        <ul class="todo-list">
          {#each completedTodos as todo (todo.id)}
            <li class="todo-item">
              <label>
                <input
                  type="checkbox"
                  checked={todo.status === "completed"}
                  onchange={() => updateStatus(todo.id, "workingSet")}
                />
                <span class="completed">{todo.text}</span>
              </label>
              <div class="item-actions">
                <button
                  type="button"
                  class="delete-btn"
                  onclick={() => deleteTodo(todo.id)}
                >
                  Delete
                </button>
              </div>
            </li>
          {/each}
        </ul>
      {/if}
    {/if}
  </section>
</main>

<style>
  .container {
    max-width: 500px;
    margin: 40px auto;
    padding: 0 20px;
    font-family: sans-serif;
  }

  .header-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 20px;
  }

  .header-row h1 {
    margin: 0;
  }

  .sync-badge {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    font-size: 12px;
    padding: 4px 10px;
    border-radius: 12px;
    font-weight: 500;
  }

  .sync-dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
  }

  .sync-badge.connected {
    background-color: #e6f4ea;
    color: #137333;
  }
  .sync-badge.connected .sync-dot {
    background-color: #137333;
  }

  .sync-badge.connecting {
    background-color: #fef7e0;
    color: #b06000;
  }
  .sync-badge.connecting .sync-dot {
    background-color: #b06000;
  }

  .sync-badge.disconnected {
    background-color: #f1f3f4;
    color: #5f6368;
  }
  .sync-badge.disconnected .sync-dot {
    background-color: #5f6368;
  }

  .sync-badge.error {
    background-color: #fce8e6;
    color: #c5221f;
  }
  .sync-badge.error .sync-dot {
    background-color: #c5221f;
  }

  h2 {
    font-size: 1.2rem;
    margin-bottom: 10px;
  }

  .collapse-header {
    background: none;
    border: none;
    padding: 0;
    font-size: 1.2rem;
    font-weight: bold;
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: space-between;
    width: 100%;
    margin-bottom: 10px;
    color: inherit;
    font-family: inherit;
  }

  .arrow-icon {
    transition: transform 0.2s ease;
    transform: rotate(-90deg);
  }

  .arrow-icon.open {
    transform: rotate(0deg);
  }

  .todo-form {
    display: flex;
    gap: 10px;
    margin-bottom: 30px;
  }

  .todo-form input[type="text"] {
    flex: 1;
    padding: 8px 12px;
    font-size: 16px;
    border: 1px solid #ccc;
    border-radius: 4px;
  }

  .todo-form button {
    padding: 8px 16px;
    font-size: 16px;
    cursor: pointer;
  }

  .todo-section {
    margin-bottom: 30px;
  }

  .empty-msg {
    color: #777;
    font-style: italic;
    font-size: 14px;
    margin: 0;
  }

  .todo-list {
    list-style: none;
    padding: 0;
    margin: 0;
  }

  .todo-item {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 8px 0;
    border-bottom: 1px solid #eee;
  }

  .todo-item label {
    display: flex;
    align-items: center;
    gap: 10px;
    cursor: pointer;
  }

  .item-actions {
    display: flex;
    gap: 6px;
  }

  .completed {
    text-decoration: line-through;
    color: #888;
  }

  .action-btn,
  .delete-btn {
    padding: 4px 8px;
    font-size: 13px;
    cursor: pointer;
    background: transparent;
    border: 1px solid #ddd;
    border-radius: 4px;
  }

  .action-btn:hover {
    background-color: #f0f0f0;
  }
</style>




