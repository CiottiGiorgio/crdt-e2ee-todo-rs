<script lang="ts">
  import { onMount } from "svelte";
  import { commands, type SyncStatus, type TodoItem, type TodoStatus } from "$lib/bindings";
  import { listen } from "@tauri-apps/api/event";
  import { type as osType } from "@tauri-apps/plugin-os";

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

  async function handleReconnect() {
    const res = await commands.manualReconnect();
    if (res.status !== "ok") {
      console.error("Failed to trigger reconnect:", res.error);
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
    const unlistenDbError = listen<string>("db-error", (event) => {
      console.error("Database error:", event.payload);
      // TODO: render as a toast notification
    });

    const handleAutoReconnect = () => {
      if (syncStatus.status === "disconnected") {
        handleReconnect();
      }
    };
    window.addEventListener("online", handleAutoReconnect);

    let removeVisibilityListener: (() => void) | undefined;
    const currentOs = osType();
    const isMobile = currentOs === "android" || currentOs === "ios";
    // On desktop, window switching is frequent so we avoid auto-reconnecting on focus.
    // On mobile, visibilitychange reliably detects foreground resume. Since visibilitychange
    // fires in both directions (hidden & visible), we check visibilityState to only reconnect on focus-in.
    if (isMobile) {
      const handleVisibility = () => {
        if (document.visibilityState === "visible") {
          handleAutoReconnect();
        }
      };
      document.addEventListener("visibilitychange", handleVisibility);
      removeVisibilityListener = () => {
        document.removeEventListener("visibilitychange", handleVisibility);
      };
    }

    return () => {
      unlistenTodos.then((u) => u());
      unlistenSync.then((u) => u());
      unlistenDbError.then((u) => u());
      window.removeEventListener("online", handleAutoReconnect);
      removeVisibilityListener?.();
    };
  });

  let isArchivedOpen = $state(false);
  let isCompletedOpen = $state(false);

  let todoTasks = $derived(todos.filter((t) => t.status === "todo"));
  let archivedTasks = $derived(todos.filter((t) => t.status === "archived"));
  let completedTasks = $derived(todos.filter((t) => t.status === "completed"));

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
    {#if syncStatus.status === "disconnected"}
      <button
        type="button"
        class="sync-badge disconnected retry-btn"
        onclick={handleReconnect}
        title="Click to retry connection"
      >
        <span class="sync-dot"></span>
        <span>Local · <strong>Retry ↻</strong></span>
      </button>
    {:else}
      <div class="sync-badge {syncStatus.status}">
        <span class="sync-dot"></span>
        {#if syncStatus.status === "connected"}
          <span>Synced</span>
        {:else if syncStatus.status === "connecting"}
          <span>Connecting...</span>
        {/if}
      </div>
    {/if}
  </div>

  <form onsubmit={addTodo} class="todo-form">
    <input
      type="text"
      placeholder="Add a new task..."
      bind:value={newTodoText}
    />
    <button type="submit">Add</button>
  </form>

  <!-- 1. Todo Tasks (First List) -->
  <section class="todo-section">
    <h2>Todo</h2>
    {#if todoTasks.length === 0}
      <p class="empty-msg">No tasks to do.</p>
    {:else}
      <ul class="todo-list">
        {#each todoTasks as todo (todo.id)}
          <li class="todo-item">
            <label>
              <input
                type="checkbox"
                checked={todo.status === "completed"}
                onchange={() => updateStatus(todo.id, todo.status === "completed" ? "todo" : "completed")}
              />
              <span>{todo.text}</span>
            </label>
            <div class="item-actions">
              <button
                type="button"
                class="action-btn"
                onclick={() => updateStatus(todo.id, "archived")}
              >
                Archive
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

  <!-- 2. Archived Tasks (Collapsible, default collapsed) -->
  <section class="todo-section">
    <button
      type="button"
      class="collapse-header"
      onclick={() => (isArchivedOpen = !isArchivedOpen)}
    >
      <span>Archived ({archivedTasks.length})</span>
      <svg
        class="arrow-icon"
        class:open={isArchivedOpen}
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
    {#if isArchivedOpen}
      {#if archivedTasks.length === 0}
        <p class="empty-msg">No archived tasks.</p>
      {:else}
        <ul class="todo-list">
          {#each archivedTasks as todo (todo.id)}
            <li class="todo-item">
              <label>
                <input
                  type="checkbox"
                  checked={todo.status === "completed"}
                  onchange={() => updateStatus(todo.id, todo.status === "completed" ? "archived" : "completed")}
                />
                <span>{todo.text}</span>
              </label>
              <div class="item-actions">
                <button
                  type="button"
                  class="action-btn"
                  onclick={() => updateStatus(todo.id, "todo")}
                >
                  Unarchive
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
      <span>Completed ({completedTasks.length})</span>
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
      {#if completedTasks.length === 0}
        <p class="empty-msg">No completed tasks.</p>
      {:else}
        <ul class="todo-list">
          {#each completedTasks as todo (todo.id)}
            <li class="todo-item">
              <label>
                <input
                  type="checkbox"
                  checked={todo.status === "completed"}
                  onchange={() => updateStatus(todo.id, "todo")}
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

  .sync-badge.retry-btn {
    cursor: pointer;
    border: 1px solid #dadce0;
    font-family: inherit;
    transition: background-color 0.15s ease, border-color 0.15s ease, transform 0.1s ease;
  }

  .sync-badge.retry-btn:hover {
    background-color: #e8eaed;
    border-color: #bdc1c6;
    transform: translateY(-1px);
  }

  .sync-badge.retry-btn:active {
    transform: translateY(0);
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




