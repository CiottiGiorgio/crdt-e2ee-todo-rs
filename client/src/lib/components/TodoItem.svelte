<script lang="ts">
  import type { TodoItem, TodoStatus } from "$lib/bindings";

  let {
    item,
    onStatusChange,
    onDelete,
  }: {
    item: TodoItem;
    onStatusChange: (status: TodoStatus) => void;
    onDelete: () => void;
  } = $props();

  function handleToggleCheckbox() {
    if (item.status === "completed") {
      onStatusChange("todo");
    } else {
      onStatusChange("completed");
    }
  }
</script>

<li class="todo-item">
  <label>
    <input
      type="checkbox"
      checked={item.status === "completed"}
      onchange={handleToggleCheckbox}
    />
    <span class:completed={item.status === "completed"}>{item.text}</span>
  </label>

  <div class="item-actions">
    {#if item.status === "todo"}
      <button
        type="button"
        class="action-btn"
        onclick={() => onStatusChange("archived")}
      >
        Archive
      </button>
    {:else if item.status === "archived"}
      <button
        type="button"
        class="action-btn"
        onclick={() => onStatusChange("todo")}
      >
        Unarchive
      </button>
    {/if}

    <button
      type="button"
      class="delete-btn"
      onclick={onDelete}
    >
      Delete
    </button>
  </div>
</li>

<style>
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

  .completed {
    text-decoration: line-through;
    color: #888;
  }

  .item-actions {
    display: flex;
    gap: 6px;
    opacity: 0;
    visibility: hidden;
    transition: opacity 0.15s ease, visibility 0.15s ease;
  }

  .todo-item:hover .item-actions,
  .todo-item:focus-within .item-actions {
    opacity: 1;
    visibility: visible;
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

  .delete-btn:hover {
    background-color: #fee2e2;
    border-color: #fca5a5;
    color: #b91c1c;
  }
</style>
