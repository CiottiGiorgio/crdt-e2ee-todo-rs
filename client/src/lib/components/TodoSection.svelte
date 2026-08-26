<script lang="ts">
  import type { TodoItem as TodoItemType, TodoStatus } from "$lib/bindings";
  import TodoItem from "./TodoItem.svelte";

  let {
    title,
    items,
    emptyMessage = "No tasks.",
    collapsible = false,
    defaultOpen = true,
    onStatusChange,
    onDelete,
  }: {
    title?: string;
    items: TodoItemType[];
    emptyMessage?: string;
    collapsible?: boolean;
    defaultOpen?: boolean;
    onStatusChange: (id: string, status: TodoStatus) => void;
    onDelete: (id: string) => void;
  } = $props();

  // svelte-ignore state_referenced_locally
  let isOpen = $state(defaultOpen);
</script>

<section class="todo-section">
  {#if title}
    {#if collapsible}
      <button
        type="button"
        class="collapse-header"
        onclick={() => (isOpen = !isOpen)}
      >
        <span>{title} ({items.length})</span>
        <svg
          class="arrow-icon"
          class:open={isOpen}
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
    {:else}
      <h2>{title}</h2>
    {/if}
  {/if}

  {#if !collapsible || !title || isOpen}
    {#if items.length === 0}
      <p class="empty-msg">{emptyMessage}</p>
    {:else}
      <ul class="todo-list">
        {#each items as todo (todo.id)}
          <TodoItem
            item={todo}
            onStatusChange={(status) => onStatusChange(todo.id, status)}
            onDelete={() => onDelete(todo.id)}
          />
        {/each}
      </ul>
    {/if}
  {/if}
</section>

<style>
  .todo-section {
    margin-bottom: 30px;
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
</style>
