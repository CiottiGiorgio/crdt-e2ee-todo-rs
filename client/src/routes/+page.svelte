<script lang="ts">
  import { onMount } from "svelte";
  import { listen } from "@tauri-apps/api/event";
  import { toast } from "svelte-sonner";
  import { useQueryClient } from "@tanstack/svelte-query";
  import {
    useTodosByStatus,
    useAddTodo,
    useUpdateTodoStatus,
    useDeleteTodo,
    todoKeys,
  } from "$lib/queries";
  import type { TodoStatus } from "$lib/bindings";
  import SyncBadge from "$lib/components/SyncBadge.svelte";
  import TodoSection from "$lib/components/TodoSection.svelte";

  const queryClient = useQueryClient();

  let isArchivedOpen = $state(false);
  let isCompletedOpen = $state(false);

  const todoTasksQuery = useTodosByStatus("todo");
  const archivedTasksQuery = useTodosByStatus("archived");
  const completedTasksQuery = useTodosByStatus("completed");

  const addTodoMutation = useAddTodo();
  const updateStatusMutation = useUpdateTodoStatus();
  const deleteTodoMutation = useDeleteTodo();

  onMount(() => {
    const unlistenTodos = listen("todos-updated", () => {
      queryClient.invalidateQueries({ queryKey: todoKeys.all });
    });
    const unlistenDbError = listen<string>("db-error", (event) => {
      toast.error("Database error", { description: event.payload });
    });

    return () => {
      unlistenTodos.then((u) => u());
      unlistenDbError.then((u) => u());
    };
  });

  let todoTasks = $derived(todoTasksQuery.data ?? []);
  let archivedTasks = $derived(archivedTasksQuery.data ?? []);
  let completedTasks = $derived(completedTasksQuery.data ?? []);

  let newTodoText = $state("");

  function addTodo(event: Event) {
    event.preventDefault();
    const text = newTodoText.trim();
    if (!text) return;
    addTodoMutation.mutate(text);
    newTodoText = "";
  }

  function updateStatus(id: string, newStatus: TodoStatus) {
    updateStatusMutation.mutate({ id, status: newStatus });
  }

  function deleteTodo(id: string) {
    deleteTodoMutation.mutate(id);
  }
</script>

<main class="container">
  <div class="header-row">
    <h1>Todo List</h1>
    <SyncBadge />
  </div>

  <form onsubmit={addTodo} class="todo-form">
    <input
      type="text"
      placeholder="Add a new task..."
      bind:value={newTodoText}
    />
    <button type="submit">Add</button>
  </form>

  <TodoSection
    items={todoTasks}
    emptyMessage="No tasks to do."
    onStatusChange={updateStatus}
    onDelete={deleteTodo}
  />

  <TodoSection
    title="Archived"
    items={archivedTasks}
    collapsible
    bind:isOpen={isArchivedOpen}
    emptyMessage="No archived tasks."
    onStatusChange={updateStatus}
    onDelete={deleteTodo}
  />

  <TodoSection
    title="Completed"
    items={completedTasks}
    collapsible
    bind:isOpen={isCompletedOpen}
    emptyMessage="No completed tasks."
    onStatusChange={updateStatus}
    onDelete={deleteTodo}
  />
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
</style>
