import { createQuery, createMutation, useQueryClient } from "@tanstack/svelte-query";
import { commands, type TodoStatus } from "$lib/bindings";
import { toast } from "svelte-sonner";

export const todoKeys = {
  all: ["todos"] as const,
  byStatus: (status: TodoStatus) => [...todoKeys.all, status] as const,
};

export function useTodosByStatus(
  status: TodoStatus,
  enabled: () => boolean = () => true
) {
  return createQuery(() => ({
    queryKey: todoKeys.byStatus(status),
    queryFn: async () => {
      const res = await commands.getTodosByStatus(status);
      if (res.status === "error") throw new Error(res.error);
      return res.data;
    },
    enabled: enabled(),
  }));
}

export function useAddTodo() {
  const queryClient = useQueryClient();
  return createMutation(() => ({
    mutationFn: async (text: string) => {
      const res = await commands.addTodo(text);
      if (res.status === "error") throw new Error(res.error);
      return res.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: todoKeys.all });
    },
    onError: (err: Error) => {
      toast.error("Failed to add todo", { description: err.message });
    },
  }));
}

export function useUpdateTodoStatus() {
  const queryClient = useQueryClient();
  return createMutation(() => ({
    mutationFn: async ({ id, status }: { id: string; status: TodoStatus }) => {
      const res = await commands.updateTodoStatus(id, status);
      if (res.status === "error") throw new Error(res.error);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: todoKeys.all });
    },
    onError: (err: Error) => {
      toast.error("Failed to update todo status", { description: err.message });
    },
  }));
}

export function useDeleteTodo() {
  const queryClient = useQueryClient();
  return createMutation(() => ({
    mutationFn: async (id: string) => {
      const res = await commands.deleteTodo(id);
      if (res.status === "error") throw new Error(res.error);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: todoKeys.all });
    },
    onError: (err: Error) => {
      toast.error("Failed to delete todo", { description: err.message });
    },
  }));
}
