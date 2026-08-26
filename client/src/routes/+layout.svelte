<script lang="ts">
  import "../app.css";
  import { Toaster } from "svelte-sonner";
  import { QueryClient, QueryClientProvider } from "@tanstack/svelte-query";
  import type { Snippet } from "svelte";

  let { children }: { children: Snippet } = $props();

  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        staleTime: Infinity,
      },
    },
  });
</script>

<QueryClientProvider client={queryClient}>
  <Toaster richColors position="bottom-right" />
  {@render children()}
</QueryClientProvider>
