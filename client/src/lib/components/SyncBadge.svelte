<script lang="ts">
  import { onMount } from "svelte";
  import { commands, type SyncStatus } from "$lib/bindings";
  import { listen } from "@tauri-apps/api/event";
  import { type as osType } from "@tauri-apps/plugin-os";

  let syncStatus = $state<SyncStatus>({ status: "connecting" });

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
    loadSyncStatus();

    const unlistenSync = listen<SyncStatus>("sync-status", (event) => {
      syncStatus = event.payload;
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
      unlistenSync.then((u) => u());
      window.removeEventListener("online", handleAutoReconnect);
      removeVisibilityListener?.();
    };
  });
</script>

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

<style>
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
</style>
