<script lang="ts">
  import { Spinner } from "@gradio/icons";
  import AudioWave from "./AudioWave.svelte";
  import { createEventDispatcher } from 'svelte';

  const dispatch = createEventDispatcher();

  export let stream_state;
  export let onStartChat
  export let audio_source_callback
  export let wave_color
  export let assetLoaded = true
  export let loadingProgress = 0
  
  let isConnecting = false;
  let retryCount = 0;
  const maxRetries = 3;
  
  // 改进的连接处理逻辑
  async function handleStartChat() {
    if (isConnecting) {
      console.log("Connection already in progress, ignoring click");
      return;
    }
    
    isConnecting = true;
    retryCount = 0;
    
    try {
      await connectWithRetry();
    } finally {
      isConnecting = false;
    }
  }
  
  async function connectWithRetry() {
    while (retryCount < maxRetries) {
      try {
        console.log(`Connection attempt ${retryCount + 1}/${maxRetries}`);
        
        // 强制清理现有连接
        if (stream_state === "open" || stream_state === "waiting") {
          console.log("Cleaning up existing connection...");
          await cleanupConnections();
        }
        
        // 等待清理完成
        await new Promise(resolve => setTimeout(resolve, 300));
        
        // 尝试建立新连接
        onStartChat();
        
        // 等待连接建立 (最多等待10秒)
        const connectionEstablished = await waitForConnection(10000);
        
        if (connectionEstablished) {
          console.log("Connection established successfully");
          retryCount = 0;
          return;
        } else {
          throw new Error("Connection timeout");
        }
        
      } catch (error) {
        retryCount++;
        console.warn(`Connection attempt ${retryCount} failed:`, error);
        
        if (retryCount < maxRetries) {
          console.log(`Retrying in ${retryCount * 1000}ms...`);
          await new Promise(resolve => setTimeout(resolve, retryCount * 1000));
        } else {
          console.error("Max retries reached, connection failed");
          // 可以在这里显示错误提示
        }
      }
    }
  }
  
  async function cleanupConnections() {
    try {
      const response = await fetch('/cleanup_connections', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({action: 'force_cleanup'})
      });
      
      if (response.ok) {
        console.log("Server connections cleaned up successfully");
      }
    } catch (error) {
      console.warn("Failed to cleanup server connections:", error);
    }
  }
  
  async function waitForConnection(timeout) {
    return new Promise((resolve) => {
      const startTime = Date.now();
      
      const checkConnection = () => {
        if (stream_state === "open" && assetLoaded) {
          resolve(true);
        } else if (Date.now() - startTime > timeout) {
          resolve(false);
        } else {
          setTimeout(checkConnection, 100);
        }
      };
      
      checkConnection();
    });
  }
</script>

<div class="player-controls">
  <!-- svelte-ignore a11y-click-events-have-key-events -->
  <!-- svelte-ignore a11y-no-static-element-interactions -->
  <div
    class="chat-btn"
    class:start-chat={stream_state === "closed" && !isConnecting}
    class:stop-chat={stream_state === "open" && assetLoaded === true}
    class:connecting={isConnecting}
    on:click={handleStartChat}
  >
    {#if stream_state === "closed" && !isConnecting}
      <span>点击开始对话</span>
    {:else if stream_state === "waiting" || assetLoaded === false || isConnecting}
      <div class="waiting-icon-text">
        <div class="icon" title="spinner">
          <Spinner />
        </div>
        {#if isConnecting && retryCount > 0}
          <span>重试中 ({retryCount}/{maxRetries})</span>
        {:else if loadingProgress > 0 && loadingProgress < 100}
          <span>加载中 {Math.round(loadingProgress)}%</span>
        {:else}
          <span>连接中</span>
        {/if}
      </div>
    {:else}
      <div class="stop-chat-inner"></div>
    {/if}
  </div>
  {#if stream_state === "open" && assetLoaded === true}
  <div class="input-audio-wave">
    <AudioWave {audio_source_callback} {stream_state} {wave_color} />
  </div>
  {/if}
</div>

<style lang="less">
  .player-controls {
    height: 15%;
    position: relative;
    display: flex;
    justify-content: center;
    align-items: center;
    min-height: 84px;

    .chat-btn {
      height: 64px;
      width: 296px;
      display: flex;
      justify-content: center;
      align-items: center;
      border-radius: 999px;
      opacity: 1;
      background: linear-gradient(180deg, #7873f6 0%, #524de1 100%);
      transition: all 0.3s;
      z-index: 2;
      cursor: pointer;
      
      &.connecting {
        opacity: 0.8;
        cursor: not-allowed;
      }
    }
    .start-chat {
      font-size: 16px;
      font-weight: 500;
      text-align: center;
      color: #ffffff;
    }
    .waiting-icon-text {
      width: 120px;
      align-items: center;
      font-size: 14px;
      font-weight: 500;
      color: #ffffff;
      margin: 0 var(--spacing-sm);
      display: flex;
      justify-content: space-evenly;
      gap: var(--size-1);
      .icon {
        width: 25px;
        height: 25px;
        fill: #ffffff;
        stroke: #ffffff;
        color: #ffffff;
      }
    }

    .stop-chat {
      width: 64px;
      .stop-chat-inner {
        width: 25px;
        height: 25px;
        border-radius: 6.25px;
        background: #fafafa;
      }
    }

    .input-audio-wave {
      position: absolute;
    }
  }
</style>
