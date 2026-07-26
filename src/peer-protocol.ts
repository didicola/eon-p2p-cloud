import { type Env, type PeerCapability, type Task } from "./types";
import { getSwarmDO, getModelFamily } from "./models";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PEER_PROTOCOL_VERSION = "2.0";

// ---------------------------------------------------------------------------
// WebSocket upgrade handler
// ---------------------------------------------------------------------------

/**
 * Handle a WebSocket upgrade request from an external GPU peer.
 *
 * Expects `peerId` as a query parameter (auto-generates one if absent).
 * On connection sends a `{ type: "connected", peerId, version: "2.0" }`
 * handshake.  Incoming messages are routed by `type`:
 *
 *   - `heartbeat`   — updates load/latency in the swarm capability registry
 *   - `task_result` — submits a completed inference result back to the swarm
 *   - `capabilities` — registers or updates the peer's capability record
 *
 * @param request  The incoming HTTP upgrade request
 * @param env      Workers environment bindings
 * @param ctx      Execution context for waitUntil
 * @returns A 101 Switching Protocols Response, or 400/500 on failure.
 */
export async function handleWebSocketUpgrade(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
): Promise<Response> {
  const upgrade = request.headers.get("Upgrade");
  if (upgrade?.toLowerCase() !== "websocket") {
    return new Response("Expected WebSocket upgrade", { status: 426 });
  }

  try {
    const url = new URL(request.url);
    const peerId =
      url.searchParams.get("peerId") ||
      url.searchParams.get("peer_id") ||
      `ext:${crypto.randomUUID()}`;
    const model = url.searchParams.get("model") || "general";
    const region = url.searchParams.get("region") || "unknown";
    const colo = request.cf?.colo || "";

    const pair = new WebSocketPair();
    const [client, server] = [pair[0], pair[1]];

    server.accept();

    // -- Handshake: tell the peer it is connected ---------------------------
    const handshake = {
      type: "connected",
      peerId,
      version: PEER_PROTOCOL_VERSION,
      region,
    };
    server.send(JSON.stringify(handshake));

    // -- Register the connection with the correct swarm shard ---------------
    const doStub = getSwarmDO(env, model);
    await doStub.fetch("http://internal/register-connection", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ peerId, model, region, colo }),
    });

    // -- Inbound message router ---------------------------------------------
    server.addEventListener("message", async (event: MessageEvent) => {
      try {
        const data = JSON.parse(event.data as string);

        switch (data.type) {
          case "heartbeat":
            await handleHeartbeat(env, peerId, data, model);
            break;

          case "task_result":
            await handleTaskResult(env, peerId, data, model);
            break;

          case "capabilities":
            await handleCapabilities(env, peerId, data, model);
            break;

          default:
            console.warn(
              `peer-protocol: unknown message type "${data.type}" from ${peerId}`,
            );
        }
      } catch (err) {
        console.warn(
          `peer-protocol: malformed message from ${peerId}:`,
          err,
        );
      }
    });

    // -- Cleanup on disconnect ----------------------------------------------
    server.addEventListener("close", () => {
      ctx.waitUntil(
        (async () => {
          try {
            const stub = getSwarmDO(env, model);
            await stub.fetch("http://internal/unregister-connection", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ peerId }),
            });
          } catch {
            // best-effort cleanup
          }
        })(),
      );
    });

    return new Response(null, { status: 101, webSocket: client });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error("peer-protocol: WebSocket upgrade failed:", msg);
    return new Response(
      JSON.stringify({ error: "upgrade_failed", message: msg }),
      {
        status: 500,
        headers: { "Content-Type": "application/json" },
      },
    );
  }
}

// ---------------------------------------------------------------------------
// Message handlers
// ---------------------------------------------------------------------------

/**
 * Process an incoming heartbeat from a peer.
 *
 * Updates the peer's load, latency, and heartbeat timestamp in the swarm
 * DO's capability registry so the router can make informed scheduling
 * decisions.
 */
async function handleHeartbeat(
  env: Env,
  peerId: string,
  data: Record<string, unknown>,
  model: string,
): Promise<void> {
  const stub = getSwarmDO(env, model);
  await stub.fetch("http://internal/heartbeat", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      peerId,
      load: data.load ?? 0,
      latency: data.latency ?? 0,
      currentLoad: data.currentLoad ?? data.load ?? 0,
      avgLatency: data.avgLatency ?? data.latency ?? 0,
      freeMemory: data.freeMemory,
      gpuUtilization: data.gpuUtilization,
    }),
  });
}

/**
 * Process a completed task result submitted by a peer over WebSocket.
 *
 * Forwards the result to the swarm DO's submitResult endpoint so the task
 * is marked done and any waiting caller can retrieve it.
 */
async function handleTaskResult(
  env: Env,
  peerId: string,
  data: Record<string, unknown>,
  model: string,
): Promise<void> {
  const taskId = data.taskId as string | undefined;
  const result = data.result as string | undefined;

  if (!taskId || typeof result !== "string") {
    console.warn(
      `peer-protocol: invalid task_result from ${peerId} (missing taskId or result)`,
    );
    return;
  }

  const stub = getSwarmDO(env, model);
  const resp = await stub.fetch(`http://internal/task/${taskId}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ result }),
  });

  if (!resp.ok) {
    console.warn(
      `peer-protocol: submitResult failed for task ${taskId} from ${peerId}: ${resp.status}`,
    );
  }
}

/**
 * Process a capabilities announcement from a peer.
 *
 * Registers or updates the peer's full capability record (models, max tokens,
 * load limits, region, etc.) so the swarm router can select this peer for
 * future tasks.
 */
async function handleCapabilities(
  env: Env,
  peerId: string,
  data: Record<string, unknown>,
  model: string,
): Promise<void> {
  const cap: PeerCapability = {
    peerId,
    models: (data.models as string[]) ?? [],
    maxTokens: (data.maxTokens as number) ?? 4096,
    avgLatency: (data.avgLatency as number) ?? 0,
    reliability: (data.reliability as number) ?? 1.0,
    currentLoad: (data.currentLoad as number) ?? 0,
    maxLoad: (data.maxLoad as number) ?? 5,
    region: (data.region as string) ?? "unknown",
    colo: data.colo as string | undefined,
    lastHeartbeat: Date.now(),
    version: (data.version as string) ?? PEER_PROTOCOL_VERSION,
    protocol: "websocket",
    endpoint: data.endpoint as string | undefined,
  };

  const stub = getSwarmDO(env, model);
  await stub.fetch("http://internal/register-capability", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(cap),
  });
}

// ---------------------------------------------------------------------------
// Broadcast helper
// ---------------------------------------------------------------------------

/**
 * Broadcast a JSON message to all WebSocket-connected peers in a given model
 * shard.
 *
 * Each shard DO maintains its own set of WebSocket connections.  The DO's
 * existing internal `broadcastToPeers` handles individual connection errors
 * and evicts dead sockets.
 *
 * @param env    Workers environment bindings
 * @param model  Model id (used to determine the target shard)
 * @param msg    A serialisable object to send to every peer
 */
export async function broadcastToPeers(
  env: Env,
  model: string,
  msg: Record<string, unknown>,
): Promise<void> {
  const stub = getSwarmDO(env, model);
  await stub.fetch("http://internal/broadcast", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ payload: msg }),
  });
}
