import { DurableObject } from "cloudflare:workers";

// Tunnel Relay Durable Object — WebSocket tunnel between Client-Edge and
// this worker. Reconstructed from the LIVE worker bundle (2026-09-25) because
// the GitHub repo had drifted out of sync (this file was missing entirely).
//
// Behaviour (matches live worker exactly):
//   GET ?action=connect  -> WebSocket handshake (101), auth via AUTH_TOKEN
//   GET ?action=proxy    -> forward a request through the open tunnel
//   GET (no action)      -> status JSON
export class TunnelRelayDO extends DurableObject<Env> {
  tunnelWs: WebSocket | null = null;
  connectedAt = 0;
  pendingRequests = new Map<
    string,
    { resolve: (r: Response) => void; timer: ReturnType<typeof setTimeout> }
  >();

  constructor(state: DurableObjectState, env: Env) {
    super(state, env);
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const action = url.searchParams.get("action") || "status";

    if (action === "connect") {
      return this.handleConnect(request);
    }
    if (action === "proxy") {
      return this.handleProxy(request);
    }

    // default: status
    return new Response(
      JSON.stringify({
        tunnel: this.tunnelWs ? "active" : "offline",
        connectedAt: this.connectedAt,
        pending: this.pendingRequests.size,
      }),
      { headers: { "Content-Type": "application/json" } }
    );
  }

  handleConnect(request: Request): Response {
    const authToken =
      request.headers.get("Authorization")?.replace("Bearer ", "") || "";
    const expectedToken = this.env.AUTH_TOKEN || "";
    if (expectedToken && authToken !== expectedToken) {
      return new Response("unauthorized", { status: 401 });
    }

    const pair = new WebSocketPair();
    const [client, server] = [pair[0], pair[1]];
    server.accept();

    this.tunnelWs = server;
    this.connectedAt = Date.now();

    server.addEventListener("message", (event) => {
      try {
        const msg = JSON.parse(String(event.data));
        if (msg.type === "response" && msg.id) {
          const pending = this.pendingRequests.get(msg.id);
          if (pending) {
            clearTimeout(pending.timer);
            this.pendingRequests.delete(msg.id);
            const headers = new Headers(msg.headers || {});
            headers.set("X-Tunnel", "eon-sovereign");
            pending.resolve(
              new Response(msg.body || "", {
                status: msg.status || 200,
                headers,
              })
            );
          }
        }
      } catch (e) {
        console.error("tunnel message error:", e);
      }
    });

    server.addEventListener("close", () => {
      this.tunnelWs = null;
      for (const [id, pending] of this.pendingRequests) {
        clearTimeout(pending.timer);
        pending.resolve(
          new Response(
            JSON.stringify({ error: "tunnel_disconnected" }),
            {
              status: 503,
              headers: { "Content-Type": "application/json" },
            }
          )
        );
      }
      this.pendingRequests.clear();
    });

    server.send(JSON.stringify({ type: "connected", ts: Date.now() }));
    return new Response(null, { status: 101, webSocket: client });
  }

  async handleProxy(request: Request): Promise<Response> {
    if (!this.tunnelWs || this.tunnelWs.readyState !== 1) {
      return new Response(
        JSON.stringify({ error: "tunnel_offline" }),
        {
          status: 503,
          headers: { "Content-Type": "application/json" },
        }
      );
    }

    const url = new URL(request.url);
    const headers: Record<string, string> = {};
    request.headers.forEach((v, k) => {
      headers[k] = v;
    });

    const body =
      request.method !== "GET" && request.method !== "HEAD"
        ? await request.text()
        : "";

    const targetPath = url.searchParams.get("path") || "/";
    const targetUrl = new URL(request.url);
    targetUrl.pathname = targetPath;
    targetUrl.searchParams.delete("action");
    targetUrl.searchParams.delete("path");
    targetUrl.searchParams.delete("id");

    const id = crypto.randomUUID();
    const tunnelMsg = JSON.stringify({
      type: "request",
      id,
      method: request.method,
      path: targetPath + targetUrl.search,
      host: headers.host || "cloud.eon",
      headers,
      body,
      ts: Date.now(),
    });

    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.pendingRequests.delete(id);
        resolve(
          new Response(
            JSON.stringify({ error: "tunnel_timeout" }),
            {
              status: 504,
              headers: { "Content-Type": "application/json" },
            }
          )
        );
      }, 29_000);
      this.pendingRequests.set(id, { resolve, timer });
      this.tunnelWs!.send(tunnelMsg);
    });
  }
}