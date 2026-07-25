import { createHash, createHmac } from "node:crypto"
import { hostname } from "node:os"

const baseURL = (process.env.MUDKAT_TRACKER_URL || "").replace(/\/$/, "")
const secret = process.env.MUDKAT_TRACKER_SECRET || ""

export const MudKatTokenTracker = async () => ({
  event: async ({ event }) => {
    const info = event.type === "message.updated" ? event.properties?.info : null
    if (!baseURL || !secret || info?.role !== "assistant" || !info.time?.completed || !info.tokens) return

    const tokens = info.tokens
    const body = JSON.stringify({ events: [{
      idempotency_key: "opencode:" + createHash("sha256")
        .update([hostname(), info.sessionID, info.id, info.time.completed, tokens.total].join("|"))
        .digest("hex"),
      client: "opencode",
      host: hostname(),
      provider: info.providerID || "unknown",
      model: info.modelID || "unknown",
      session_id: info.sessionID,
      event_time: new Date(info.time.completed).toISOString(),
      input_tokens: tokens.input || 0,
      cached_input_tokens: tokens.cache?.read || 0,
      cache_write_tokens: tokens.cache?.write || 0,
      output_tokens: tokens.output || 0,
      reasoning_output_tokens: tokens.reasoning || 0,
      total_tokens: tokens.total || 0,
      api_call_count: 1,
      billing_mode: "api",
      attribution: "exact",
      actual_cost_usd: info.cost ?? null,
    }] })
    const timestamp = Math.floor(Date.now() / 1000).toString()
    const signature = createHmac("sha256", secret).update(timestamp + "\n" + body).digest("hex")
    try {
      const response = await fetch(baseURL + "/api/v1/ingest/opencode", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-MudKat-Timestamp": timestamp,
          "X-MudKat-Signature": signature,
        },
        body,
      })
      if (!response.ok) console.warn(`MudKat tracker rejected usage: HTTP ${response.status}`)
    } catch (error) {
      console.warn(`MudKat tracker unavailable: ${error.message}`)
    }
  },
})
