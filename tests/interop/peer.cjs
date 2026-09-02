// TypeScript-side peer for the interop test: hosts a `ts-user` service with the
// original @riaskov/nevo-messaging router runtime and calls the Rust `rs-user`
// service with the original NevoNatsClient. Every observation is printed as one
// JSON line; the Rust test asserts on them.
"use strict"

require("reflect-metadata")
const nevo = require("@riaskov/nevo-messaging")
const { connect } = require("@nats-io/transport-node")
const { headers: createHeaders } = require("@nats-io/nats-core")

const servers = (process.env.NEVO_TEST_NATS_URL || "nats://127.0.0.1:4222").split(",")
const rustService = process.env.NEVO_INTEROP_RUST_SERVICE || "rs-user"
const tsService = process.env.NEVO_INTEROP_TS_SERVICE || "ts-user"

function emit(obj) {
  const line = JSON.stringify(obj, (_k, v) => (typeof v === "bigint" ? `${v}n` : v))
  process.stdout.write(line + "\n")
}

class UserService {
  async getById(id) {
    return {
      id,
      idType: typeof id,
      name: "TS Eddie",
      uid: "018f0d7a-6b6c-7c3e-9f2e-2f5a1a2b3c4d",
      createdAt: new Date("2026-01-02T03:04:05.000Z"),
      tags: ["@@nevo:tricky", "plain"],
      big: (1n << 70n) + 1n,
      missing: undefined,
      list: [undefined, 1n]
    }
  }

  async getByIdV2(id) {
    return { id, version: 2 }
  }

  async echo(params) {
    return params
  }

  async fail() {
    throw new nevo.MessagingError(nevo.ErrorCode.VALIDATION_FAILED, { message: "ts says no", field: "id" })
  }

  async large(params) {
    return "y".repeat(Number(params.size ?? 0))
  }
}

class UserController {
  constructor() {
    this.users = new UserService()
  }
}

nevo.addSignalMetadata(UserController, "user.getById", "getById", (d) => [d.id])
nevo.addSignalMetadata(UserController, "user.getById", "getByIdV2", (d) => [d.id], undefined, { version: "v2" })
nevo.addSignalMetadata(UserController, "user.echo", "echo", (d) => [d])
nevo.addSignalMetadata(UserController, "user.fail", "fail")
nevo.addSignalMetadata(UserController, "user.large", "large", (d) => [d])

async function serveTypescriptRouter(nc) {
  const options = { serviceName: tsService, tracing: { enabled: false }, devtools: false }
  const decorate = nevo.createSignalRouterDecorator(
    UserService,
    options,
    (data) => ({ method: data.method, params: data.params, uuid: data.uuid, meta: data.meta }),
    () => {}
  )
  decorate(UserController)
  nevo.bindSignalRouterForTesting(UserController, options)
  const controller = new UserController()
  const codec = nevo.getDefaultCodec()
  const compression = nevo.resolveCompressionOptions({ enabled: true, algorithm: "gzip", threshold: 512 })
  const sub = nc.subscribe(`${tsService}-events`)
  ;(async () => {
    for await (const msg of sub) {
      try {
        const encoding = msg.headers?.get?.("content-encoding")
        const raw = nevo.maybeDecompress(msg.data, encoding)
        const payload = codec.decode(raw)
        const result = await controller.handleSignalMessage(payload)
        if (msg.reply) {
          const out = nevo.maybeCompress(codec.encode(result), compression)
          const h = createHeaders()
          if (out.encoding !== "identity") h.set("content-encoding", out.encoding)
          nc.publish(msg.reply, out.data, { headers: h })
        }
      } catch (err) {
        emit({ kind: "router-error", message: err?.message })
      }
    }
  })()
  return sub
}

async function callRust(client) {
  const user = await client.query(rustService, "user.getById", { id: 9007199254740993n })
  emit({
    kind: "query",
    id: user.id,
    idType: typeof user.id,
    name: user.name,
    big: user.big,
    bigType: typeof user.big,
    tricky: user.tags[0],
    date: user.createdAt,
    uid: user.uid,
    uidType: typeof user.uid
  })

  const v2 = await client.query(rustService, "user.getById", { id: 1n }, { version: "v2" })
  emit({ kind: "query-v2", version: v2.version })

  try {
    await client.query(rustService, "user.fail", {})
    emit({ kind: "error", missing: true })
  } catch (err) {
    emit({ kind: "error", code: err.code, message: err.message, service: err.serviceName, field: err.details?.field })
  }

  const echoed = await client.query(
    rustService,
    "user.echo",
    { s: "@@nevo:bigint:5", plain: "x", n: 2n },
    { tenantId: "t1", idempotencyKey: "k-1", headers: { "x-h": "v" } }
  )
  emit({ kind: "echo", echoed })

  await client.emit(rustService, "audit.write", { n: 1 })
  await client.publish(rustService, "order.placed", { n: 2 })
  await client.broadcast("system.status", { ok: true })

  const contract = await client.query(rustService, "nevo.contract", {})
  emit({ kind: "contract", protocol: contract.protocol, methods: contract.methods.map((m) => `${m.signalName}@${m.version}`) })

  const big = await client.query(rustService, "user.large", { size: 40000 })
  emit({ kind: "large", length: big.length })
}

async function main() {
  const nc = await connect({ servers })
  const routerSub = await serveTypescriptRouter(nc)

  const client = await nevo.NevoNatsClient.create([rustService], {
    servers,
    serviceName: "ts-caller",
    timeoutMs: 5000,
    compression: { enabled: true, algorithm: "gzip", threshold: 1 },
    retry: { enabled: false }
  })
  const jsonClient = await nevo.NevoNatsClient.create([rustService], {
    servers,
    serviceName: "ts-json",
    codec: new nevo.JsonCodec(),
    retry: { enabled: false }
  })

  const subscription = await client.subscribe(rustService, "order.placed", {}, async (data, ctx) => {
    emit({ kind: "sub", data, tenant: ctx.meta?.tenantId })
  })
  const broadcastSub = await client.subscribe("__broadcast", "system.status", {}, async (data) => {
    emit({ kind: "broadcast", data })
  })
  emit({ kind: "ready" })

  await callRust(client)
  const fromJson = await jsonClient.query(rustService, "user.getById", { id: 7n })
  emit({ kind: "json-query", id: fromJson.id, idType: typeof fromJson.id })
  emit({ kind: "done" })

  await new Promise((resolve) => {
    process.stdin.on("end", resolve)
    process.stdin.on("close", resolve)
    process.stdin.resume()
  })
  await subscription.unsubscribe()
  await broadcastSub.unsubscribe()
  routerSub.unsubscribe()
  await client.close(1000)
  await jsonClient.close(1000)
  await nc.drain()
}

main().catch((err) => {
  emit({ kind: "fatal", message: err?.stack || String(err) })
  process.exit(1)
})
