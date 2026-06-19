#!/usr/bin/env bun

/// <reference types="bun" />

import { PaidClient } from "@paid-ai/paid-node";
import type { Paid } from "@paid-ai/paid-node";

type Customer = Paid.Customer;
type CustomerTarget = { customerId: string } | { externalCustomerId: string };
type ProductTarget = { externalProductId: string };
type UsageCost = Paid.Cost.Usage;
type CustomMetadata = Record<string, string>;

type Config = {
  apiKey: string;
  externalProductId?: string;
  flushMs: number;
  debug: boolean;
  logFile?: string;
  claudeArgs: string[];
  querySourceAllow?: Set<string>;
  querySourceDeny?: Set<string>;
};

type Summary = {
  received: number;
  enqueued: number;
  submitted: number;
  duplicates: number;
  filtered: number;
  skipped: number;
  failedBatches: number;
  failedRecords: number;
};

type LogRecord = {
  attributes?: OtlpAttribute[];
  body?: unknown;
  timeUnixNano?: string | number;
  observedTimeUnixNano?: string | number;
};

type OtlpAttribute = {
  key: string;
  value?: OtlpAnyValue;
};

type OtlpAnyValue = {
  stringValue?: string;
  intValue?: string | number;
  doubleValue?: string | number;
  boolValue?: boolean;
  arrayValue?: { values?: OtlpAnyValue[] };
  kvlistValue?: { values?: OtlpAttribute[] };
};

type IteratedLogRecord = {
  record: LogRecord;
  attributes: Record<string, unknown>;
};

const RESERVED_METADATA_FIELDS = new Set([
  "type",
  "customer",
  "product",
  "vendor",
  "model",
  "usage",
  "timestamp",
  "costOverride",
]);

class Logger {
  private sink?: { write(input: string): unknown; flush(): unknown };

  constructor(readonly path?: string) {
    if (!path) {
      return;
    }
    Bun.write(path, "");
    this.sink = Bun.file(path).writer();
  }

  get enabled() {
    return this.sink !== undefined;
  }

  write(message: string, detail?: unknown) {
    if (!this.sink) {
      return;
    }

    const line =
      `[${new Date().toISOString()}] ${message}` +
      (detail === undefined ? "" : ` ${safeJson(detail)}`) +
      "\n";
    try {
      this.sink.write(line);
    } catch {
      // Diagnostics should never interfere with Claude Code's terminal UI.
    }
  }

  flush() {
    if (!this.sink) {
      return;
    }

    try {
      this.sink.flush();
    } catch {
      // Best-effort diagnostics.
    }
  }
}

class CostQueue {
  private queue: UsageCost[] = [];
  private flushing = false;
  private flushAgain = false;
  private timer?: ReturnType<typeof setInterval>;

  constructor(
    private readonly client: PaidClient,
    private readonly logger: Logger,
    private readonly summary: Summary,
    private readonly flushMs: number,
  ) {}

  start() {
    this.timer = setInterval(() => {
      this.flush().catch((error) => this.logger.write("timer flush failed", formatError(error)));
    }, this.flushMs);
  }

  stop() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = undefined;
    }
  }

  enqueue(record: UsageCost) {
    this.queue.push(record);
    this.summary.enqueued++;
    if (this.queue.length >= 50) {
      this.flush().catch((error) => this.logger.write("threshold flush failed", formatError(error)));
    }
  }

  size() {
    return this.queue.length;
  }

  async flush() {
    if (this.flushing) {
      this.flushAgain = true;
      return;
    }

    this.flushing = true;
    try {
      do {
        this.flushAgain = false;
        const batch = this.queue.splice(0, 50);
        if (batch.length === 0) {
          return;
        }

        try {
          const response = await this.client.costs.createCosts({ costs: batch });
          this.summary.submitted += response.ingested;
          this.summary.duplicates += response.duplicates;
          this.logger.write("submitted batch", {
            size: batch.length,
            ingested: response.ingested,
            duplicates: response.duplicates,
          });
        } catch (error) {
          this.summary.failedBatches++;
          this.summary.failedRecords += batch.length;
          this.queue.unshift(...batch);
          this.logger.write("failed to submit batch; records retained", {
            size: batch.length,
            error: formatError(error),
          });
          return;
        }
      } while (this.flushAgain && this.queue.length > 0);
    } finally {
      this.flushing = false;
    }
  }

  async drain(maxAttempts = 5) {
    for (let attempt = 1; attempt <= maxAttempts && this.queue.length > 0; attempt++) {
      const before = this.queue.length;
      await this.flush();
      if (this.queue.length >= before) {
        break;
      }
    }
  }
}

function parseArgs(argv: string[]): Config {
  const separatorIndex = argv.indexOf("--");
  const ownArgs = separatorIndex === -1 ? argv : argv.slice(0, separatorIndex);
  const claudeArgs = separatorIndex === -1 ? [] : argv.slice(separatorIndex + 1);

  const env = process.env;
  let externalProductId = env.PAID_EXTERNAL_PRODUCT_ID;
  let flushMs = parsePositiveInt(env.CCPAID_FLUSH_MS, 5000);
  let logFile = env.CCPAID_LOG_FILE;
  let debug = parseBooleanEnv(env.CCPAID_DEBUG);
  let help = false;

  for (let index = 0; index < ownArgs.length; index++) {
    const arg = ownArgs[index];
    if (arg === "-h" || arg === "--help") {
      help = true;
    } else if (arg === "--external-product-id") {
      externalProductId = readFlagValue(ownArgs, ++index, arg);
    } else if (arg.startsWith("--external-product-id=")) {
      externalProductId = arg.slice("--external-product-id=".length);
    } else if (arg === "--flush-ms") {
      flushMs = parsePositiveInt(readFlagValue(ownArgs, ++index, arg), 5000);
    } else if (arg.startsWith("--flush-ms=")) {
      flushMs = parsePositiveInt(arg.slice("--flush-ms=".length), 5000);
    } else if (arg === "--debug") {
      debug = true;
    } else if (arg === "--log-file") {
      logFile = readFlagValue(ownArgs, ++index, arg);
    } else if (arg.startsWith("--log-file=")) {
      logFile = arg.slice("--log-file=".length);
    } else {
      throw new Error(`Unknown ccpaid option: ${arg}. Put Claude Code args after --.`);
    }
  }

  if (debug && !logFile) {
    logFile = defaultLogFile();
  }

  if (help) {
    printHelp();
    process.exit(0);
  }

  const apiKey = env.PAID_API_KEY;
  if (!apiKey) {
    throw new Error("PAID_API_KEY is required.");
  }

  return {
    apiKey,
    externalProductId,
    flushMs,
    debug,
    logFile,
    claudeArgs,
    querySourceAllow: parseCsvSet(env.CCPAID_QUERY_SOURCE_ALLOW),
    querySourceDeny: parseCsvSet(env.CCPAID_QUERY_SOURCE_DENY),
  };
}

function printHelp() {
  process.stdout.write(`ccpaid - run Claude Code with Paid customer usage attribution

Usage:
  ccpaid [options] -- [claude args...]
  bun run ccpaid.ts [options] -- [claude args...]

Options:
  --external-product-id <id>  Attach Paid product attribution by external ID.
  --flush-ms <ms>            Batch flush interval. Default: 5000.
  --debug                    Write compact telemetry diagnostics to a log file.
  --log-file <path>          Enable diagnostics logging at the given path.
  -h, --help                 Show this help.

Environment:
  PAID_API_KEY               Required Paid API key.
  PAID_EXTERNAL_PRODUCT_ID   Same as --external-product-id.
  CCPAID_FLUSH_MS            Same as --flush-ms.
  CCPAID_DEBUG               Same as --debug when set to 1, true, or yes.
  CCPAID_LOG_FILE            Same as --log-file.
  CCPAID_QUERY_SOURCE_ALLOW  Optional comma-separated query_source allow list.
  CCPAID_QUERY_SOURCE_DENY   Optional comma-separated query_source deny list.
`);
}

async function main() {
  let config: Config;
  try {
    config = parseArgs(process.argv.slice(2));
  } catch (error) {
    process.stderr.write(`ccpaid: ${formatErrorMessage(error)}\n`);
    process.stderr.write("Run ccpaid -h for usage.\n");
    process.exit(1);
  }

  const logger = new Logger(config.logFile);
  const summary: Summary = {
    received: 0,
    enqueued: 0,
    submitted: 0,
    duplicates: 0,
    filtered: 0,
    skipped: 0,
    failedBatches: 0,
    failedRecords: 0,
  };

  if (logger.enabled && config.logFile) {
    process.stdout.write(`ccpaid diagnostics: ${config.logFile}\n`);
  }
  logger.write("starting ccpaid", {
    flushMs: config.flushMs,
    claudeArgs: config.claudeArgs,
    externalProductId: config.externalProductId,
    debug: config.debug,
  });

  const client = new PaidClient({ token: config.apiKey });
  const customers = await listAllCustomers(client, logger);
  const target = await selectCustomer(customers);
  const customMetadata = await collectCustomMetadata();
  const product = config.externalProductId ? { externalProductId: config.externalProductId } : undefined;
  const queue = new CostQueue(client, logger, summary, config.flushMs);
  const server = createReceiver({ queue, summary, logger, target, product, config, customMetadata });
  const port = server.port;
  if (port === undefined) {
    throw new Error("Failed to start local OTLP receiver on a TCP port.");
  }

  logger.write("receiver started", { port });
  queue.start();

  const child = spawnClaude(port, config.claudeArgs, logger);
  let exitCode = 1;

  try {
    exitCode = await child.exited;
  } finally {
    await sleep(500);
    queue.stop();
    await queue.drain();
    server.stop();
    logger.write("shutdown", { exitCode, summary, undelivered: queue.size() });
    logger.flush();
  }

  process.stdout.write(
    `ccpaid: submitted=${summary.submitted} duplicates=${summary.duplicates} ` +
      `filtered=${summary.filtered} skipped=${summary.skipped} undelivered=${queue.size()}` +
      (config.logFile ? ` log=${config.logFile}` : "") +
      "\n",
  );
  process.exit(exitCode ?? 0);
}

async function listAllCustomers(client: PaidClient, logger: Logger): Promise<Customer[]> {
  const limit = 100;
  let offset = 0;
  const customers: Customer[] = [];

  while (true) {
    const response = await client.customers.listCustomers({ limit, offset });
    customers.push(...response.data);
    logger.write("listed customers page", {
      count: response.data.length,
      offset,
      total: response.pagination.total,
      hasMore: response.pagination.hasMore,
    });

    if (!response.pagination.hasMore || response.data.length === 0) {
      break;
    }
    offset += limit;
  }

  if (customers.length === 0) {
    throw new Error("No Paid customers found.");
  }

  return customers;
}

async function selectCustomer(customers: Customer[]): Promise<CustomerTarget> {
  if (process.stdin.isTTY && process.stdout.isTTY) {
    const customer = await selectCustomerTui(customers);
    return { customerId: customer.id };
  }

  const customer = await selectCustomerNumbered(customers);
  return { customerId: customer.id };
}

async function selectCustomerNumbered(customers: Customer[]): Promise<Customer> {
  const visible = customers.slice(0, 50);
  process.stdout.write("Select Paid customer:\n");
  visible.forEach((customer, index) => {
    process.stdout.write(`${index + 1}. ${formatCustomer(customer)}\n`);
  });
  process.stdout.write("> ");

  const input = await readLine();
  const selectedIndex = Number(input.trim()) - 1;
  const customer = visible[selectedIndex];
  if (!customer) {
    throw new Error("Invalid customer selection.");
  }
  return customer;
}

async function selectCustomerTui(customers: Customer[]): Promise<Customer> {
  let query = "";
  let selected = 0;
  let filtered = filterCustomers(customers, query);

  const stdin = process.stdin;

  return await new Promise<Customer>((resolve, reject) => {
    const cleanup = () => {
      stdin.off("data", onData);
      if (typeof stdin.setRawMode === "function") {
        stdin.setRawMode(false);
      }
      stdin.pause();
      process.stdout.write("\x1b[?25h\x1b[2J\x1b[H");
    };

    const finish = (customer: Customer) => {
      cleanup();
      resolve(customer);
    };

    const fail = (error: Error) => {
      cleanup();
      reject(error);
    };

    const render = () => {
      filtered = filterCustomers(customers, query);
      if (selected >= filtered.length) {
        selected = Math.max(filtered.length - 1, 0);
      }

      const windowStart = Math.max(0, Math.min(selected - 9, Math.max(filtered.length - 10, 0)));
      const rows = filtered.slice(windowStart, windowStart + 10);
      process.stdout.write("\x1b[?25l\x1b[2J\x1b[H");
      process.stdout.write("PaidCode customer\n\n");
      process.stdout.write(`Search: ${query}\n\n`);

      if (rows.length === 0) {
        process.stdout.write("  No customers match your search.\n");
      } else {
        rows.forEach((customer, index) => {
          const absoluteIndex = windowStart + index;
          const pointer = absoluteIndex === selected ? ">" : " ";
          process.stdout.write(`${pointer} ${formatCustomer(customer)}\n`);
        });
        process.stdout.write(`\nShowing ${windowStart + 1}-${windowStart + rows.length} of ${filtered.length}\n`);
      }

      process.stdout.write("\nType to search, arrows to move, Enter to select, Ctrl+C to cancel.\n");
    };

    const onData = (chunk: Buffer | string) => {
      const key = chunk.toString("utf8");
      if (key === "\u0003") {
        fail(new Error("Cancelled."));
      } else if (key === "\r" || key === "\n") {
        const customer = filtered[selected];
        if (customer) {
          finish(customer);
        }
      } else if (key === "\u001b[A") {
        selected = Math.max(0, selected - 1);
        render();
      } else if (key === "\u001b[B") {
        selected = Math.min(Math.max(filtered.length - 1, 0), selected + 1);
        render();
      } else if (key === "\u007f" || key === "\b") {
        query = query.slice(0, -1);
        selected = 0;
        render();
      } else if (key === "\u001b") {
        // Ignore bare escape.
      } else if (/^[ -~]$/.test(key)) {
        query += key;
        selected = 0;
        render();
      }
    };

    if (typeof stdin.setRawMode === "function") {
      stdin.setRawMode(true);
    }
    stdin.resume();
    stdin.on("data", onData);
    render();
  });
}

function filterCustomers(customers: Customer[], query: string): Customer[] {
  const normalized = query.trim().toLowerCase();
  if (!normalized) {
    return customers;
  }

  return customers.filter((customer) => {
    return [customer.name, customer.id, customer.externalId]
      .filter(Boolean)
      .some((value) => String(value).toLowerCase().includes(normalized));
  });
}

async function collectCustomMetadata(): Promise<CustomMetadata> {
  if (!process.stdin.isTTY || !process.stdout.isTTY) {
    return {};
  }

  const shouldAdd = (await promptLine("Add custom metadata? (y/N) ")).trim().toLowerCase();
  if (shouldAdd !== "y" && shouldAdd !== "yes") {
    return {};
  }

  const metadata: CustomMetadata = {};
  process.stdout.write("Enter custom metadata. Leave field name blank when done.\n");

  while (true) {
    const field = (await promptLine("Field name: ")).trim();
    if (!field) {
      break;
    }

    const validationError = validateCustomMetadataField(field);
    if (validationError) {
      process.stdout.write(`${validationError}\n`);
      continue;
    }

    metadata[field] = await promptLine("Value: ");
  }

  return metadata;
}

function createReceiver(args: {
  queue: CostQueue;
  summary: Summary;
  logger: Logger;
  target: CustomerTarget;
  product?: ProductTarget;
  config: Config;
  customMetadata: CustomMetadata;
}) {
  const { queue, summary, logger, target, product, config, customMetadata } = args;

  return Bun.serve({
    port: 0,
    hostname: "127.0.0.1",
    async fetch(req) {
      if (req.method !== "POST") {
        return new Response("not found", { status: 404 });
      }

      try {
        const payload = await parseOtlpRequest(req);
        for (const item of iterateLogRecords(payload)) {
          const eventName = stringAttr(item.attributes["event.name"]);
          if (eventName !== "api_request") {
            continue;
          }

          summary.received++;
          const querySource = stringAttr(item.attributes.query_source);
          if (!isQuerySourceAllowed(querySource, config)) {
            summary.filtered++;
            continue;
          }

          const record = buildUsageRecord(item, target, product, customMetadata);
          if (!record) {
            summary.skipped++;
            logger.write("skipped api_request record", {
              reason: "missing required fields or token usage",
              attributes: compactAttributes(item.attributes),
            });
            continue;
          }

          if (config.debug) {
            logger.write("api_request telemetry", compactTelemetryDebug(item.attributes, record));
          }

          queue.enqueue(record);
        }
      } catch (error) {
        logger.write("failed to process otlp request", formatError(error));
      }

      return new Response("{}", {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    },
  });
}

async function parseOtlpRequest(req: Request): Promise<unknown> {
  const body = await req.arrayBuffer();
  const encoding = req.headers.get("content-encoding")?.toLowerCase();
  const bytes = new Uint8Array(body);
  let decoded: Uint8Array;

  if (encoding === "gzip") {
    decoded = Bun.gunzipSync(bytes);
  } else if (encoding === "deflate") {
    decoded = Bun.inflateSync(bytes);
  } else {
    decoded = bytes;
  }

  return JSON.parse(new TextDecoder().decode(decoded));
}

export function* iterateLogRecords(payload: unknown): Generator<IteratedLogRecord> {
  const root = payload as {
    resourceLogs?: Array<{
      resource?: { attributes?: OtlpAttribute[] };
      scopeLogs?: Array<{ logRecords?: LogRecord[] }>;
    }>;
  };

  for (const resourceLog of root.resourceLogs ?? []) {
    const resourceAttributes = attributesToObject(resourceLog.resource?.attributes);
    for (const scopeLog of resourceLog.scopeLogs ?? []) {
      for (const record of scopeLog.logRecords ?? []) {
        yield {
          record,
          attributes: {
            ...resourceAttributes,
            ...attributesToObject(record.attributes),
          },
        };
      }
    }
  }
}

export function attributesToObject(attributes: OtlpAttribute[] | undefined): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const attribute of attributes ?? []) {
    result[attribute.key] = coerceOtlpValue(attribute.value);
  }
  return result;
}

export function coerceOtlpValue(value: OtlpAnyValue | undefined): unknown {
  if (!value) {
    return undefined;
  }
  if ("stringValue" in value) {
    return value.stringValue;
  }
  if ("intValue" in value) {
    return Number(value.intValue);
  }
  if ("doubleValue" in value) {
    return Number(value.doubleValue);
  }
  if ("boolValue" in value) {
    return value.boolValue;
  }
  if (value.arrayValue) {
    return (value.arrayValue.values ?? []).map(coerceOtlpValue);
  }
  if (value.kvlistValue) {
    return attributesToObject(value.kvlistValue.values);
  }
  return undefined;
}

export function buildUsageRecord(
  item: IteratedLogRecord,
  customer: CustomerTarget,
  product?: ProductTarget,
  customMetadata: CustomMetadata = {},
): UsageCost | undefined {
  const attrs = item.attributes;
  const model = stringAttr(attrs.model);
  if (!model) {
    return undefined;
  }

  const usage = {
    inputTokens: positiveIntegerAttr(attrs.input_tokens),
    outputTokens: positiveIntegerAttr(attrs.output_tokens),
    cacheReadInputTokens: positiveIntegerAttr(attrs.cache_read_tokens),
    cacheCreationInputTokens: positiveIntegerAttr(attrs.cache_creation_tokens),
  };

  const hasUsage = Object.values(usage).some((value) => typeof value === "number" && value > 0);
  if (!hasUsage) {
    return undefined;
  }

  const record: UsageCost = {
    type: "usage",
    customer,
    vendor: "anthropic",
    model,
    usage: stripUndefined(usage),
  };

  if (product) {
    record.product = product;
  }

  const timestamp = timestampFromRecord(item);
  if (timestamp) {
    record.timestamp = timestamp;
  }

  const metadata = stripUndefined({
    request_id: stringAttr(attrs.request_id),
    "session.id": stringAttr(attrs["session.id"]),
    "prompt.id": stringAttr(attrs["prompt.id"]),
    "event.sequence": numberAttr(attrs["event.sequence"]),
    query_source: stringAttr(attrs.query_source),
    speed: stringAttr(attrs.speed),
    effort: stringAttr(attrs.effort),
    duration_ms: numberAttr(attrs.duration_ms),
    cost_usd: numberAttr(attrs.cost_usd),
    "workspace.host_paths": metadataAttr(attrs["workspace.host_paths"]),
    app_version: stringAttr(attrs["app.version"]),
    "terminal.type": stringAttr(attrs["terminal.type"]),
    "user.id": stringAttr(attrs["user.id"]),
    user_email: stringAttr(attrs["user.email"]),
    "organization.id": stringAttr(attrs["organization.id"]),
    "agent.name": stringAttr(attrs["agent.name"]),
    "skill.name": stringAttr(attrs["skill.name"]),
    "plugin.name": stringAttr(attrs["plugin.name"]),
    "marketplace.name": stringAttr(attrs["marketplace.name"]),
    "mcp_server.name": stringAttr(attrs["mcp_server.name"]),
    "mcp_tool.name": stringAttr(attrs["mcp_tool.name"]),
    ...namespaceCustomMetadata(customMetadata),
  });

  if (Object.keys(metadata).length > 0) {
    record.metadata = metadata;
  }

  return record;
}

function spawnClaude(port: number, args: string[], logger: Logger) {
  const env = { ...process.env };
  delete env.OTEL_EXPORTER_OTLP_PROTOCOL;
  delete env.OTEL_EXPORTER_OTLP_ENDPOINT;

  Object.assign(env, {
    CLAUDE_CODE_ENABLE_TELEMETRY: "1",
    OTEL_LOGS_EXPORTER: "otlp",
    OTEL_METRICS_EXPORTER: "none",
    OTEL_EXPORTER_OTLP_LOGS_PROTOCOL: "http/json",
    OTEL_EXPORTER_OTLP_LOGS_ENDPOINT: `http://127.0.0.1:${port}/v1/logs`,
    OTEL_EXPORTER_OTLP_LOGS_COMPRESSION: "none",
    OTEL_LOGS_EXPORT_INTERVAL: "3000",
  });

  logger.write("spawning claude", {
    args,
    logsEndpoint: env.OTEL_EXPORTER_OTLP_LOGS_ENDPOINT,
  });

  return Bun.spawn(["claude", ...args], {
    stdio: ["inherit", "inherit", "inherit"],
    env,
  });
}

function timestampFromRecord(item: IteratedLogRecord): string | undefined {
  const timestamp = stringAttr(item.attributes["event.timestamp"]);
  if (timestamp) {
    return timestamp;
  }

  const unixNano = item.record.timeUnixNano ?? item.record.observedTimeUnixNano;
  if (unixNano === undefined) {
    return undefined;
  }

  try {
    const ms = Number(BigInt(String(unixNano)) / 1_000_000n);
    return new Date(ms).toISOString();
  } catch {
    return undefined;
  }
}

function isQuerySourceAllowed(querySource: string | undefined, config: Config): boolean {
  if (config.querySourceAllow?.size) {
    return querySource !== undefined && config.querySourceAllow.has(querySource);
  }
  if (querySource && config.querySourceDeny?.has(querySource)) {
    return false;
  }
  return true;
}

function metadataAttr(value: unknown): unknown {
  return value === undefined ? undefined : value;
}

function stringAttr(value: unknown): string | undefined {
  if (typeof value === "string" && value.length > 0) {
    return value;
  }
  return undefined;
}

function numberAttr(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }
  return undefined;
}

function positiveIntegerAttr(value: unknown): number | undefined {
  const parsed = numberAttr(value);
  if (parsed === undefined || parsed <= 0) {
    return undefined;
  }
  return Math.trunc(parsed);
}

function stripUndefined<T extends Record<string, unknown>>(value: T): T {
  return Object.fromEntries(Object.entries(value).filter(([, item]) => item !== undefined)) as T;
}

function namespaceCustomMetadata(metadata: CustomMetadata): Record<string, string> {
  return Object.fromEntries(Object.entries(metadata).map(([key, value]) => [`custom.${key}`, value]));
}

function validateCustomMetadataField(field: string): string | undefined {
  if (field.length > 80) {
    return "Field name must be 80 characters or fewer.";
  }
  if (!/^[A-Za-z0-9_.-]+$/.test(field)) {
    return "Field name can only contain letters, numbers, dots, underscores, and hyphens.";
  }
  if (RESERVED_METADATA_FIELDS.has(field)) {
    return `Field name '${field}' is reserved.`;
  }
  return undefined;
}

function compactAttributes(attributes: Record<string, unknown>) {
  return stripUndefined({
    "event.name": attributes["event.name"],
    model: attributes.model,
    input_tokens: attributes.input_tokens,
    output_tokens: attributes.output_tokens,
    cache_read_tokens: attributes.cache_read_tokens,
    cache_creation_tokens: attributes.cache_creation_tokens,
    request_id: attributes.request_id,
    query_source: attributes.query_source,
    "session.id": attributes["session.id"],
    "event.sequence": attributes["event.sequence"],
  });
}

function compactTelemetryDebug(attributes: Record<string, unknown>, record: UsageCost) {
  return stripUndefined({
    model: attributes.model,
    input_tokens: attributes.input_tokens,
    output_tokens: attributes.output_tokens,
    cache_read_tokens: attributes.cache_read_tokens,
    cache_creation_tokens: attributes.cache_creation_tokens,
    cost_usd: attributes.cost_usd,
    request_id: attributes.request_id,
    "session.id": attributes["session.id"],
    "prompt.id": attributes["prompt.id"],
    query_source: attributes.query_source,
    metadataKeys: record.metadata ? Object.keys(record.metadata).sort() : [],
  });
}

function parseCsvSet(value: string | undefined): Set<string> | undefined {
  const items = value
    ?.split(",")
    .map((item) => item.trim())
    .filter(Boolean);
  return items && items.length > 0 ? new Set(items) : undefined;
}

function parseBooleanEnv(value: string | undefined): boolean {
  if (!value) {
    return false;
  }
  return ["1", "true", "yes"].includes(value.trim().toLowerCase());
}

function parsePositiveInt(value: string | undefined, fallback: number): number {
  if (!value) {
    return fallback;
  }
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

function readFlagValue(args: string[], index: number, flag: string): string {
  const value = args[index];
  if (!value || value.startsWith("--")) {
    throw new Error(`${flag} requires a value.`);
  }
  return value;
}

function formatCustomer(customer: Customer): string {
  const external = customer.externalId ? ` externalId=${customer.externalId}` : "";
  return `${customer.name} (${customer.id}${external})`;
}

async function promptLine(prompt: string): Promise<string> {
  process.stdout.write(prompt);
  return await readLine();
}

async function readLine(): Promise<string> {
  return await new Promise((resolve) => {
    let buffer = "";
    const onData = (chunk: Buffer | string) => {
      buffer += chunk.toString();
      if (buffer.includes("\n")) {
        process.stdin.off("data", onData);
        process.stdin.pause();
        resolve(buffer.split(/\r?\n/, 1)[0] ?? "");
      }
    };
    process.stdin.resume();
    process.stdin.on("data", onData);
  });
}

function defaultLogFile() {
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  return `./ccpaid-${stamp}.log`;
}

function safeJson(value: unknown): string {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function formatError(error: unknown): string {
  if (error instanceof Error) {
    return error.stack || error.message;
  }
  return String(error);
}

function formatErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

if (import.meta.main) {
  main().catch((error) => {
    process.stderr.write(`ccpaid: ${formatErrorMessage(error)}\n`);
    process.exit(1);
  });
}
