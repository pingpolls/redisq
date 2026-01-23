import { RedisClient as BunRedisClient } from "bun";
import { Cron } from "croner";

export type QueueOptions =
    | {
          redis: BunRedisClient;
      }
    | {
          host: string;
          port: string;
          user?: string;
          password?: string;
          namespace?: string;
          tls?: boolean;
      };

/**
 * Configuration for creating a queue
 * Regular queues process messages individually
 * Batch queues (suffix ':batch') collect messages and process them in batches at specified intervals
 */
export type CreateQueueOptions<QueueName extends string = string> =
    QueueName extends `${string}:batch`
        ? {
              /**
               * Queue name ending with ':batch' to enable batch processing
               */
              qname: QueueName;
              /**
               * Maximum message size in bytes
               * @default 65536
               */
              maxsize?: number;
              /**
               * Maximum number of retries per batch
               * undefined = no retry (1 attempt), -1 = unlimited, n = max retries
               * @default 0
               */
              maxRetries?: number;
              /**
               * Maximum backoff time in seconds for exponential backoff
               * @default 30
               */
              maxBackoffSeconds?: number;
              /**
               * Interval in seconds for collecting and processing batches
               * @default 60
               */
              every?: number;
          }
        : {
              /**
               * Queue name for regular message processing
               */
              qname: QueueName;
              /**
               * Maximum message size in bytes
               * @default 65536
               */
              maxsize?: number;
              /**
               * Maximum number of retries per message
               * undefined = no retry (1 attempt), -1 = unlimited, n = max retries
               * @default 0
               */
              maxRetries?: number;
              /**
               * Maximum backoff time in seconds for exponential backoff
               * @default 30
               */
              maxBackoffSeconds?: number;
          };

export interface QueueAttributes {
    maxsize: number;
    created: number;
    msgs: number;
    isBatch: boolean;
    maxRetries: number;
    maxBackoffSeconds: number;
    every?: number;
}

export interface SendMessageOptions {
    qname: string;
    message: string;
    /**
     * Delay in milliseconds before message is available for processing
     */
    delay?: number;
}

export interface SendBatchMessageOptions {
    qname: string;
    /**
     * User-defined batch ID to group messages together
     */
    batchId: string;
    message: string;
}

export interface Message {
    id: string;
    message: string;
    /**
     * Timestamp when message was sent
     */
    sent: number;
    /**
     * Current attempt number (1-based)
     */
    attempt: number;
}

export interface BatchMessage {
    batchId: string;
    messages: Omit<Message, "attempt">[];
    /**
     * Timestamp when batch was created
     */
    sent: number;
    /**
     * Batch-level attempt counter
     */
    attempt: number;
}

interface StoredMessage {
    id: string;
    message: string;
    sent: number;
    attempt: number;
}

interface StoredBatchMeta {
    batchId: string;
    sent: number;
    attempt: number;
}

export class RedisQueue {
    private redis: BunRedisClient;
    private redisUrl: string;
    private ns: string;
    private workers: Map<string, AbortController> = new Map();
    private batchJobs: Map<string, Cron> = new Map();

    constructor(options: QueueOptions) {
        if ("redis" in options) {
            this.redis = options.redis;
            this.redisUrl = "redis://localhost:6379";
            this.ns = "rsmq";
        } else {
            const { host, port, user, password, namespace, tls } = options;
            const protocol = tls ? "rediss" : "redis";

            let auth = "";
            if (user && password) {
                auth = `${user}:${password}@`;
            } else if (password) {
                auth = `:${password}@`;
            }

            this.redisUrl = `${protocol}://${auth}${host}:${port}`;
            this.redis = new BunRedisClient(this.redisUrl);
            this.ns = namespace || "rsmq";
        }
    }

    /**
     * Constructs a Redis key with namespace and optional suffix
     */
    private getKey(qname: string, suffix = ""): string {
        return suffix ? `${this.ns}:${qname}:${suffix}` : `${this.ns}:${qname}`;
    }

    /**
     * Checks if queue name indicates batch processing
     */
    private isBatchQueue(qname: string): boolean {
        return qname.endsWith(":batch");
    }

    /**
     * Creates a new queue with specified configuration
     * @returns true if queue was created, false if already exists
     */
    async createQueue<QueueName extends string>(
        options: CreateQueueOptions<QueueName>,
    ): Promise<boolean> {
        const {
            qname,
            maxsize = 65536,
            maxRetries = 0,
            maxBackoffSeconds = 30,
        } = options;

        if (!/^[a-zA-Z0-9_:-]{1,160}$/.test(qname)) {
            throw new Error("Invalid queue name");
        }

        const key = this.getKey(qname);
        const exists = await this.redis.exists(key);

        if (exists) {
            return false;
        }

        const isBatch = this.isBatchQueue(qname);

        const attrs: Record<string, string | number> = {
            created: Date.now(),
            maxBackoffSeconds,
            maxRetries,
            maxsize,
            type: isBatch ? "batch" : "single",
        };

        if (isBatch && "every" in options) {
            attrs.every = options.every ?? 60;
        }

        await this.redis.hset(key, attrs);
        return true;
    }

    /**
     * Lists all queue names
     */
    async listQueues(): Promise<string[]> {
        const pattern = `${this.ns}:*`;
        const keys = await this.redis.keys(pattern);

        return keys
            .map((k) => k.replace(`${this.ns}:`, ""))
            .filter((k) => !k.includes(":") || k.endsWith(":batch"));
    }

    /**
     * Retrieves queue attributes and message count
     */
    async getQueue(qname: string): Promise<QueueAttributes | null> {
        const key = this.getKey(qname);
        const attrs = await this.redis.hgetall(key);

        if (!attrs || Object.keys(attrs).length === 0) {
            return null;
        }

        const isBatch = attrs.type === "batch";
        const messagesKey = this.getKey(qname, "messages");
        const delayedKey = this.getKey(qname, "delayed");

        const [msgCount, delayedCount] = await Promise.all([
            this.redis.send("HLEN", [messagesKey]),
            this.redis.send("ZCARD", [delayedKey]),
        ]);

        const result: QueueAttributes = {
            created: Number(attrs.created),
            isBatch,
            maxBackoffSeconds: Number(attrs.maxBackoffSeconds),
            maxRetries: Number(attrs.maxRetries),
            maxsize: Number(attrs.maxsize),
            msgs: (msgCount as number) + (delayedCount as number),
        };

        if (isBatch && attrs.every) {
            result.every = Number(attrs.every);
        }

        return result;
    }

    /**
     * Deletes a queue and all its associated data
     */
    async deleteQueue(qname: string): Promise<boolean> {
        const pattern = `${this.ns}:${qname}*`;
        const keys = await this.redis.keys(pattern);

        if (keys.length === 0) {
            return false;
        }

        await this.redis.del(...keys);
        return true;
    }

    /**
     * Sends a message to a regular queue
     */
    async sendMessage(options: SendMessageOptions): Promise<string> {
        const { qname, message, delay } = options;

        if (this.isBatchQueue(qname)) {
            throw new Error("Use sendBatchMessage for batch queues");
        }

        const key = this.getKey(qname);
        const attrs = await this.redis.hgetall(key);

        if (!attrs || Object.keys(attrs).length === 0) {
            throw new Error("Queue not found");
        }

        const maxsize = Number(attrs.maxsize);
        if (message.length > maxsize) {
            throw new Error(`Message too large. Max size: ${maxsize}`);
        }

        const now = Date.now();
        const id = Bun.randomUUIDv7("hex", now);

        const storedMsg: StoredMessage = {
            attempt: 1,
            id,
            message,
            sent: now,
        };

        const messagesKey = this.getKey(qname, "messages");
        const encoded = this.encodeMessage(storedMsg);
        await this.redis.hset(messagesKey, { [id]: encoded });

        if (delay && delay > 0) {
            const delayedKey = this.getKey(qname, "delayed");
            const deliverAt = now + delay;
            await this.redis.send("ZADD", [
                delayedKey,
                deliverAt.toString(),
                id,
            ]);
        } else {
            const queueKey = this.getKey(qname, "queue");
            await this.redis.send("LPUSH", [queueKey, id]);
        }

        return id;
    }

    /**
     * Sends a message to a batch queue
     * Messages are grouped by batchId and processed together at scheduled intervals
     */
    async sendBatchMessage(options: SendBatchMessageOptions): Promise<string> {
        const { qname, batchId, message } = options;

        if (!this.isBatchQueue(qname)) {
            throw new Error("Use sendMessage for non-batch queues");
        }

        const key = this.getKey(qname);
        const attrs = await this.redis.hgetall(key);

        if (!attrs || Object.keys(attrs).length === 0) {
            throw new Error("Queue not found");
        }

        const maxsize = Number(attrs.maxsize);
        if (message.length > maxsize) {
            throw new Error(`Message too large. Max size: ${maxsize}`);
        }

        const now = Date.now();
        const id = Bun.randomUUIDv7("hex", now);

        const storedMsg: StoredMessage = {
            attempt: 1,
            id,
            message,
            sent: now,
        };

        const messagesKey = this.getKey(qname, "messages");
        const encoded = this.encodeMessage(storedMsg);
        await this.redis.hset(messagesKey, { [id]: encoded });

        const batchKey = this.getKey(qname, `batch:${batchId}`);
        await this.redis.sadd(batchKey, id);

        const batchMetaKey = this.getKey(qname, "batch-meta");
        const batchExists = await this.redis.hexists(batchMetaKey, batchId);

        if (!batchExists) {
            const batchMeta: StoredBatchMeta = {
                attempt: 1,
                batchId,
                sent: now,
            };
            const encodedMeta = this.encodeBatchMeta(batchMeta);
            await this.redis.hset(batchMetaKey, { [batchId]: encodedMeta });

            const pendingBatchesKey = this.getKey(qname, "pending-batches");
            await this.redis.sadd(pendingBatchesKey, batchId);
        }

        return id;
    }

    /**
     * Encodes a message for storage
     */
    private encodeMessage(msg: StoredMessage): string {
        return JSON.stringify({
            attempt: msg.attempt,
            id: msg.id,
            message: msg.message,
            sent: msg.sent,
        });
    }

    /**
     * Decodes a stored message
     */
    private decodeMessage(encoded: string): StoredMessage {
        const parsed = JSON.parse(encoded);
        return {
            attempt: parsed.attempt,
            id: parsed.id,
            message: parsed.message,
            sent: parsed.sent,
        };
    }

    /**
     * Encodes batch metadata for storage
     */
    private encodeBatchMeta(meta: StoredBatchMeta): string {
        return JSON.stringify(meta);
    }

    /**
     * Decodes batch metadata
     */
    private decodeBatchMeta(encoded: string): StoredBatchMeta {
        return JSON.parse(encoded);
    }

    /**
     * Fetches a single message from storage
     */
    private async fetchMessage(
        qname: string,
        id: string,
    ): Promise<Message | null> {
        const messagesKey = this.getKey(qname, "messages");
        const encoded = await this.redis.hget(messagesKey, id);

        if (!encoded) {
            return null;
        }

        const stored = this.decodeMessage(encoded);
        return {
            attempt: stored.attempt,
            id: stored.id,
            message: stored.message,
            sent: stored.sent,
        };
    }

    /**
     * Fetches all messages in a batch
     */
    private async fetchBatchMessage(
        qname: string,
        batchId: string,
    ): Promise<BatchMessage | null> {
        const batchKey = this.getKey(qname, `batch:${batchId}`);
        const batchMetaKey = this.getKey(qname, "batch-meta");

        const [messageIds, metaEncoded] = await Promise.all([
            this.redis.send("SMEMBERS", [batchKey]),
            this.redis.hget(batchMetaKey, batchId),
        ]);

        if (
            !messageIds ||
            (messageIds as string[]).length === 0 ||
            !metaEncoded
        ) {
            return null;
        }

        const meta = this.decodeBatchMeta(metaEncoded);
        const messages: Omit<Message, "attempt">[] = [];

        for (const id of messageIds as string[]) {
            const msg = await this.fetchMessage(qname, id);
            if (msg) {
                const { attempt, ...msgWithoutAttempt } = msg;
                messages.push(msgWithoutAttempt);
            }
        }

        return {
            attempt: meta.attempt,
            batchId: meta.batchId,
            messages,
            sent: meta.sent,
        };
    }

    /**
     * Deletes a message from storage
     */
    async deleteMessage(qname: string, id: string): Promise<boolean> {
        const messagesKey = this.getKey(qname, "messages");
        const deleted = await this.redis.hdel(messagesKey, id);
        return deleted > 0;
    }

    /**
     * Deletes an entire batch and all its messages
     */
    private async deleteBatch(qname: string, batchId: string): Promise<void> {
        const batchKey = this.getKey(qname, `batch:${batchId}`);
        const batchMetaKey = this.getKey(qname, "batch-meta");

        const messageIds = (await this.redis.send("SMEMBERS", [batchKey])) as
            | string[]
            | null;

        if (messageIds && messageIds.length > 0) {
            const messagesKey = this.getKey(qname, "messages");
            for (const id of messageIds) {
                await this.redis.hdel(messagesKey, id);
            }
        }

        await this.redis.del(batchKey);
        await this.redis.hdel(batchMetaKey, batchId);
    }

    /**
     * Retries a failed message with exponential backoff
     */
    private async retryMessage(qname: string, id: string): Promise<void> {
        const key = this.getKey(qname);
        const attrs = await this.redis.hgetall(key);

        if (!attrs || Object.keys(attrs).length === 0) {
            return;
        }

        const maxRetries = Number(attrs.maxRetries);
        const maxBackoffSeconds = Number(attrs.maxBackoffSeconds);

        const messagesKey = this.getKey(qname, "messages");
        const encoded = await this.redis.hget(messagesKey, id);

        if (!encoded) {
            return;
        }

        const msg = this.decodeMessage(encoded);
        const { attempt } = msg;

        if (maxRetries === 0 || (maxRetries !== -1 && attempt > maxRetries)) {
            await this.redis.hdel(messagesKey, id);
            return;
        }

        const randomMs = Math.floor(Math.random() * 1000);
        const backoffSeconds = Math.min(
            2 ** attempt + randomMs / 1000,
            maxBackoffSeconds,
        );
        const backoffMs = backoffSeconds * 1000;

        msg.attempt += 1;
        const updatedEncoded = this.encodeMessage(msg);
        await this.redis.hset(messagesKey, { [id]: updatedEncoded });

        const delayedKey = this.getKey(qname, "delayed");
        const deliverAt = Date.now() + backoffMs;
        await this.redis.send("ZADD", [delayedKey, deliverAt.toString(), id]);
    }

    /**
     * Retries a failed batch by re-queuing it for next batch processing cycle
     */
    private async retryBatch(qname: string, batchId: string): Promise<void> {
        const key = this.getKey(qname);
        const attrs = await this.redis.hgetall(key);

        if (!attrs || Object.keys(attrs).length === 0) {
            return;
        }

        const maxRetries = Number(attrs.maxRetries);

        const batchMetaKey = this.getKey(qname, "batch-meta");
        const metaEncoded = await this.redis.hget(batchMetaKey, batchId);

        if (!metaEncoded) {
            return;
        }

        const meta = this.decodeBatchMeta(metaEncoded);
        const { attempt } = meta;

        if (maxRetries === 0 || (maxRetries !== -1 && attempt > maxRetries)) {
            await this.deleteBatch(qname, batchId);
            return;
        }

        meta.attempt += 1;
        const updatedEncoded = this.encodeBatchMeta(meta);
        await this.redis.hset(batchMetaKey, { [batchId]: updatedEncoded });

        const pendingBatchesKey = this.getKey(qname, "pending-batches");
        await this.redis.sadd(pendingBatchesKey, batchId);
    }

    /**
     * Processes delayed messages that are ready for execution
     */
    private async processDelayedMessages(qname: string): Promise<void> {
        const delayedKey = this.getKey(qname, "delayed");
        const queueKey = this.getKey(qname, "queue");
        const now = Date.now();

        const ready = await this.redis.send("ZRANGEBYSCORE", [
            delayedKey,
            "-inf",
            now.toString(),
        ]);

        if (ready && (ready as string[]).length > 0) {
            for (const id of ready as string[]) {
                await this.redis.send("LPUSH", [queueKey, id]);
                await this.redis.send("ZREM", [delayedKey, id]);
            }
        }
    }

    /**
     * Processes all pending batches in a single cycle
     * Retrieves all batch IDs, fetches their messages, and processes them concurrently
     */
    private async processBatches(
        qname: string,
        handler: (message: BatchMessage) => Promise<{ success: boolean }>,
        silent: boolean,
    ): Promise<void> {
        const pendingBatchesKey = this.getKey(qname, "pending-batches");
        const batchIds = (await this.redis.send("SMEMBERS", [
            pendingBatchesKey,
        ])) as string[] | null;

        if (!batchIds || batchIds.length === 0) {
            return;
        }

        const batchPromises = batchIds.map(async (batchId) => {
            await this.redis.srem(pendingBatchesKey, batchId);

            const batchMessage = await this.fetchBatchMessage(qname, batchId);

            if (!batchMessage) {
                return;
            }

            try {
                const { success } = await handler(batchMessage);

                if (success) {
                    await this.deleteBatch(qname, batchId);
                } else {
                    await this.retryBatch(qname, batchId);
                }
            } catch (error) {
                if (!silent) {
                    console.error(
                        `Batch ${batchId} handler error:`,
                        (error as Error).message,
                    );
                }
                await this.retryBatch(qname, batchId);
            }
        });

        await Promise.all(batchPromises);
    }

    /**
     * Starts a worker for processing messages
     * For regular queues, processes messages individually as they arrive
     * For batch queues, collects messages and processes them in batches at scheduled intervals
     */
    async startWorker<QueueName extends `${string}:batch` | (string & {})>(
        qname: QueueName,
        handler: (
            message: QueueName extends `${string}:batch`
                ? BatchMessage
                : Message,
        ) => Promise<{ success: boolean }>,
        options: { concurrency?: number; silent?: boolean } = {},
    ): Promise<void> {
        const { concurrency = 1, silent = false } = options;

        if (this.workers.has(qname)) {
            throw new Error(`Worker already running for queue: ${qname}`);
        }

        const isBatch = this.isBatchQueue(qname);

        if (isBatch) {
            const key = this.getKey(qname);
            const attrs = await this.redis.hgetall(key);

            if (!attrs || Object.keys(attrs).length === 0) {
                throw new Error("Queue not found");
            }

            const every = Number(attrs.every || 60);

            const job = new Cron(`*/${every} * * * * *`, async () => {
                await this.processBatches(
                    qname,
                    handler as (
                        message: BatchMessage,
                    ) => Promise<{ success: boolean }>,
                    silent,
                );
            });

            this.batchJobs.set(qname, job);
        } else {
            const controller = new AbortController();
            this.workers.set(qname, controller);

            const workers = Array.from({ length: concurrency }, (_, i) =>
                this.runWorker(
                    qname,
                    handler as (
                        message: Message,
                    ) => Promise<{ success: boolean }>,
                    controller.signal,
                    i,
                    silent,
                ),
            );

            await Promise.all(workers);
        }
    }

    /**
     * Worker loop for processing regular queue messages
     */
    private async runWorker(
        qname: string,
        handler: (message: Message) => Promise<{ success: boolean }>,
        signal: AbortSignal,
        workerId: number,
        silent: boolean,
    ): Promise<void> {
        const queueKey = this.getKey(qname, "queue");

        while (!signal.aborted) {
            try {
                await this.processDelayedMessages(qname);

                const result = await this.redis.send("BRPOP", [queueKey, "1"]);

                if (result && Array.isArray(result) && result.length >= 2) {
                    const id = result[1] as string;
                    const message = await this.fetchMessage(qname, id);

                    if (message) {
                        try {
                            const { success } = await handler(message);

                            if (success) {
                                await this.deleteMessage(qname, message.id);
                            } else {
                                await this.retryMessage(qname, message.id);
                            }
                        } catch (error) {
                            if (!silent) {
                                console.error(
                                    `Worker ${workerId} handler error:`,
                                    (error as Error).message,
                                );
                            }
                            await this.retryMessage(qname, message.id);
                        }
                    }
                } else {
                    const delayedKey = this.getKey(qname, "delayed");
                    const nextDelay = await this.redis.send("ZRANGE", [
                        delayedKey,
                        "0",
                        "0",
                        "WITHSCORES",
                    ]);

                    let waitTime = 1000;

                    if (
                        nextDelay &&
                        Array.isArray(nextDelay) &&
                        nextDelay.length >= 2
                    ) {
                        const deliverAt = Number(nextDelay[1]);
                        waitTime = Math.min(
                            waitTime,
                            Math.max(0, deliverAt - Date.now()),
                        );
                    }

                    if (waitTime < 1000) {
                        await Bun.sleep(waitTime);
                    }
                }
            } catch (error) {
                if (!signal.aborted) {
                    if (!silent) {
                        console.error(
                            `Worker ${workerId} error:`,
                            (error as Error).message,
                        );
                    }
                    await Bun.sleep(1000);
                }
            }
        }
    }

    /**
     * Stops a worker for the specified queue
     */
    stopWorker(qname: string): void {
        const controller = this.workers.get(qname);
        if (controller) {
            controller.abort();
            this.workers.delete(qname);
        }

        const job = this.batchJobs.get(qname);
        if (job) {
            job.stop();
            this.batchJobs.delete(qname);
        }
    }

    /**
     * Closes all workers and the Redis connection
     */
    async close(): Promise<void> {
        for (const qname of this.workers.keys()) {
            this.stopWorker(qname);
        }
        for (const qname of this.batchJobs.keys()) {
            this.stopWorker(qname);
        }
        this.redis.close();
    }
}

export default RedisQueue;
