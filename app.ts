import { RedisClient as BunRedisClient } from "bun";
import { Cron } from "croner";

/**
 * Options for initializing the RedisQ client.
 * You can either provide a Redis client instance or connection details.
 */
export type QueueOptions =
    | {
          /** A Bun Redis client instance. */
          redis: BunRedisClient;
      }
    | {
          /** Redis server host. */
          host: string;
          /** Redis server port. */
          port: string;
          /** Redis server user. */
          user?: string;
          /** Redis server password. */
          password?: string;
          /** Namespace for all Redis keys. */
          namespace?: string;
          /** Whether to use TLS for the connection. */
          tls?: boolean;
      };

/**
 * Options for creating a new queue.
 * The options are different for regular and batch queues.
 */
export type CreateQueueOptions<QueueName extends string = string> =
    QueueName extends `${string}:batch`
        ? {
              /**
               * The name of the queue. Must end with `:batch` for batch queues.
               * @example "my-queue:batch"
               */
              qname: QueueName;
              /** Maximum message size in bytes.
               * @default 65536
               */
              maxsize?: number;
              /**
               * Maximum number of retries for a message.
               * `0` = no retry, `-1` = unlimited, `n` = max retries.
               * @default 0
               */
              maxRetries?: number;
              /**
               * Maximum backoff time in seconds for exponential backoff.
               * @default 30
               */
              maxBackoffSeconds?: number;
              /**
               * Batch processing interval in seconds.
               * @default 60
               */
              every?: number;
          }
        : {
              /**
               * The name of the queue.
               * @example "my-queue"
               */
              qname: QueueName;
              /** Maximum message size in bytes.
               * @default 65536
               */
              maxsize?: number;
              /**
               * Maximum number of retries for a message.
               * `0` = no retry, `-1` = unlimited, `n` = max retries.
               * @default 0
               */
              maxRetries?: number;
              /**
               * Maximum backoff time in seconds for exponential backoff.
               * @default 30
               */
              maxBackoffSeconds?: number;
          };

/**
 * Attributes of a queue.
 */
export interface QueueAttributes {
    /** Maximum message size in bytes. */
    maxsize: number;
    /** Timestamp when the queue was created. */
    created: number;
    /** Number of pending messages in the queue. */
    msgs: number;
    /** Whether the queue is a batch queue. */
    isBatch: boolean;
    /** Maximum number of retries for a message. */
    maxRetries: number;
    /** Maximum backoff time in seconds for exponential backoff. */
    maxBackoffSeconds: number;
    /** Batch processing interval in seconds (for batch queues only). */
    every?: number;
}

/**
 * Options for sending a message to a regular queue.
 */
export interface SendMessageOptions {
    /** The name of the queue. */
    qname: string;
    /** The message content. */
    message: string;
    /** Optional delay in milliseconds before the message is processed. */
    delay?: number;
}

/**
 * Options for sending a message to a batch queue.
 */
export interface SendBatchMessageOptions {
    /** The name of the batch queue. */
    qname: string;
    /**
     * The ID of the batch. Messages with the same `batchId` are processed together.
     */
    batchId: string;
    /** The message content. */
    message: string;
}

/**
 * A message received from a regular queue.
 */
export interface Message {
    /** The unique ID of the message. */
    id: string;
    /** The message content. */
    message: string;
    /** Timestamp when the message was sent. */
    sent: number;
    /** The current attempt number (1-based). */
    attempt: number;
}

/**
 * A batch of messages received from a batch queue.
 */
export interface BatchMessage {
    /** The ID of the batch. */
    batchId: string;
    /** Array of messages in the batch. */
    messages: Omit<Message, "attempt">[];
    /** Timestamp when the batch was created. */
    sent: number;
    /** The current attempt number for the batch (1-based). */
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

/**
 * A lightweight, type-safe Redis-based message queue for Bun.
 */
export class RedisQ {
    private redis: BunRedisClient;
    private redisUrl: string;
    private ns: string;
    private workers: Map<string, AbortController> = new Map();
    private batchJobs: Map<string, Cron> = new Map();
    private isClosing = false;

    /**
     * Initializes the RedisQ client.
     * @param options - Options for initializing the client.
     */
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

    private getKey(qname: string, suffix = ""): string {
        return suffix ? `${this.ns}:${qname}:${suffix}` : `${this.ns}:${qname}`;
    }

    private isBatchQueue(qname: string): boolean {
        return qname.endsWith(":batch");
    }

    /**
     * Creates a new queue with the specified options.
     * @param options - Options for creating the queue.
     * @returns `true` if the queue was created, `false` if it already exists.
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

        if (await this.redis.exists(key)) {
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
     * Lists all queue names.
     * @returns An array of queue names.
     */
    async listQueues(): Promise<string[]> {
        const pattern = `${this.ns}:*`;
        const keys = await this.redis.keys(pattern);

        return keys
            .map((k) => k.replace(`${this.ns}:`, ""))
            .filter((k) => !k.includes(":") || k.endsWith(":batch"));
    }

    /**
     * Gets the attributes of a queue.
     * @param qname - The name of the queue.
     * @returns The queue attributes, or `null` if the queue does not exist.
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
     * Deletes a queue and all its data.
     * @param qname - The name of the queue to delete.
     * @returns `true` if the queue was deleted, `false` if it did not exist.
     */
    async deleteQueue(qname: string): Promise<boolean> {
        const pattern = `${this.ns}:${qname}*`;
        let cursor = "0";
        let keysFound = false;

        do {
            const [nextCursor, keys] = await this.redis.scan(
                cursor,
                "MATCH",
                pattern,
                "COUNT",
                500,
            );
            cursor = nextCursor as string;
            if (keys.length > 0) {
                keysFound = true;
                await this.redis.del(...keys);
            }
        } while (cursor !== "0");

        return keysFound;
    }

    /**
     * Sends a message to a regular queue.
     * @param options - Options for sending the message.
     * @returns The unique ID of the message.
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

        // Use pipeline for atomic operations
        if (delay && delay > 0) {
            const delayedKey = this.getKey(qname, "delayed");
            const deliverAt = now + delay;
            await Promise.all([
                this.redis.hset(messagesKey, { [id]: encoded }),
                this.redis.send("ZADD", [delayedKey, deliverAt.toString(), id]),
            ]);
        } else {
            const queueKey = this.getKey(qname, "queue");
            await Promise.all([
                this.redis.hset(messagesKey, { [id]: encoded }),
                this.redis.send("LPUSH", [queueKey, id]),
            ]);
        }

        return id;
    }

    /**
     * Sends a message to a batch queue.
     * @param options - Options for sending the batch message.
     * @returns The unique ID of the message.
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
        const batchKey = this.getKey(qname, `batch:${batchId}`);
        const batchMetaKey = this.getKey(qname, "batch-meta");

        const [_, __, batchExists] = await Promise.all([
            this.redis.hset(messagesKey, { [id]: encoded }),
            this.redis.sadd(batchKey, id),
            this.redis.hexists(batchMetaKey, batchId),
        ]);

        if (!batchExists) {
            const batchMeta: StoredBatchMeta = {
                attempt: 1,
                batchId,
                sent: now,
            };
            const encodedMeta = this.encodeBatchMeta(batchMeta);
            const pendingBatchesKey = this.getKey(qname, "pending-batches");

            await Promise.all([
                this.redis.hset(batchMetaKey, { [batchId]: encodedMeta }),
                this.redis.sadd(pendingBatchesKey, batchId),
            ]);
        }

        return id;
    }

    private encodeMessage(msg: StoredMessage): string {
        return JSON.stringify({
            attempt: msg.attempt,
            id: msg.id,
            message: msg.message,
            sent: msg.sent,
        });
    }

    private decodeMessage(encoded: string): StoredMessage {
        const parsed = JSON.parse(encoded);
        return {
            attempt: parsed.attempt,
            id: parsed.id,
            message: parsed.message,
            sent: parsed.sent,
        };
    }

    private encodeBatchMeta(meta: StoredBatchMeta): string {
        return JSON.stringify(meta);
    }

    private decodeBatchMeta(encoded: string): StoredBatchMeta {
        return JSON.parse(encoded);
    }

    private async fetchMessages(
        qname: string,
        ids: string[],
    ): Promise<Message[]> {
        if (ids.length === 0) return [];

        const messagesKey = this.getKey(qname, "messages");
        const encoded = await this.redis.hmget(messagesKey, ...ids);

        const messages: Message[] = [];
        for (const encodedMsg of encoded) {
            if (encodedMsg) {
                const stored = this.decodeMessage(encodedMsg);
                messages.push({
                    attempt: stored.attempt,
                    id: stored.id,
                    message: stored.message,
                    sent: stored.sent,
                });
            }
        }

        return messages;
    }

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
        const messages = await this.fetchMessages(
            qname,
            messageIds as string[],
        );

        return {
            attempt: meta.attempt,
            batchId: meta.batchId,
            messages: messages.map(({ attempt, ...rest }) => rest),
            sent: meta.sent,
        };
    }

    /**
     * Manually deletes a message from the queue.
     * @param qname - The name of the queue.
     * @param id - The ID of the message to delete.
     * @returns `true` if the message was deleted, `false` otherwise.
     */
    async deleteMessage(qname: string, id: string): Promise<boolean> {
        const messagesKey = this.getKey(qname, "messages");
        const deleted = await this.redis.hdel(messagesKey, id);
        return deleted > 0;
    }

    private async deleteBatch(qname: string, batchId: string): Promise<void> {
        const batchKey = this.getKey(qname, `batch:${batchId}`);
        const batchMetaKey = this.getKey(qname, "batch-meta");

        const messageIds = (await this.redis.send("SMEMBERS", [batchKey])) as [
            string,
        ];

        if (messageIds && messageIds.length > 0) {
            const messagesKey = this.getKey(qname, "messages");
            await this.redis.hdel(messagesKey, ...messageIds);
        }

        await Promise.all([
            this.redis.del(batchKey),
            this.redis.hdel(batchMetaKey, batchId),
        ]);
    }

    private async retryMessage(qname: string, id: string): Promise<void> {
        if (this.isClosing) return;

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

        const delayedKey = this.getKey(qname, "delayed");
        const deliverAt = Date.now() + backoffMs;

        await Promise.all([
            this.redis.hset(messagesKey, { [id]: updatedEncoded }),
            this.redis.send("ZADD", [delayedKey, deliverAt.toString(), id]),
        ]);
    }

    private async retryBatch(qname: string, batchId: string): Promise<void> {
        if (this.isClosing) return;

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
        const pendingBatchesKey = this.getKey(qname, "pending-batches");

        await Promise.all([
            this.redis.hset(batchMetaKey, { [batchId]: updatedEncoded }),
            this.redis.sadd(pendingBatchesKey, batchId),
        ]);
    }

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
            const pipeline = [];
            for (const id of ready as string[]) {
                pipeline.push(
                    this.redis.send("LPUSH", [queueKey, id]),
                    this.redis.send("ZREM", [delayedKey, id]),
                );
            }
            await Promise.all(pipeline);
        }
    }

    private async processBatches(
        qname: string,
        handler: (batch: BatchMessage) => Promise<{ success: boolean }>,
        silent: boolean,
    ): Promise<void> {
        if (this.isClosing) return;

        const pendingBatchesKey = this.getKey(qname, "pending-batches");
        const batchIds = (await this.redis.send("SMEMBERS", [
            pendingBatchesKey,
        ])) as string[] | null;

        if (!batchIds || batchIds.length === 0) {
            return;
        }

        const batchPromises = batchIds.map(async (batchId) => {
            if (this.isClosing) return;

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
     * Starts a worker to process messages from a queue.
     * @param qname - The name of the queue.
     * @param handler - The function to process messages.
     * @param options - Options for the worker.
     * @returns `true` if the worker spawned, `false` if it already exists.
     */
    async startWorker<QueueName extends `${string}:batch` | (string & {})>(
        qname: QueueName,
        handler: (
            received: QueueName extends `${string}:batch`
                ? BatchMessage
                : Message,
        ) => Promise<{ success: boolean }>,
        options: { concurrency?: number; silent?: boolean } = {},
    ): Promise<boolean> {
        const { concurrency = 1, silent = false } = options;

        if (this.workers.has(qname)) {
            return false;
        }

        const isBatch = this.isBatchQueue(qname);

        if (isBatch) {
            const key = this.getKey(qname);
            const attrs = await this.redis.hgetall(key);

            if (!attrs || Object.keys(attrs).length === 0) {
                throw new Error("Queue not found");
            }

            const every = Number(attrs.every || 60);

            const job = new Cron(
                "* * * * * *",
                { interval: every, startAt: new Date() },
                async () => {
                    if (this.isClosing) return;
                    try {
                        await this.processBatches(
                            qname,
                            handler as (
                                received: BatchMessage,
                            ) => Promise<{ success: boolean }>,
                            silent,
                        );
                    } catch (error) {
                        if (!silent && !this.isClosing) {
                            console.error(
                                "Batch processing error:",
                                (error as Error).message,
                            );
                        }
                    }
                },
            );

            this.batchJobs.set(qname, job);
        } else {
            const controller = new AbortController();
            this.workers.set(qname, controller);

            const workers = Array.from({ length: concurrency }, (_, i) =>
                this.runWorker(
                    qname,
                    handler as (
                        received: Message,
                    ) => Promise<{ success: boolean }>,
                    controller.signal,
                    i,
                    silent,
                ),
            );

            await Promise.all(workers);
        }
        return true;
    }

    private async runWorker(
        qname: string,
        handler: (received: Message) => Promise<{ success: boolean }>,
        signal: AbortSignal,
        workerId: number,
        silent: boolean,
    ): Promise<void> {
        const queueKey = this.getKey(qname, "queue");
        const BATCH_SIZE = 50;

        while (!signal.aborted) {
            try {
                await this.processDelayedMessages(qname);

                const ids = await this.redis.rpop(queueKey, BATCH_SIZE);

                if (ids && ids.length > 0) {
                    const messages = await this.fetchMessages(
                        qname,
                        ids as string[],
                    );

                    const processingPromises = messages.map((received) =>
                        handler(received)
                            .then(async ({ success }) => {
                                if (success) {
                                    await this.deleteMessage(
                                        qname,
                                        received.id,
                                    );
                                } else {
                                    await this.retryMessage(qname, received.id);
                                }
                            })
                            .catch(async (error) => {
                                if (!silent) {
                                    console.error(
                                        `Worker ${workerId} handler error:`,
                                        (error as Error).message,
                                    );
                                }
                                await this.retryMessage(qname, received.id);
                            }),
                    );
                    await Promise.all(processingPromises);
                } else {
                    await Bun.sleep(50);
                }
            } catch (error) {
                if (!signal.aborted) {
                    if (!silent) {
                        console.error(
                            `Worker ${workerId} error:`,
                            (error as Error).message,
                        );
                    }
                    await Bun.sleep(100);
                }
            }
        }
    }

    /**
     * Stops a specific worker.
     * @param qname - The name of the queue for which to stop the worker.
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
     * Closes all workers and the Redis connection gracefully.
     */
    async close(): Promise<void> {
        this.isClosing = true;

        for (const qname of this.workers.keys()) {
            const controller = this.workers.get(qname);
            if (controller) {
                controller.abort();
            }
        }
        this.workers.clear();

        for (const qname of this.batchJobs.keys()) {
            const job = this.batchJobs.get(qname);
            if (job) {
                job.stop();
            }
        }
        this.batchJobs.clear();

        await Bun.sleep(100);

        this.redis.close();
    }
}
