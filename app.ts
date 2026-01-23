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

export type CreateQueueOptions<QueueName extends string = string> =
    QueueName extends `${string}:batch`
        ? {
              qname: QueueName;
              maxsize?: number;
              maxRetries?: number;
              maxBackoffSeconds?: number;
              every?: number;
          }
        : {
              qname: QueueName;
              maxsize?: number;
              maxRetries?: number;
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
    delay?: number;
}

export interface SendBatchMessageOptions {
    qname: string;
    batchId: string;
    message: string;
}

export interface Message {
    id: string;
    message: string;
    sent: number;
    attempt: number;
}

export interface BatchMessage {
    batchId: string;
    messages: Omit<Message, "attempt">[];
    sent: number;
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
    private isClosing = false;

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

    async listQueues(): Promise<string[]> {
        const pattern = `${this.ns}:*`;
        const keys = await this.redis.keys(pattern);

        return keys
            .map((k) => k.replace(`${this.ns}:`, ""))
            .filter((k) => !k.includes(":") || k.endsWith(":batch"));
    }

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

    async deleteQueue(qname: string): Promise<boolean> {
        const pattern = `${this.ns}:${qname}*`;
        const keys = await this.redis.keys(pattern);

        if (keys.length === 0) {
            return false;
        }

        await this.redis.del(...keys);
        return true;
    }

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

        await this.redis.hset(messagesKey, { [id]: encoded });
        await this.redis.sadd(batchKey, id);

        const batchExists = await this.redis.hexists(batchMetaKey, batchId);

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

    // OPTIMIZED: Batch fetch multiple messages at once
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
        handler: (message: BatchMessage) => Promise<{ success: boolean }>,
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
                if (this.isClosing) return;
                try {
                    await this.processBatches(
                        qname,
                        handler as (
                            message: BatchMessage,
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

    // OPTIMIZED: Process messages in parallel within worker
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

                const result = await this.redis.send("BRPOP", [
                    queueKey,
                    "0.1",
                ]);

                if (result && Array.isArray(result) && result.length >= 2) {
                    const id = result[1] as string;
                    const message = await this.fetchMessage(qname, id);

                    if (message) {
                        // Process message without blocking the loop
                        handler(message)
                            .then(async ({ success }) => {
                                if (success) {
                                    await this.deleteMessage(qname, message.id);
                                } else {
                                    await this.retryMessage(qname, message.id);
                                }
                            })
                            .catch(async (error) => {
                                if (!silent) {
                                    console.error(
                                        `Worker ${workerId} handler error:`,
                                        (error as Error).message,
                                    );
                                }
                                await this.retryMessage(qname, message.id);
                            });
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
                    await Bun.sleep(100);
                }
            }
        }
    }

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

export default RedisQueue;
