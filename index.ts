import { RedisClient as BunRedisClient } from "bun";

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

export interface CreateQueueOptions {
    qname: string;
    maxsize?: number; // max message size in bytes
}

export interface QueueAttributes {
    maxsize: number;
    created: number;
    msgs: number;
}

export interface SendMessageOptions {
    qname: string;
    message: Uint8Array;
    delay?: number; // delay in milliseconds
    maxRetries?: number; // undefined = no retry (1 attempt), -1 = unlimited, n = max retries
    maxBackoffSeconds?: number; // default 30 seconds
}

export interface SendBatchMessageOptions {
    qname: string;
    batchId: string; // user-defined batch ID
    message: Uint8Array;
    delay?: number;
    maxRetries?: number;
    maxBackoffSeconds?: number;
}

export interface Message {
    id: string;
    message: Uint8Array;
    sent: number; // sent timestamp
    attempt: number; // current attempt number (1-based)
}

export interface BatchMessage {
    batchId: string;
    messages: Message[];
    sent: number;
}

export class RedisQueue {
    private redis: BunRedisClient;
    private redisUrl: string;
    private ns: string;
    private workers: Map<string, AbortController> = new Map();

    constructor(options: QueueOptions) {
        if ("redis" in options) {
            this.redis = options.redis;
            this.redisUrl = "redis://localhost:6379"; // fallback for duplicate
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

    async createQueue(options: CreateQueueOptions): Promise<boolean> {
        const { qname, maxsize = 65536 } = options;

        if (!/^[a-zA-Z0-9_-]{1,160}$/.test(qname)) {
            throw new Error("Invalid queue name");
        }

        const key = this.getKey(qname);
        const exists = await this.redis.exists(key);

        if (exists) {
            return false;
        }

        const attrs = {
            created: Date.now(),
            maxsize,
        };

        await this.redis.hset(key, attrs);
        return true;
    }

    async listQueues(): Promise<string[]> {
        const pattern = `${this.ns}:*`;
        const keys = await this.redis.keys(pattern);

        return keys
            .map((k) => k.replace(`${this.ns}:`, ""))
            .filter((k) => !k.includes(":"));
    }

    async getQueue(qname: string): Promise<QueueAttributes | null> {
        const key = this.getKey(qname);
        const attrs = await this.redis.hgetall(key);

        if (!attrs || Object.keys(attrs).length === 0) {
            return null;
        }

        const queueKey = this.getKey(qname, "queue");
        const delayedKey = this.getKey(qname, "delayed");
        const batchDelayedKey = this.getKey(qname, "batch-delayed");

        const [msgs, delayedMsgs, batchDelayedMsgs] = await Promise.all([
            this.redis.send("LLEN", [queueKey]),
            this.redis.send("ZCARD", [delayedKey]),
            this.redis.send("ZCARD", [batchDelayedKey]),
        ]);

        // Count batch queue messages
        const batchPattern = `${this.ns}:${qname}:batch:*:meta`;
        const batchKeys = await this.redis.keys(batchPattern);
        const batchCount = batchKeys.length;

        return {
            created: Number(attrs.created),
            maxsize: Number(attrs.maxsize),
            msgs:
                (msgs as number) +
                (delayedMsgs as number) +
                (batchDelayedMsgs as number) +
                batchCount,
        };
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
        const {
            qname,
            message,
            delay,
            maxRetries,
            maxBackoffSeconds = 30,
        } = options;

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

        const msgKey = this.getKey(qname, `msgs:${id}`);
        await this.redis.set(msgKey, Buffer.from(message));
        await this.redis.hset(`${msgKey}:meta`, {
            attempt: 1,
            id,
            maxBackoffSeconds,
            maxRetries: maxRetries !== undefined ? maxRetries : 0,
            sent: now,
        });

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
            await this.redis.publish(`${this.ns}:rt:${qname}`, "1");
        }

        return id;
    }

    async sendBatchMessage(options: SendBatchMessageOptions): Promise<string> {
        const {
            qname,
            batchId,
            message,
            delay,
            maxRetries,
            maxBackoffSeconds = 30,
        } = options;

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

        const msgKey = this.getKey(qname, `msgs:${id}`);
        await this.redis.set(msgKey, Buffer.from(message));
        await this.redis.hset(`${msgKey}:meta`, {
            attempt: 1,
            id,
            maxBackoffSeconds,
            maxRetries: maxRetries !== undefined ? maxRetries : 0,
            sent: now,
        });

        const batchKey = this.getKey(qname, `batch:${batchId}`);
        await this.redis.sadd(batchKey, id);

        const batchMetaKey = this.getKey(qname, `batch:${batchId}:meta`);
        const batchExists = await this.redis.exists(batchMetaKey);

        if (!batchExists) {
            await this.redis.hset(batchMetaKey, {
                batchId,
                sent: now,
            });

            if (delay && delay > 0) {
                const delayedKey = this.getKey(qname, "batch-delayed");
                const deliverAt = now + delay;
                await this.redis.send("ZADD", [
                    delayedKey,
                    deliverAt.toString(),
                    batchId,
                ]);
            } else {
                // Add to batch list with batchId as identifier
                const batchListKey = this.getKey(qname, "batch-list");
                await this.redis.send("LPUSH", [batchListKey, batchId]);
                await this.redis.publish(`${this.ns}:rt:${qname}`, "batch");
            }
        }

        return id;
    }

    async popMessage(qname: string): Promise<Message | BatchMessage | null> {
        await this.processDelayedMessages(qname);
        await this.processDelayedBatches(qname);

        // Try batch list first (FIFO for batch IDs)
        const batchListKey = this.getKey(qname, "batch-list");
        const batchId = await this.redis.send("RPOP", [batchListKey]);

        if (batchId) {
            return await this.popBatchMessage(qname, batchId as string);
        }

        // Try regular queue
        const queueKey = this.getKey(qname, "queue");
        const id = await this.redis.send("RPOP", [queueKey]);

        if (!id) {
            return null;
        }

        return await this.getMessage(qname, id as string);
    }

    private async popBatchMessage(
        qname: string,
        batchId: string,
    ): Promise<BatchMessage | null> {
        const batchKey = this.getKey(qname, `batch:${batchId}`);
        const batchMetaKey = this.getKey(qname, `batch:${batchId}:meta`);

        const [messageIds, meta] = await Promise.all([
            this.redis.send("SMEMBERS", [batchKey]),
            this.redis.hgetall(batchMetaKey),
        ]);

        if (!messageIds || (messageIds as string[]).length === 0 || !meta) {
            await this.redis.del(batchKey, batchMetaKey);
            return null;
        }

        const messages: Message[] = [];
        for (const id of messageIds as string[]) {
            const msg = await this.getMessage(qname, id, false);
            if (msg) {
                messages.push(msg as Message);
            }
        }

        await this.redis.del(batchKey, batchMetaKey);

        return {
            batchId,
            messages,
            sent: Number(meta.sent),
        };
    }

    private async getMessage(
        qname: string,
        id: string,
        deleteAfter = true,
    ): Promise<Message | null> {
        const msgKey = this.getKey(qname, `msgs:${id}`);
        const metaKey = `${msgKey}:meta`;

        const [messageBuffer, meta] = await Promise.all([
            this.redis.getBuffer(msgKey),
            this.redis.hgetall(metaKey),
        ]);

        if (!messageBuffer || !meta) {
            return null;
        }

        if (deleteAfter) {
            await this.redis.del(msgKey, metaKey);
        }

        return {
            attempt: Number(meta.attempt),
            id: id,
            message: new Uint8Array(messageBuffer as Buffer),
            sent: Number(meta.sent),
        };
    }

    async receiveMessage(
        qname: string,
    ): Promise<Message | BatchMessage | null> {
        return await this.popMessage(qname);
    }

    async deleteMessage(qname: string, id: string): Promise<boolean> {
        const msgKey = this.getKey(qname, `msgs:${id}`);
        const metaKey = `${msgKey}:meta`;

        const deleted = await this.redis.del(msgKey, metaKey);
        return deleted > 0;
    }

    private async retryMessage(qname: string, id: string): Promise<void> {
        const msgKey = this.getKey(qname, `msgs:${id}`);
        const metaKey = `${msgKey}:meta`;

        const meta = await this.redis.hgetall(metaKey);
        if (!meta) {
            return;
        }

        const attempt = Number(meta.attempt);
        const maxRetries = Number(meta.maxRetries);
        const maxBackoffSeconds = Number(meta.maxBackoffSeconds);

        // Check if we should retry
        if (maxRetries === 0 || (maxRetries !== -1 && attempt > maxRetries)) {
            // No more retries, delete message
            await this.redis.del(msgKey, metaKey);
            return;
        }

        // Calculate exponential backoff: min(((2^attempt)+randomMilliseconds), maxBackoffSeconds)
        const randomMs = Math.floor(Math.random() * 1000);
        const backoffSeconds = Math.min(
            2 ** attempt + randomMs / 1000,
            maxBackoffSeconds,
        );
        const backoffMs = backoffSeconds * 1000;

        // Increment attempt
        await this.redis.send("HINCRBY", [metaKey, "attempt", "1"]);

        // Schedule retry
        const delayedKey = this.getKey(qname, "delayed");
        const deliverAt = Date.now() + backoffMs;
        await this.redis.send("ZADD", [delayedKey, deliverAt.toString(), id]);
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
            for (const id of ready as string[]) {
                await this.redis.send("LPUSH", [queueKey, id]);
                await this.redis.send("ZREM", [delayedKey, id]);
            }
            await this.redis.publish(
                `${this.ns}:rt:${qname}`,
                (ready as string[]).length.toString(),
            );
        }
    }

    private async processDelayedBatches(qname: string): Promise<void> {
        const delayedKey = this.getKey(qname, "batch-delayed");
        const batchListKey = this.getKey(qname, "batch-list");
        const now = Date.now();

        const ready = await this.redis.send("ZRANGEBYSCORE", [
            delayedKey,
            "-inf",
            now.toString(),
        ]);

        if (ready && (ready as string[]).length > 0) {
            for (const batchId of ready as string[]) {
                await this.redis.send("LPUSH", [batchListKey, batchId]);
                await this.redis.send("ZREM", [delayedKey, batchId]);
            }
            await this.redis.publish(`${this.ns}:rt:${qname}`, "batch");
        }
    }

    async startWorker(
        qname: string,
        handler: (
            message: Message | BatchMessage,
        ) => Promise<{ success: boolean }>,
        options: { concurrency?: number } = {},
    ): Promise<void> {
        const { concurrency = 1 } = options;

        if (this.workers.has(qname)) {
            throw new Error(`Worker already running for queue: ${qname}`);
        }

        const controller = new AbortController();
        this.workers.set(qname, controller);

        const workers = Array.from({ length: concurrency }, (_, i) =>
            this.runWorker(qname, handler, controller.signal, i),
        );

        await Promise.all(workers);
    }

    private async runWorker(
        qname: string,
        handler: (
            message: Message | BatchMessage,
        ) => Promise<{ success: boolean }>,
        signal: AbortSignal,
        workerId: number,
    ): Promise<void> {
        const queueKey = this.getKey(qname, "queue");
        const batchListKey = this.getKey(qname, "batch-list");
        const delayedKey = this.getKey(qname, "delayed");
        const batchDelayedKey = this.getKey(qname, "batch-delayed");

        // Create subscriber with duplicate connection
        const subscriber = new BunRedisClient(this.redisUrl);
        await subscriber.connect();
        await subscriber.subscribe(`${this.ns}:rt:${qname}`, () => {});

        while (!signal.aborted) {
            try {
                await this.processDelayedMessages(qname);
                await this.processDelayedBatches(qname);

                // Try batch list first, then regular queue
                let result = await this.redis.send("BRPOP", [
                    batchListKey,
                    "0.1",
                ]);
                let isBatch = true;

                if (!result) {
                    result = await this.redis.send("BRPOP", [queueKey, "0.1"]);
                    isBatch = false;
                }

                if (result && Array.isArray(result) && result.length >= 2) {
                    const id = result[1] as string;
                    let message: Message | BatchMessage | null;

                    if (isBatch) {
                        message = await this.popBatchMessage(qname, id);
                    } else {
                        message = await this.getMessage(qname, id, false);
                    }

                    if (message) {
                        try {
                            const { success } = await handler(message);

                            if (success) {
                                // Delete message on success
                                if ("batchId" in message) {
                                    // Already deleted in popBatchMessage
                                } else {
                                    await this.deleteMessage(qname, message.id);
                                }
                            } else {
                                // Retry on failure
                                if ("batchId" in message) {
                                    // Retry each message in batch
                                    for (const msg of message.messages) {
                                        await this.retryMessage(qname, msg.id);
                                    }
                                } else {
                                    await this.retryMessage(qname, message.id);
                                }
                            }
                        } catch (error) {
                            console.error(
                                `Worker ${workerId} handler error:`,
                                error,
                            );
                            // Retry on exception
                            if (message && "batchId" in message) {
                                for (const msg of message.messages) {
                                    await this.retryMessage(qname, msg.id);
                                }
                            } else if (message) {
                                await this.retryMessage(
                                    qname,
                                    (message as Message).id,
                                );
                            }
                        }
                    }
                } else {
                    // Check for delayed messages
                    const nextDelay = await this.redis.send("ZRANGE", [
                        delayedKey,
                        "0",
                        "0",
                        "WITHSCORES",
                    ]);
                    const nextBatchDelay = await this.redis.send("ZRANGE", [
                        batchDelayedKey,
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

                    if (
                        nextBatchDelay &&
                        Array.isArray(nextBatchDelay) &&
                        nextBatchDelay.length >= 2
                    ) {
                        const deliverAt = Number(nextBatchDelay[1]);
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
                    console.error(`Worker ${workerId} error:`, error);
                    await Bun.sleep(1000);
                }
            }
        }

        await subscriber.unsubscribe();
        subscriber.close();
    }

    stopWorker(qname: string): void {
        const controller = this.workers.get(qname);
        if (controller) {
            controller.abort();
            this.workers.delete(qname);
        }
    }

    async close(): Promise<void> {
        for (const qname of this.workers.keys()) {
            this.stopWorker(qname);
        }
        this.redis.close();
    }
}

export default RedisQueue;
