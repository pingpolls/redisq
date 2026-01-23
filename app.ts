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
    /**
     * Use suffix `:batch` to treat this queue as batched
     */
    qname: string;
    maxsize?: number; // max message size in bytes
}

export interface QueueAttributes {
    maxsize: number;
    created: number;
    msgs: number;
    isBatch: boolean;
}

export interface SendMessageOptions {
    qname: string;
    message: string;
    delay?: number; // delay in milliseconds
    maxRetries?: number; // undefined = no retry (1 attempt), -1 = unlimited, n = max retries
    maxBackoffSeconds?: number; // default 30 seconds
}

export interface SendBatchMessageOptions {
    qname: string;
    batchId: string; // user-defined batch ID
    message: string;
    delay?: number;
    maxRetries?: number;
    maxBackoffSeconds?: number;
}

export interface Message {
    id: string;
    message: string;
    sent: number; // sent timestamp
    attempt: number; // current attempt number (1-based)
    maxRetries: number;
    maxBackoffSeconds: number;
}

export interface BatchMessage {
    batchId: string;
    messages: Message[];
    sent: number;
}

interface StoredMessage {
    id: string;
    message: string;
    sent: number;
    attempt: number;
    maxRetries: number;
    maxBackoffSeconds: number;
}

interface StoredBatchMeta {
    batchId: string;
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

    private isBatchQueue(qname: string): boolean {
        return qname.endsWith(":batch");
    }

    async createQueue(options: CreateQueueOptions): Promise<boolean> {
        const { qname, maxsize = 65536 } = options;

        if (!/^[a-zA-Z0-9_:-]{1,160}$/.test(qname)) {
            throw new Error("Invalid queue name");
        }

        const key = this.getKey(qname);
        const exists = await this.redis.exists(key);

        if (exists) {
            return false;
        }

        const isBatch = this.isBatchQueue(qname);

        const attrs = {
            created: Date.now(),
            maxsize,
            type: isBatch ? "batch" : "single",
        };

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

        return {
            created: Number(attrs.created),
            isBatch,
            maxsize: Number(attrs.maxsize),
            msgs: (msgCount as number) + (delayedCount as number),
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
            maxBackoffSeconds,
            maxRetries: maxRetries !== undefined ? maxRetries : 0,
            message,
            sent: now,
        };

        // Store message in hash field under messages hash
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

    async sendBatchMessage(options: SendBatchMessageOptions): Promise<string> {
        const {
            qname,
            batchId,
            message,
            delay,
            maxRetries,
            maxBackoffSeconds = 30,
        } = options;

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
            maxBackoffSeconds,
            maxRetries: maxRetries !== undefined ? maxRetries : 0,
            message,
            sent: now,
        };

        // Store message in hash
        const messagesKey = this.getKey(qname, "messages");
        const encoded = this.encodeMessage(storedMsg);
        await this.redis.hset(messagesKey, { [id]: encoded });

        // Add to batch set
        const batchKey = this.getKey(qname, `batch:${batchId}`);
        await this.redis.sadd(batchKey, id);

        // Create batch metadata if doesn't exist
        const batchMetaKey = this.getKey(qname, "batch-meta");
        const batchExists = await this.redis.hexists(batchMetaKey, batchId);

        if (!batchExists) {
            const batchMeta: StoredBatchMeta = {
                batchId,
                sent: now,
            };
            const encodedMeta = this.encodeBatchMeta(batchMeta);
            await this.redis.hset(batchMetaKey, { [batchId]: encodedMeta });

            if (delay && delay > 0) {
                const delayedKey = this.getKey(qname, "delayed");
                const deliverAt = now + delay;
                await this.redis.send("ZADD", [
                    delayedKey,
                    deliverAt.toString(),
                    batchId,
                ]);
            } else {
                const queueKey = this.getKey(qname, "queue");
                await this.redis.send("LPUSH", [queueKey, batchId]);
            }
        }

        return id;
    }

    private encodeMessage(msg: StoredMessage): string {
        return JSON.stringify({
            attempt: msg.attempt,
            id: msg.id,
            maxBackoffSeconds: msg.maxBackoffSeconds,
            maxRetries: msg.maxRetries,
            message: msg.message,
            sent: msg.sent,
        });
    }

    private decodeMessage(encoded: string): StoredMessage {
        const parsed = JSON.parse(encoded);
        return {
            attempt: parsed.attempt,
            id: parsed.id,
            maxBackoffSeconds: parsed.maxBackoffSeconds,
            maxRetries: parsed.maxRetries,
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
            maxBackoffSeconds: stored.maxBackoffSeconds,
            maxRetries: stored.maxRetries,
            message: stored.message,
            sent: stored.sent,
        };
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
        const messages: Message[] = [];

        for (const id of messageIds as string[]) {
            const msg = await this.fetchMessage(qname, id);
            if (msg) {
                messages.push(msg);
            }
        }

        return {
            batchId: meta.batchId,
            messages,
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

    private async retryMessage(qname: string, id: string): Promise<void> {
        const messagesKey = this.getKey(qname, "messages");
        const encoded = await this.redis.hget(messagesKey, id);

        if (!encoded) {
            return;
        }

        const msg = this.decodeMessage(encoded);
        const { attempt, maxRetries, maxBackoffSeconds } = msg;

        // Check if we should retry
        if (maxRetries === 0 || (maxRetries !== -1 && attempt > maxRetries)) {
            // No more retries, delete message
            await this.redis.hdel(messagesKey, id);
            return;
        }

        // Calculate exponential backoff
        const randomMs = Math.floor(Math.random() * 1000);
        const backoffSeconds = Math.min(
            2 ** attempt + randomMs / 1000,
            maxBackoffSeconds,
        );
        const backoffMs = backoffSeconds * 1000;

        // Update message with incremented attempt
        msg.attempt += 1;
        const updatedEncoded = this.encodeMessage(msg);
        await this.redis.hset(messagesKey, { [id]: updatedEncoded });

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
        }
    }

    /**
     * For regular queues, the handler receives `Message`.
     *
     * However batch queues (ending with `:batch`), the handler receives `BatchMessage`.
     *
     * Retries are handled automatically by providing
     * certain configuration on `sendMessage` or `sendBatchMessage` method.
     *
     * Batched retries will
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

        const controller = new AbortController();
        this.workers.set(qname, controller);

        const isBatch = this.isBatchQueue(qname);

        const workers = Array.from({ length: concurrency }, (_, i) =>
            this.runWorker(
                qname,
                handler as (
                    message: Message | BatchMessage,
                ) => Promise<{ success: boolean }>,
                controller.signal,
                i,
                silent,
                isBatch,
            ),
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
        silent: boolean,
        isBatch: boolean,
    ): Promise<void> {
        const queueKey = this.getKey(qname, "queue");
        const delayedKey = this.getKey(qname, "delayed");

        while (!signal.aborted) {
            try {
                await this.processDelayedMessages(qname);

                const result = await this.redis.send("BRPOP", [queueKey, "1"]);

                if (result && Array.isArray(result) && result.length >= 2) {
                    const id = result[1] as string;
                    let message: Message | BatchMessage | null;

                    if (isBatch) {
                        message = await this.fetchBatchMessage(qname, id);
                    } else {
                        message = await this.fetchMessage(qname, id);
                    }

                    if (message) {
                        try {
                            const { success } = await handler(message);

                            if (success) {
                                // Delete message on success
                                if (isBatch && "batchId" in message) {
                                    await this.deleteBatch(
                                        qname,
                                        message.batchId,
                                    );
                                } else if ("id" in message) {
                                    await this.deleteMessage(qname, message.id);
                                }
                            } else {
                                // Retry on failure
                                if (isBatch && "batchId" in message) {
                                    for (const msg of message.messages) {
                                        await this.retryMessage(qname, msg.id);
                                    }
                                } else if ("id" in message) {
                                    await this.retryMessage(qname, message.id);
                                }
                            }
                        } catch (error) {
                            if (!silent) {
                                console.error(
                                    `Worker ${workerId} handler error:`,
                                    (error as Error).message,
                                );
                            }
                            // Retry on exception
                            if (isBatch && message && "batchId" in message) {
                                for (const msg of message.messages) {
                                    await this.retryMessage(qname, msg.id);
                                }
                            } else if (message && "id" in message) {
                                await this.retryMessage(qname, message.id);
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
