import { afterAll, describe, expect, test } from "bun:test";
import RedisQueue from "./app";

const redisConfig = {
    host: process.env.REDIS_HOST || "127.0.0.1",
    namespace: "redisq-testing",
    port: process.env.REDIS_PORT || "6379",
};

const workerConfig = {
    silent: true,
};

/**
 * Helper to wait for a specific duration
 */
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe("RedisQueue Tests", () => {
    test("1. Can send and receive single message", async () => {
        const queue = new RedisQueue(redisConfig);

        await queue.createQueue({ qname: "test-basic" });

        const message = "Hello, World!";
        let receivedCount = 0;

        const id = await queue.sendMessage({
            message,
            qname: "test-basic",
        });

        await new Promise<void>((resolve) => {
            queue.startWorker(
                "test-basic",
                async (received) => {
                    expect(received.message).toEqual(message);
                    expect(received.attempt).toBe(1);
                    expect(received.id).toEqual(id);
                    receivedCount++;
                    resolve();
                    return { success: true };
                },
                workerConfig,
            );
        });

        expect(receivedCount).toBe(1);

        await queue.close();
    });

    test("2. Can send and receive delayed message", async () => {
        const queue = new RedisQueue(redisConfig);

        await queue.createQueue({ qname: "test-delayed" });

        const message = "Delayed message";
        const delayMs = 2000;
        let receivedCount = 0;

        const sentAt = Date.now();
        const id = await queue.sendMessage({
            message,
            qname: "test-delayed",
            delay: delayMs,
        });

        await new Promise<void>((resolve) => {
            queue.startWorker(
                "test-delayed",
                async (received) => {
                    const receivedAt = Date.now();
                    const actualDelay = receivedAt - sentAt;

                    expect(received.message).toEqual(message);
                    expect(received.id).toEqual(id);
                    expect(received.attempt).toBe(1);

                    /**
                     * Check that message was delayed by at least the specified delay
                     * Allow 100ms margin for processing time
                     */
                    expect(actualDelay).toBeGreaterThanOrEqual(delayMs - 100);

                    receivedCount++;
                    resolve();
                    return { success: true };
                },
                workerConfig,
            );
        });

        expect(receivedCount).toBe(1);

        await queue.close();
    });

    test("3. Can retry send and receive single message", async () => {
        const queue = new RedisQueue(redisConfig);

        await queue.createQueue({
            qname: "test-retry",
            maxRetries: 3,
            maxBackoffSeconds: 1,
        });

        const message = "Retry me!";
        let attemptCount = 0;

        await queue.sendMessage({
            message,
            qname: "test-retry",
        });

        await new Promise<void>((resolve) => {
            queue.startWorker(
                "test-retry",
                async (received) => {
                    attemptCount++;
                    expect(received.attempt).toBe(attemptCount);
                    expect(received.message).toBe(message);

                    /**
                     * Fail first 2 attempts, succeed on 3rd
                     */
                    if (received.attempt < 3) {
                        return { success: false };
                    }

                    resolve();
                    return { success: true };
                },
                workerConfig,
            );
        });

        expect(attemptCount).toBe(3);

        await queue.close();
    });

    test("4. Can retry send and receive delayed message", async () => {
        const queue = new RedisQueue(redisConfig);

        await queue.createQueue({
            qname: "test-delayed-retry",
            maxRetries: 2,
            maxBackoffSeconds: 1,
        });

        const message = "Delayed retry message";
        const delayMs = 1000;
        let attemptCount = 0;

        const sentAt = Date.now();
        await queue.sendMessage({
            message,
            qname: "test-delayed-retry",
            delay: delayMs,
        });

        await new Promise<void>((resolve) => {
            queue.startWorker(
                "test-delayed-retry",
                async (received) => {
                    attemptCount++;
                    expect(received.attempt).toBe(attemptCount);
                    expect(received.message).toBe(message);

                    /**
                     * First attempt should be delayed by initial delay
                     */
                    if (attemptCount === 1) {
                        const actualDelay = Date.now() - sentAt;
                        expect(actualDelay).toBeGreaterThanOrEqual(
                            delayMs - 100,
                        );
                    }

                    /**
                     * Fail first attempt, succeed on 2nd
                     */
                    if (received.attempt < 2) {
                        return { success: false };
                    }

                    resolve();
                    return { success: true };
                },
                workerConfig,
            );
        });

        expect(attemptCount).toBe(2);

        await queue.close();
    });

    test("5. Can send and receive batched messages (multiple batches in same period)", async () => {
        const queue = new RedisQueue(redisConfig);

        /**
         * Create batch queue with 3-second interval
         * This simulates a spreadsheet-queue:batch with shorter period for testing
         */
        await queue.createQueue({
            qname: "spreadsheet-queue:batch",
            every: 3,
            maxRetries: 0,
        });

        /**
         * Simulate the scenario:
         * - 10 messages sent at different timestamps
         * - 6 messages with batchId "batch-001"
         * - 4 messages with batchId "batch-002"
         * - All should be processed in the SAME 3-second period
         */
        const batch001Messages: string[] = [];
        const batch002Messages: string[] = [];

        const processedBatches = new Set<string>();
        let batch001Processed = false;
        let batch002Processed = false;

        /**
         * Start worker BEFORE sending messages
         * This ensures the cron job is running when messages arrive
         */
        const workerPromise = new Promise<void>((resolve) => {
            queue.startWorker(
                "spreadsheet-queue:batch",
                async (received) => {
                    processedBatches.add(received.batchId);

                    if (received.batchId === "batch-001") {
                        expect(received.messages.length).toBe(6);
                        expect(received.attempt).toBe(1);

                        /**
                         * Verify all messages are present
                         */
                        received.messages.forEach((msg, idx) => {
                            expect(msg.message).toBe(
                                batch001Messages.at(idx) ?? "",
                            );
                        });

                        batch001Processed = true;
                    } else if (received.batchId === "batch-002") {
                        expect(received.messages.length).toBe(4);
                        expect(received.attempt).toBe(1);

                        received.messages.forEach((msg, idx) => {
                            expect(msg.message).toBe(
                                batch002Messages.at(idx) ?? "",
                            );
                        });

                        batch002Processed = true;
                    }

                    /**
                     * Resolve when both batches are processed
                     */
                    if (batch001Processed && batch002Processed) {
                        resolve();
                    }

                    return { success: true };
                },
                workerConfig,
            );
        });

        /**
         * Send 6 messages to batch-001
         */
        for (let i = 1; i <= 6; i++) {
            const message = `Spreadsheet row ${i} for batch-001`;
            await queue.sendBatchMessage({
                qname: "spreadsheet-queue:batch",
                batchId: "batch-001",
                message,
            });
            batch001Messages.push(message);
            await sleep(100); // Simulate messages arriving at different times
        }

        /**
         * Send 4 messages to batch-002
         */
        for (let i = 1; i <= 4; i++) {
            const message = `Spreadsheet row ${i} for batch-002`;
            await queue.sendBatchMessage({
                qname: "spreadsheet-queue:batch",
                batchId: "batch-002",
                message,
            });
            batch002Messages.push(message);
            await sleep(100);
        }

        /**
         * Wait for batches to be processed
         * The cron job will run every 3 seconds and process all pending batches
         */
        await workerPromise;

        /**
         * Assert both batches were processed
         */
        expect(processedBatches.size).toBe(2);
        expect(batch001Processed).toBe(true);
        expect(batch002Processed).toBe(true);

        await queue.close();
    });

    test("6. Can send and receive batched messages with selective retry", async () => {
        const queue = new RedisQueue(redisConfig);

        /**
         * Create batch queue with 2-second interval and retry enabled
         */
        await queue.createQueue({
            qname: "retry-test:batch",
            every: 2,
            maxRetries: 3,
            maxBackoffSeconds: 1,
        });

        const processedBatches: Array<{
            batchId: string;
            attempt: number;
            success: boolean;
        }> = [];

        /**
         * Start worker BEFORE sending messages
         */
        const workerPromise = new Promise<void>((resolve) => {
            queue.startWorker(
                "retry-test:batch",
                async (received) => {
                    processedBatches.push({
                        batchId: received.batchId,
                        attempt: received.attempt,
                        success: false,
                    });

                    /**
                     * Simulate batch-001 failing on first 2 attempts
                     * batch-002 and batch-003 always succeed
                     */
                    if (received.batchId === "batch-001") {
                        if (received.attempt < 3) {
                            /**
                             * Mark as failed, will retry
                             */
                            const lastBatch = processedBatches.at(-1);
                            if (lastBatch) {
                                lastBatch.success = false;
                            }
                            return { success: false };
                        }
                        /**
                         * 3rd attempt succeeds
                         */
                        expect(received.attempt).toBe(3);
                        const lastBatch = processedBatches.at(-1);
                        if (lastBatch) {
                            lastBatch.success = true;
                        }
                    } else {
                        /**
                         * batch-002 and batch-003 succeed immediately
                         */
                        expect(received.attempt).toBe(1);
                        const lastBatch = processedBatches.at(-1);
                        if (lastBatch) {
                            lastBatch.success = true;
                        }
                    }

                    /**
                     * Check if all batches are done:
                     * - batch-001: should appear 3 times (attempt 1, 2, 3)
                     * - batch-002: should appear 1 time (attempt 1)
                     * - batch-003: should appear 1 time (attempt 1)
                     * Total: 5 invocations
                     */
                    const batch001Count = processedBatches.filter(
                        (b) => b.batchId === "batch-001",
                    ).length;
                    const batch002Count = processedBatches.filter(
                        (b) => b.batchId === "batch-002",
                    ).length;
                    const batch003Count = processedBatches.filter(
                        (b) => b.batchId === "batch-003",
                    ).length;

                    if (
                        batch001Count === 3 &&
                        batch002Count === 1 &&
                        batch003Count === 1
                    ) {
                        resolve();
                    }

                    return { success: true };
                },
                workerConfig,
            );
        });

        /**
         * Create 3 different batches
         */
        for (let i = 1; i <= 3; i++) {
            await queue.sendBatchMessage({
                qname: "retry-test:batch",
                batchId: `batch-00${i}`,
                message: `Message ${i}`,
            });
        }

        /**
         * Wait for all batches to be processed (including retries)
         */
        await workerPromise;

        /**
         * Verify total handler invocations
         */
        expect(processedBatches.length).toBe(5);

        /**
         * Verify batch-001 retry behavior
         */
        const batch001Attempts = processedBatches.filter(
            (b) => b.batchId === "batch-001",
        );
        expect(batch001Attempts.length).toBe(3);

        const batch001Attempt1 = batch001Attempts.at(0);
        expect(batch001Attempt1?.attempt).toBe(1);
        expect(batch001Attempt1?.success).toBe(false);

        const batch001Attempt2 = batch001Attempts.at(1);
        expect(batch001Attempt2?.attempt).toBe(2);
        expect(batch001Attempt2?.success).toBe(false);

        const batch001Attempt3 = batch001Attempts.at(2);
        expect(batch001Attempt3?.attempt).toBe(3);
        expect(batch001Attempt3?.success).toBe(true);

        /**
         * Verify batch-002 succeeded on first attempt
         */
        const batch002Attempts = processedBatches.filter(
            (b) => b.batchId === "batch-002",
        );
        expect(batch002Attempts.length).toBe(1);

        const batch002Attempt1 = batch002Attempts.at(0);
        expect(batch002Attempt1?.attempt).toBe(1);
        expect(batch002Attempt1?.success).toBe(true);

        /**
         * Verify batch-003 succeeded on first attempt
         */
        const batch003Attempts = processedBatches.filter(
            (b) => b.batchId === "batch-003",
        );
        expect(batch003Attempts.length).toBe(1);

        const batch003Attempt1 = batch003Attempts.at(0);
        expect(batch003Attempt1?.attempt).toBe(1);
        expect(batch003Attempt1?.success).toBe(true);

        /**
         * Verify independence: batch-002 and batch-003 were not affected by batch-001 failures
         */
        const successfulBatches = processedBatches.filter(
            (b) => b.success && b.attempt === 1,
        );
        expect(successfulBatches.length).toBe(2); // batch-002 and batch-003

        await queue.close();
    });
});

afterAll(async () => {
    const redis = new Bun.RedisClient(
        `redis://${redisConfig.host}:${redisConfig.port}`,
    );
    let cursor = "0";
    const pattern = `${redisConfig.namespace}:*`;

    do {
        const result = await redis.scan(cursor, "MATCH", pattern);
        const [nextCursor, keys] = result as [string, string[]];

        cursor = nextCursor;

        if (keys.length > 0) {
            await redis.del(...keys);
        }
    } while (cursor !== "0");

    redis.close();
});
