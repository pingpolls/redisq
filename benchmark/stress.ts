#!/usr/bin/env bun

import type { Worker } from "bun";
import RedisQueue from "../app";

const COLORS = {
    blue: "\x1b[34m",
    bright: "\x1b[1m",
    cyan: "\x1b[36m",
    green: "\x1b[32m",
    magenta: "\x1b[35m",
    red: "\x1b[31m",
    reset: "\x1b[0m",
    yellow: "\x1b[33m",
};

interface StressTestConfig {
    messageCount: number;
    workerCount: number;
    messageSize: number;
    concurrency: number;
}

interface TestResults {
    totalMessages: number;
    duration: number;
    throughput: number;
    latencies: number[];
    p50: number;
    p95: number;
    p99: number;
    min: number;
    max: number;
    avg: number;
}

interface WorkerMessage {
    type: "start" | "result" | "error";
    data?: {
        workerIndex?: number;
        messagesPerWorker?: number;
        messageSize?: number;
        qname?: string;
        redisConfig?: {
            host: string;
            port: string;
            namespace: string;
        };
        latencies?: number[];
        error?: string;
    };
}

function log(color: keyof typeof COLORS, message: string) {
    console.log(`${COLORS[color]}${message}${COLORS.reset}`);
}

function calculatePercentile(sorted: number[], percentile: number): number {
    const index = Math.ceil((percentile / 100) * sorted.length) - 1;
    return sorted[Math.max(0, index)] ?? 0;
}

function calculateStats(
    latencies: number[],
): Omit<TestResults, "totalMessages" | "duration" | "throughput"> {
    const sorted = [...latencies].sort((a, b) => a - b);
    const sum = latencies.reduce((acc, val) => acc + val, 0);

    return {
        avg: sum / latencies.length,
        latencies,
        max: sorted[sorted.length - 1] ?? 0,
        min: sorted[0] ?? 0,
        p50: calculatePercentile(sorted, 50),
        p95: calculatePercentile(sorted, 95),
        p99: calculatePercentile(sorted, 99),
    };
}

function printResults(testName: string, results: TestResults) {
    log("bright", `\n${"=".repeat(60)}`);
    log("cyan", `  ${testName}`);
    log("bright", "=".repeat(60));

    log("green", "\n📊 Overall Performance:");
    console.log(
        `  Total Messages:     ${results.totalMessages.toLocaleString()}`,
    );
    console.log(`  Duration:           ${results.duration.toFixed(2)}s`);
    console.log(`  Throughput:         ${results.throughput.toFixed(2)} msg/s`);

    log("yellow", "\n⚡ Latency Statistics (ms):");
    console.log(`  Min:                ${results.min.toFixed(2)} ms`);
    console.log(`  Average:            ${results.avg.toFixed(2)} ms`);
    console.log(`  Median (p50):       ${results.p50.toFixed(2)} ms`);
    console.log(`  p95:                ${results.p95.toFixed(2)} ms`);
    console.log(`  p99:                ${results.p99.toFixed(2)} ms`);
    console.log(`  Max:                ${results.max.toFixed(2)} ms`);
}

async function testRegularQueueParallel(
    config: StressTestConfig,
): Promise<TestResults> {
    const queue = new RedisQueue({
        host: process.env.REDIS_HOST || "127.0.0.1",
        namespace: "stress-test",
        port: process.env.REDIS_PORT || "6379",
    });

    const qname = "stress-regular";
    await queue.createQueue({ maxRetries: 0, qname });

    const allLatencies: number[] = [];
    let receivedCount = 0;
    const messagesPerWorker = Math.floor(
        config.messageCount / config.workerCount,
    );
    const actualMessageCount = messagesPerWorker * config.workerCount;

    const workerPromise = new Promise<void>((resolve) => {
        queue.startWorker(
            qname,
            async () => {
                receivedCount++;
                if (receivedCount === actualMessageCount) {
                    resolve();
                }
                return { success: true };
            },
            { concurrency: config.concurrency, silent: true },
        );
    });

    log(
        "blue",
        `\n🚀 Spawning ${config.workerCount} workers to send ${actualMessageCount.toLocaleString()} messages...`,
    );
    const startTime = performance.now();

    // Spawn workers using Bun's Worker API
    const workers: Worker[] = [];
    const workerPromises: Promise<number[]>[] = [];

    for (let i = 0; i < config.workerCount; i++) {
        const worker = new Worker(
            new URL("./stress-worker.ts", import.meta.url).href,
        );
        workers.push(worker);

        const workerPromise = new Promise<number[]>((resolve, reject) => {
            worker.addEventListener(
                "message",
                (event: MessageEvent<WorkerMessage>) => {
                    if (event.data.type === "result") {
                        resolve(event.data.data?.latencies || []);
                        worker.terminate();
                    } else if (event.data.type === "error") {
                        reject(
                            new Error(event.data.data?.error || "Worker error"),
                        );
                        worker.terminate();
                    }
                },
            );

            worker.addEventListener("error", (error) => {
                reject(error);
                worker.terminate();
            });

            worker.postMessage({
                data: {
                    messageSize: config.messageSize,
                    messagesPerWorker,
                    qname,
                    redisConfig: {
                        host: process.env.REDIS_HOST || "127.0.0.1",
                        namespace: "stress-test",
                        port: process.env.REDIS_PORT || "6379",
                    },
                    workerIndex: i,
                },
                type: "start",
            });
        });

        workerPromises.push(workerPromise);
    }

    // Wait for all workers to finish sending
    const workerResults = await Promise.all(workerPromises);

    // Collect all latencies
    for (const latencies of workerResults) {
        allLatencies.push(...latencies);
    }

    const sendDuration = performance.now() - startTime;
    log("green", `✓ All messages sent in ${sendDuration.toFixed(2)}ms`);

    log("blue", "\n⏳ Waiting for all messages to be processed...");
    await workerPromise;

    const duration = (performance.now() - startTime) / 1000;
    const throughput = actualMessageCount / duration;

    await queue.close();

    const stats = calculateStats(allLatencies);

    return {
        duration,
        throughput,
        totalMessages: actualMessageCount,
        ...stats,
    };
}

async function testRegularQueueSingle(
    config: StressTestConfig,
): Promise<TestResults> {
    const queue = new RedisQueue({
        host: process.env.REDIS_HOST || "127.0.0.1",
        namespace: "stress-test",
        port: process.env.REDIS_PORT || "6379",
    });

    const qname = "stress-regular";
    await queue.createQueue({ maxRetries: 0, qname });

    const latencies: number[] = [];
    let receivedCount = 0;

    const workerPromise = new Promise<void>((resolve) => {
        queue.startWorker(
            qname,
            async () => {
                receivedCount++;
                if (receivedCount === config.messageCount) {
                    resolve();
                }
                return { success: true };
            },
            { concurrency: config.concurrency, silent: true },
        );
    });

    const testMessage = "x".repeat(config.messageSize);

    log(
        "blue",
        `\n🚀 Sending ${config.messageCount.toLocaleString()} messages...`,
    );
    const startTime = performance.now();

    // Send all messages and track send latency
    for (let i = 0; i < config.messageCount; i++) {
        const sendStart = performance.now();
        await queue.sendMessage({
            message: testMessage,
            qname,
        });
        const sendLatency = performance.now() - sendStart;
        latencies.push(sendLatency);
    }

    const sendDuration = performance.now() - startTime;
    log("green", `✓ All messages sent in ${sendDuration.toFixed(2)}ms`);

    log("blue", "\n⏳ Waiting for all messages to be processed...");
    await workerPromise;

    const duration = (performance.now() - startTime) / 1000;
    const throughput = config.messageCount / duration;

    await queue.close();

    const stats = calculateStats(latencies);

    return {
        duration,
        throughput,
        totalMessages: config.messageCount,
        ...stats,
    };
}

async function testBatchQueue(
    messageCount: number,
    batchSize: number,
    messageSize: number,
): Promise<TestResults> {
    const queue = new RedisQueue({
        host: process.env.REDIS_HOST || "127.0.0.1",
        namespace: "stress-test",
        port: process.env.REDIS_PORT || "6379",
    });

    const qname = "stress-batch:batch";
    const batchInterval = 2;
    await queue.createQueue({
        every: batchInterval,
        maxRetries: 0,
        qname,
    });

    const latencies: number[] = [];
    const batchTimestamps = new Map<string, number>();
    let receivedCount = 0;
    const expectedBatches = Math.ceil(messageCount / batchSize);

    const workerPromise = new Promise<void>((resolve) => {
        queue.startWorker(
            qname,
            async (batch: { batchId: string; messages: string | any[]; }) => {
                const sendTime = batchTimestamps.get(batch.batchId);
                if (sendTime) {
                    const latency = performance.now() - sendTime;
                    for (let i = 0; i < batch.messages.length; i++) {
                        latencies.push(latency);
                    }
                    batchTimestamps.delete(batch.batchId);
                }

                receivedCount += batch.messages.length;

                if (receivedCount >= messageCount) {
                    resolve();
                }

                return { success: true };
            },
            { silent: true },
        );
    });

    const testMessage = "x".repeat(messageSize);

    log(
        "blue",
        `\n🚀 Sending ${messageCount.toLocaleString()} messages in ${expectedBatches} batches...`,
    );
    const startTime = performance.now();

    for (let batchNum = 0; batchNum < expectedBatches; batchNum++) {
        const batchId = `batch-${batchNum}`;
        const batchStartTime = performance.now();
        batchTimestamps.set(batchId, batchStartTime);

        const messagesInBatch = Math.min(
            batchSize,
            messageCount - batchNum * batchSize,
        );

        await Promise.all(
            Array.from({ length: messagesInBatch }, () =>
                queue.sendBatchMessage({
                    batchId,
                    message: testMessage,
                    qname,
                }),
            ),
        );
    }

    const sendDuration = performance.now() - startTime;
    log("green", `✓ All messages sent in ${sendDuration.toFixed(2)}ms`);

    log("blue", "\n⏳ Waiting for all batches to be processed...");
    await workerPromise;

    const duration = (performance.now() - startTime) / 1000;
    const throughput = messageCount / duration;

    await queue.close();

    const stats = calculateStats(latencies);

    return {
        duration,
        throughput,
        totalMessages: messageCount,
        ...stats,
    };
}

async function cleanup() {
    log("blue", "\n🧹 Cleaning up test data...");

    const redis = new Bun.RedisClient(
        `redis://${process.env.REDIS_HOST || "127.0.0.1"}:${process.env.REDIS_PORT || "6379"}`,
    );

    let cursor = "0";
    const pattern = "stress-test:*";
    let deletedCount = 0;

    do {
        const result = await redis.scan(cursor, "MATCH", pattern);
        const [nextCursor, keys] = result as [string, string[]];

        cursor = nextCursor;

        if (keys.length > 0) {
            await redis.del(...keys);
            deletedCount += keys.length;
        }
    } while (cursor !== "0");

    redis.close();

    log("green", `✓ Cleaned up ${deletedCount} keys`);
}

async function main() {
    log(
        "bright",
        "\n╔═══════════════════════════════════════════════════════════╗",
    );
    log(
        "bright",
        "║           RedisQ Stress Test & Benchmark Tool             ║",
    );
    log(
        "bright",
        "╚═══════════════════════════════════════════════════════════╝",
    );

    const availableCPUs = navigator.hardwareConcurrency;
    log("yellow", `\n💻 Detected ${availableCPUs} CPU cores`);

    // Use only 4 cores as requested
    const workerCount = 8;
    log("blue", `\n🔧 Using ${workerCount} workers for tests`);

    try {
        // Test 1: Parallel workers - Small messages, maximum throughput
        const test1Results = await testRegularQueueParallel({
            concurrency: 20,
            messageCount: 100000,
            messageSize: 100,
            workerCount,
        });
        printResults(
            "Test 1: Parallel Workers - Small Messages (100K msgs, 100 bytes)",
            test1Results,
        );

        await cleanup();

        // Test 2: Parallel workers - Medium load
        const test2Results = await testRegularQueueParallel({
            concurrency: 10,
            messageCount: 50000,
            messageSize: 1000,
            workerCount,
        });
        printResults(
            "Test 2: Parallel Workers - Medium Messages (50K msgs, 1KB)",
            test2Results,
        );

        await cleanup();

        // Test 3: Large messages
        const test3Results = await testRegularQueueSingle({
            concurrency: 10,
            messageCount: 5000,
            messageSize: 10000,
            workerCount: 1,
        });
        printResults(
            "Test 3: Single Thread - Large Messages (5K msgs, 10KB)",
            test3Results,
        );

        await cleanup();

        // Test 4: Batch processing
        const test4Results = await testBatchQueue(10000, 500, 100);
        printResults(
            "Test 4: Batch Queue - Batch Processing (10K msgs, 500/batch)",
            test4Results,
        );

        await cleanup();

        // Summary
        log("bright", `\n${"=".repeat(60)}`);
        log("magenta", "  📈 BENCHMARK SUMMARY");
        log("bright", "=".repeat(60));

        console.log("\n  Regular Queue Performance (Parallel Workers):");
        console.log(
            `    - Small messages:   ${test1Results.throughput.toFixed(0)} msg/s (p50: ${test1Results.p50.toFixed(2)}ms)`,
        );
        console.log(
            `    - Medium messages:  ${test2Results.throughput.toFixed(0)} msg/s (p50: ${test2Results.p50.toFixed(2)}ms)`,
        );

        console.log("\n  Regular Queue Performance (Single Thread):");
        console.log(
            `    - Large messages:   ${test3Results.throughput.toFixed(0)} msg/s (p50: ${test3Results.p50.toFixed(2)}ms)`,
        );

        console.log("\n  Batch Queue Performance:");
        console.log(
            `    - Batch processing: ${test4Results.throughput.toFixed(0)} msg/s (p50: ${test4Results.p50.toFixed(2)}ms)`,
        );

        log("bright", "\n=".repeat(60));

        log("green", "\n✅ All stress tests completed successfully!");

        log(
            "yellow",
            "\n💡 To update README.md Performance section, use these values:",
        );
        log(
            "cyan",
            `   - Throughput: ~${Math.round(test1Results.throughput).toLocaleString()} messages/second`,
        );
        log("cyan", `   - Latency (p50): ${test1Results.p50.toFixed(2)} ms`);
        log("cyan", `   - Latency (p95): ${test1Results.p95.toFixed(2)} ms`);
        log("cyan", `   - Latency (p99): ${test1Results.p99.toFixed(2)} ms`);
    } catch (error) {
        log(
            "red",
            `\n❌ Error during stress test: ${(error as Error).message}`,
        );
        log("red", (error as Error).stack || "");
        process.exit(1);
    }
}

main();
