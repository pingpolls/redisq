#!/usr/bin/env bun

import RedisQueue from "./app";

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
    concurrency: number;
    messageSize: number;
    includeBatch?: boolean;
    batchSize?: number;
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

function log(color: keyof typeof COLORS, message: string) {
    console.log(`${COLORS[color]}${message}${COLORS.reset}`);
}

function calculatePercentile(sorted: number[], percentile: number): number {
    const index = Math.ceil((percentile / 100) * sorted.length) - 1;
    return sorted[Math.max(0, index)] ?? Number.POSITIVE_INFINITY;
}

function calculateStats(
    latencies: number[],
): Omit<TestResults, "totalMessages" | "duration" | "throughput"> {
    const sorted = [...latencies].sort((a, b) => a - b);
    const sum = latencies.reduce((acc, val) => acc + val, 0);

    return {
        avg: sum / latencies.length,
        latencies,
        max: sorted[sorted.length - 1] ?? Number.POSITIVE_INFINITY,
        min: sorted[0] ?? Number.POSITIVE_INFINITY,
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

async function testRegularQueue(
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
    const messageTimestamps = new Map<string, number>();
    let receivedCount = 0;

    const workerPromise = new Promise<void>((resolve) => {
        queue.startWorker(
            qname,
            async (message) => {
                const sendTime = messageTimestamps.get(message.id);
                if (sendTime) {
                    const latency = performance.now() - sendTime;
                    latencies.push(latency);
                    messageTimestamps.delete(message.id);
                }

                receivedCount++;

                if (receivedCount === config.messageCount) {
                    resolve();
                }

                return { success: true };
            },
            { concurrency: config.concurrency, silent: true },
        );
    });

    // Generate test message
    const testMessage = "x".repeat(config.messageSize);

    log(
        "blue",
        `\n🚀 Sending ${config.messageCount.toLocaleString()} messages...`,
    );
    const startTime = performance.now();

    // Send all messages
    const sendPromises = Array.from(
        { length: config.messageCount },
        async () => {
            const sendTimestamp = performance.now();
            const id = await queue.sendMessage({
                message: testMessage,
                qname,
            });
            messageTimestamps.set(id, sendTimestamp);
        },
    );

    await Promise.all(sendPromises);
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

async function testBatchQueue(config: StressTestConfig): Promise<TestResults> {
    const queue = new RedisQueue({
        host: process.env.REDIS_HOST || "127.0.0.1",
        namespace: "stress-test",
        port: process.env.REDIS_PORT || "6379",
    });

    const qname = "stress-batch:batch";
    const batchInterval = 2; // 2 seconds for stress test
    await queue.createQueue({
        every: batchInterval,
        maxRetries: 0,
        qname,
    });

    const latencies: number[] = [];
    const batchTimestamps = new Map<string, number>();
    let receivedCount = 0;
    const batchSize = config.batchSize || 100;
    const expectedBatches = Math.ceil(config.messageCount / batchSize);

    const workerPromise = new Promise<void>((resolve) => {
        queue.startWorker(
            qname,
            async (batch) => {
                const sendTime = batchTimestamps.get(batch.batchId);
                if (sendTime) {
                    const latency = performance.now() - sendTime;
                    // Record latency for each message in the batch
                    for (let i = 0; i < batch.messages.length; i++) {
                        latencies.push(latency);
                    }
                    batchTimestamps.delete(batch.batchId);
                }

                receivedCount += batch.messages.length;

                if (receivedCount >= config.messageCount) {
                    resolve();
                }

                return { success: true };
            },
            { silent: true },
        );
    });

    // Generate test message
    const testMessage = "x".repeat(config.messageSize);

    log(
        "blue",
        `\n🚀 Sending ${config.messageCount.toLocaleString()} messages in ${expectedBatches} batches...`,
    );
    const startTime = performance.now();

    // Send messages in batches
    for (let batchNum = 0; batchNum < expectedBatches; batchNum++) {
        const batchId = `batch-${batchNum}`;
        const batchStartTime = performance.now();
        batchTimestamps.set(batchId, batchStartTime);

        const messagesInBatch = Math.min(
            batchSize,
            config.messageCount - batchNum * batchSize,
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

async function testDelayedMessages(): Promise<TestResults> {
    const queue = new RedisQueue({
        host: process.env.REDIS_HOST || "127.0.0.1",
        namespace: "stress-test",
        port: process.env.REDIS_PORT || "6379",
    });

    const qname = "stress-delayed";
    await queue.createQueue({ maxRetries: 0, qname });

    const messageCount = 1000;
    const delays = [100, 500, 1000, 2000, 5000]; // Various delays in ms
    const latencies: number[] = [];
    const sendTimestamps = new Map<
        string,
        { sent: number; expectedDelay: number }
    >();
    let receivedCount = 0;

    const workerPromise = new Promise<void>((resolve) => {
        queue.startWorker(
            qname,
            async (message) => {
                const info = sendTimestamps.get(message.id);
                if (info) {
                    const actualDelay = performance.now() - info.sent;
                    const latency = actualDelay - info.expectedDelay;
                    latencies.push(Math.abs(latency));
                    sendTimestamps.delete(message.id);
                }

                receivedCount++;

                if (receivedCount === messageCount) {
                    resolve();
                }

                return { success: true };
            },
            { concurrency: 10, silent: true },
        );
    });

    log(
        "blue",
        `\n🚀 Sending ${messageCount.toLocaleString()} delayed messages...`,
    );
    const startTime = performance.now();

    // Send messages with various delays
    const sendPromises = Array.from({ length: messageCount }, async (_, i) => {
        const delay = delays[i % delays.length];
        const sendTime = performance.now();

        const id = await queue.sendMessage({
            delay,
            message: `message-${i}`,
            qname,
        });

        sendTimestamps.set(id, { expectedDelay: delay ?? 0, sent: sendTime });
    });

    await Promise.all(sendPromises);
    const sendDuration = performance.now() - startTime;
    log("green", `✓ All messages sent in ${sendDuration.toFixed(2)}ms`);

    log("blue", "\n⏳ Waiting for all delayed messages to be processed...");
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

    try {
        // Test 1: Small messages, high throughput
        const test1Results = await testRegularQueue({
            concurrency: 10,
            messageCount: 10000,
            messageSize: 100,
        });
        printResults(
            "Test 1: Regular Queue - Small Messages (10K msgs, 100 bytes)",
            test1Results,
        );

        await cleanup();

        // Test 2: Large messages
        const test2Results = await testRegularQueue({
            concurrency: 5,
            messageCount: 1000,
            messageSize: 10000,
        });
        printResults(
            "Test 2: Regular Queue - Large Messages (1K msgs, 10KB)",
            test2Results,
        );

        await cleanup();

        // Test 3: Very high throughput
        const test3Results = await testRegularQueue({
            concurrency: 20,
            messageCount: 50000,
            messageSize: 50,
        });
        printResults(
            "Test 3: Regular Queue - High Throughput (50K msgs, 50 bytes)",
            test3Results,
        );

        await cleanup();

        // Test 4: Batch processing
        const test4Results = await testBatchQueue({
            batchSize: 250,
            concurrency: 1,
            messageCount: 5000,
            messageSize: 100,
        });
        printResults(
            "Test 4: Batch Queue - Batch Processing (5K msgs, 250/batch)",
            test4Results,
        );

        await cleanup();

        // Test 5: Delayed messages
        const test5Results = await testDelayedMessages();
        printResults(
            "Test 5: Delayed Messages - Timing Accuracy (1K msgs)",
            test5Results,
        );

        await cleanup();

        // Summary
        log("bright", `\n${"=".repeat(60)}`);
        log("magenta", "  📈 BENCHMARK SUMMARY");
        log("bright", "=".repeat(60));

        console.log("\n  Regular Queue Performance:");
        console.log(
            `    - Small messages:   ${test1Results.throughput.toFixed(0)} msg/s (p50: ${test1Results.p50.toFixed(2)}ms)`,
        );
        console.log(
            `    - Large messages:   ${test2Results.throughput.toFixed(0)} msg/s (p50: ${test2Results.p50.toFixed(2)}ms)`,
        );
        console.log(
            `    - High throughput:  ${test3Results.throughput.toFixed(0)} msg/s (p50: ${test3Results.p50.toFixed(2)}ms)`,
        );

        console.log("\n  Batch Queue Performance:");
        console.log(
            `    - Batch processing: ${test4Results.throughput.toFixed(0)} msg/s (p50: ${test4Results.p50.toFixed(2)}ms)`,
        );

        console.log("\n  Delayed Messages:");
        console.log(
            `    - Timing accuracy:  ±${test5Results.avg.toFixed(2)}ms average deviation`,
        );
        console.log(
            `    - Processing rate:  ${test5Results.throughput.toFixed(0)} msg/s`,
        );

        log("bright", "\n" + "=".repeat(60));

        log("green", "\n✅ All stress tests completed successfully!");

        log(
            "yellow",
            "\n💡 To update README.md Performance section, use these values:",
        );
        log(
            "cyan",
            `   - Throughput: ~${Math.round(test3Results.throughput)} messages/second`,
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
