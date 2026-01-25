#!/usr/bin/env bun

import type { Worker } from "bun";
import RedisQueue from "../app";
import mediumMsg from "./medium.txt";
import smallMsg from "./small.txt";
import tinyMsg from "./tiny.txt";

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

const WORKER_COUNT = 8;
const CONCURRENCY = 2;
const MSG_COUNT = 100_000;

interface StressTestConfig {
    messageCount: number;
    workerCount: number;
    testMessage: string;
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
        testMessage?: string;
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
    await queue.createQueue({ maxRetries: 0, maxsize: 150_000, qname });

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
                    messagesPerWorker,
                    qname,
                    redisConfig: {
                        host: process.env.REDIS_HOST || "127.0.0.1",
                        namespace: "stress-test",
                        port: process.env.REDIS_PORT || "6379",
                    },
                    testMessage: config.testMessage,
                    workerIndex: i,
                },
                type: "start",
            });
        });

        workerPromises.push(workerPromise);
    }

    const workerResults = await Promise.all(workerPromises);

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

    await cleanup();

    const availableCPUs = navigator.hardwareConcurrency;
    log("yellow", `\n💻 Detected ${availableCPUs} CPU cores`);

    log("blue", `\n🔧 Using ${WORKER_COUNT} workers for tests`);

    try {
        log("cyan", "\n📋 Test 1: Tiny messages (100 bytes)");
        const test1Results = await testRegularQueueParallel({
            concurrency: CONCURRENCY,
            messageCount: MSG_COUNT,
            testMessage: tinyMsg,
            workerCount: WORKER_COUNT,
        });
        printResults(
            "Test 1: Tiny Messages (100K msgs, 100 bytes)",
            test1Results,
        );

        await cleanup();

        log("cyan", "\n📋 Test 2: Small messages (1KB)");
        const test2Results = await testRegularQueueParallel({
            concurrency: CONCURRENCY,
            messageCount: MSG_COUNT,
            testMessage: smallMsg,
            workerCount: WORKER_COUNT,
        });
        printResults("Test 2: Small Messages (100K msgs, 1KB)", test2Results);

        await cleanup();

        log("cyan", "\n📋 Test 3: Medium messages (10KB)");
        const test3Results = await testRegularQueueParallel({
            concurrency: CONCURRENCY,
            messageCount: MSG_COUNT,
            testMessage: mediumMsg,
            workerCount: WORKER_COUNT,
        });
        printResults("Test 3: Medium Messages (100K msgs, 10KB)", test3Results);

        await cleanup();

        // Calculate combined averages
        const avgThroughput =
            (test1Results.throughput +
                test2Results.throughput +
                test3Results.throughput) /
            3;
        const avgP50 =
            (test1Results.p50 + test2Results.p50 + test3Results.p50) / 3;
        const avgP95 =
            (test1Results.p95 + test2Results.p95 + test3Results.p95) / 3;
        const avgP99 =
            (test1Results.p99 + test2Results.p99 + test3Results.p99) / 3;

        // Summary
        log("bright", `\n${"=".repeat(60)}`);
        log("magenta", "  📈 BENCHMARK SUMMARY");
        log("bright", "=".repeat(60));

        console.log("\n  Individual Queue Performance:");
        console.log(
            `    - Tiny messages (100B):   ${test1Results.throughput.toFixed(0)} msg/s (p50: ${test1Results.p50.toFixed(2)}ms)`,
        );
        console.log(
            `    - Small messages (1KB):   ${test2Results.throughput.toFixed(0)} msg/s (p50: ${test2Results.p50.toFixed(2)}ms)`,
        );
        console.log(
            `    - Medium messages (10KB):  ${test3Results.throughput.toFixed(0)} msg/s (p50: ${test3Results.p50.toFixed(2)}ms)`,
        );

        log("bright", `\n${"=".repeat(60)}`);

        log("green", "\n✅ All stress tests completed successfully!");

        log(
            "yellow",
            "\n💡 To update README.md Performance section, use these values (averaged across all tests):",
        );
        console.log("\n  Overall:");
        log(
            "cyan",
            `   - **Throughput**: ~${Math.round(avgThroughput).toLocaleString()} messages/second`,
        );
        log("cyan", `   - **Latency (p50)**: ${avgP50.toFixed(2)} ms`);
        log("cyan", `   - **Latency (p95)**: ${avgP95.toFixed(2)} ms`);
        log("cyan", `   - **Latency (p99)**: ${avgP99.toFixed(2)} ms`);
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
