import { RedisQ } from "../app";

declare var self: Worker;

interface WorkerMessage {
    type: "start";
    data: {
        workerIndex: number;
        messagesPerWorker: number;
        testMessage: string;
        qname: string;
        redisConfig: {
            host: string;
            port: string;
            namespace: string;
        };
    };
}

self.addEventListener("message", async (event: MessageEvent<WorkerMessage>) => {
    if (event.data.type === "start") {
        const { messagesPerWorker, testMessage, qname, redisConfig } =
            event.data.data;

        try {
            const queue = new RedisQ(redisConfig);
            const latencies: number[] = [];
            // Bounded in-flight window so latency measures per-message cost,
            // not time spent queued behind thousands of concurrent sends.
            const IN_FLIGHT = 64;
            let next = 0;

            const lane = async () => {
                while (next < messagesPerWorker) {
                    next++;
                    const start = performance.now();
                    await queue.sendMessage({ message: testMessage, qname });
                    latencies.push(performance.now() - start);
                }
            };

            await Promise.all(Array.from({ length: IN_FLIGHT }, lane));

            await queue.close();

            self.postMessage({
                data: { latencies },
                type: "result",
            });
        } catch (error) {
            self.postMessage({
                data: { error: (error as Error).message },
                type: "error",
            });
        }
    }
});
