import RedisQueue from "../app";

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
            const queue = new RedisQueue(redisConfig);
            const latencies: number[] = [];

            for (let i = 0; i < messagesPerWorker; i++) {
                const start = performance.now();
                await queue.sendMessage({
                    message: testMessage,
                    qname,
                });
                const latency = performance.now() - start;
                latencies.push(latency);
            }

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
