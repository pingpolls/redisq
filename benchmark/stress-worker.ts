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
            const CHUNK_SIZE = 1000;

            for (let i = 0; i < messagesPerWorker; i += CHUNK_SIZE) {
                const promises = [];
                const chunkEnd = Math.min(i + CHUNK_SIZE, messagesPerWorker);

                for (let j = i; j < chunkEnd; j++) {
                    const start = performance.now();
                    promises.push(
                        queue
                            .sendMessage({
                                message: testMessage,
                                qname,
                            })
                            .then(() => {
                                const latency = performance.now() - start;
                                latencies.push(latency);
                            }),
                    );
                }
                await Promise.all(promises);
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
