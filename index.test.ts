import RedisQueue from "./index";

async function testBasicSendReceive() {
    console.log("\n=== Test: Basic Send/Receive ===");
    const queue = new RedisQueue({
        host: "127.0.0.1",
        port: "6379",
    });

    await queue.createQueue({ qname: "test-basic" });

    const message = new TextEncoder().encode("Hello, World!");
    const id = await queue.sendMessage({
        message,
        qname: "test-basic",
    });
    console.log(`Sent message: ${id}`);

    const received = await queue.receiveMessage("test-basic");
    if (received && !("batchId" in received)) {
        console.log(`Received: ${new TextDecoder().decode(received.message)}`);
        console.log(`Attempt: ${received.attempt}`);
    }

    await queue.deleteQueue("test-basic");
    await queue.close();
}

async function testRetrySystem() {
    console.log("\n=== Test: Retry System ===");
    const queue = new RedisQueue({
        host: "127.0.0.1",
        port: "6379",
    });

    await queue.createQueue({ qname: "test-retry" });

    let attemptCount = 0;

    queue.startWorker("test-retry", async (message) => {
        if (!("batchId" in message)) {
            attemptCount++;
            console.log(
                `Processing attempt ${message.attempt}, total attempts: ${attemptCount}`,
            );

            // Fail first 2 attempts
            if (message.attempt < 3) {
                console.log("Simulating failure...");
                return { success: false };
            }

            console.log("Success!");
            return { success: true };
        }
        return { success: true };
    });

    // Send with max 5 retries
    const message = new TextEncoder().encode("Retry me!");
    await queue.sendMessage({
        maxBackoffSeconds: 10,
        maxRetries: 5,
        message,
        qname: "test-retry",
    });

    // Let it process
    await Bun.sleep(30000);

    queue.stopWorker("test-retry");
    await queue.deleteQueue("test-retry");
    await queue.close();
}

async function testBatchMessages() {
    console.log("\n=== Test: Batch Messages ===");
    const queue = new RedisQueue({
        host: "127.0.0.1",
        port: "6379",
    });

    await queue.createQueue({ qname: "test-batch" });

    // Send multiple messages to same batch
    for (let i = 1; i <= 5; i++) {
        const message = new TextEncoder().encode(`Batch message ${i}`);
        await queue.sendBatchMessage({
            batchId: "batch-001",
            message,
            qname: "test-batch",
        });
    }

    console.log("Sent 5 messages to batch-001");

    // Receive batch
    const received = await queue.popMessage("test-batch");
    if (received && "batchId" in received) {
        console.log(
            `Received batch ${received.batchId} with ${received.messages.length} messages`,
        );
        received.messages.forEach((msg, idx) => {
            console.log(
                `  ${idx + 1}: ${new TextDecoder().decode(msg.message)}`,
            );
        });
    }

    await queue.deleteQueue("test-batch");
    await queue.close();
}

async function testMultipleBatches() {
    console.log("\n=== Test: Multiple Batches (Fixed FIFO) ===");
    const queue = new RedisQueue({
        host: "127.0.0.1",
        port: "6379",
    });

    await queue.createQueue({ qname: "test-multi-batch" });

    // Simulate hourly gemini-queue with different users
    console.log("Sending batch for userId1...");
    for (let i = 1; i <= 3; i++) {
        await queue.sendBatchMessage({
            batchId: "userId1",
            message: new TextEncoder().encode(`User1 message ${i}`),
            qname: "test-multi-batch",
        });
    }

    console.log("Sending batch for userId2...");
    for (let i = 1; i <= 2; i++) {
        await queue.sendBatchMessage({
            batchId: "userId2",
            message: new TextEncoder().encode(`User2 message ${i}`),
            qname: "test-multi-batch",
        });
    }

    console.log("Sending batch for userId3...");
    await queue.sendBatchMessage({
        batchId: "userId3",
        message: new TextEncoder().encode("User3 message 1"),
        qname: "test-multi-batch",
    });

    console.log("\nPopping batches...");

    // Pop first batch
    let batch = await queue.popMessage("test-multi-batch");
    if (batch && "batchId" in batch) {
        console.log(
            `\nBatch ${batch.batchId}: ${batch.messages.length} messages`,
        );
    }

    // Pop second batch
    batch = await queue.popMessage("test-multi-batch");
    if (batch && "batchId" in batch) {
        console.log(
            `Batch ${batch.batchId}: ${batch.messages.length} messages`,
        );
    }

    // Pop third batch
    batch = await queue.popMessage("test-multi-batch");
    if (batch && "batchId" in batch) {
        console.log(
            `Batch ${batch.batchId}: ${batch.messages.length} messages`,
        );
    }

    await queue.deleteQueue("test-multi-batch");
    await queue.close();
}

async function testBatchRetry() {
    console.log("\n=== Test: Batch Retry (Per-Message) ===");
    const queue = new RedisQueue({
        host: "127.0.0.1",
        port: "6379",
    });

    await queue.createQueue({ qname: "test-batch-retry" });

    let processCount = 0;

    queue.startWorker("test-batch-retry", async (message) => {
        if ("batchId" in message) {
            processCount++;
            console.log(
                `\nProcessing batch ${message.batchId} (attempt ${processCount})`,
            );

            message.messages.forEach((msg) => {
                console.log(
                    `  Message attempt ${msg.attempt}: ${new TextDecoder().decode(msg.message)}`,
                );
            });

            // Fail first attempt, succeed second
            if (processCount === 1) {
                console.log("Simulating batch failure...");
                return { success: false };
            }

            console.log("Batch succeeded!");
            return { success: true };
        }
        return { success: true };
    });

    // Send batch with retry
    for (let i = 1; i <= 3; i++) {
        await queue.sendBatchMessage({
            batchId: "retry-batch",
            maxBackoffSeconds: 5,
            maxRetries: 3,
            message: new TextEncoder().encode(`Message ${i}`),
            qname: "test-batch-retry",
        });
    }

    await Bun.sleep(15000);

    queue.stopWorker("test-batch-retry");
    await queue.deleteQueue("test-batch-retry");
    await queue.close();
}

// Run all tests
async function runAllTests() {
    await testBasicSendReceive();
    await testBatchMessages();
    await testMultipleBatches(); // NEW: Test multiple batches work correctly
    await testRetrySystem(); // Long running
    await testBatchRetry(); // Test batch retry mechanism
}

// Uncomment to run tests
runAllTests();
