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

const WORKER_COUNT = 16;
const CONCURRENCY = 50;
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
    await queue.createQueue({ maxRetries: 0, qname, maxsize: 150_000 });

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
                    testMessage: config.testMessage,
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


  const mediumMessage = '{"batch_id":"batch_analytics_2201","source_system":"inventory_management_v2","generated_at":"2023-10-27T10:00:00Z","record_count":5,"data":[{"sku":"KEY-MECH-RGB-001","warehouse_id":"WH-NY-01","stock_level":450,"reserved":12,"incoming":100,"last_audit":"2023-10-25","dimensions":{"w":15,"h":5,"l":40,"unit":"cm"},"tags":["electronics","peripherals","hot_item"]},{"sku":"MON-4K-IPS-002","warehouse_id":"WH-CA-02","stock_level":85,"reserved":5,"incoming":0,"last_audit":"2023-10-26","dimensions":{"w":60,"h":40,"l":10,"unit":"cm"},"tags":["electronics","display","fragile"]},{"sku":"LAP-PRO-M2-003","warehouse_id":"WH-TX-05","stock_level":200,"reserved":45,"incoming":50,"last_audit":"2023-10-20","dimensions":{"w":30,"h":2,"l":20,"unit":"cm"},"tags":["computer","high_value","insured"]},{"sku":"MOU-ERG-WL-004","warehouse_id":"WH-NY-01","stock_level":1200,"reserved":0,"incoming":200,"last_audit":"2023-10-22","dimensions":{"w":8,"h":5,"l":12,"unit":"cm"},"tags":["peripherals","office","sale"]},{"sku":"CAB-HDMI-2M-005","warehouse_id":"WH-WA-03","stock_level":5000,"reserved":120,"incoming":1000,"last_audit":"2023-10-01","dimensions":{"w":10,"h":2,"l":10,"unit":"cm"},"tags":["accessory","cable","generic"]}],"system_logs":{"warnings":[],"errors":[{"code":404,"msg":"SKU-IMG-MISSING for CAB-HDMI-2M-005","timestamp":1678892500}],"performance_metrics":{"query_time_ms":145,"db_reads":25,"cpu_load_percent":12}}}'

    const availableCPUs = navigator.hardwareConcurrency;
    log("yellow", `\n💻 Detected ${availableCPUs} CPU cores`);

    log("blue", `\n🔧 Using ${WORKER_COUNT} workers for tests`);

    try {
        // Test 1: Small messages - Maximum throughput
        log("cyan", "\n📋 Test 1: Small messages (100 bytes)");
        const test1Results = await testRegularQueueParallel({
            concurrency: CONCURRENCY,
            messageCount: MSG_COUNT,
            testMessage: '{"id":"msg_82103","type":"notification","timestamp":1678892301,"payload":{"user_id":4092,"event":"login_attempt","status":"success"}}',
            workerCount:WORKER_COUNT,
        });
        printResults(
            "Test 1: Small Messages (50K msgs, 100 bytes)",
            test1Results,
        );


        // Test 2: Medium messages
        log("cyan", "\n📋 Test 2: Medium messages (1KB)");
        const test2Results = await testRegularQueueParallel({
            concurrency: CONCURRENCY,
            messageCount: MSG_COUNT,
            testMessage: mediumMessage,
            workerCount:  WORKER_COUNT,
        });
        printResults(
            "Test 2: Medium Messages (50K msgs, 1KB)",
            test2Results,
        );


        // Test 3: Large messages
        log("cyan", "\n📋 Test 3: Large messages (10KB)");
        const test3Results = await testRegularQueueParallel({
            concurrency: CONCURRENCY,
            messageCount: MSG_COUNT,
            testMessage: '[{"_id":"6973b4f29af00ad243d8dfb1","index":0,"guid":"3da3f9d7-2406-4f07-8cc5-20c77a18ade4","isActive":false,"balance":"$1,361.82","picture":"http://placehold.it/32x32","age":37,"eyeColor":"green","name":"Hart Booth","gender":"male","company":"SULTRAX","email":"hartbooth@sultrax.com","phone":"+1 (966) 498-3673","address":"843 Hall Street, Trona, Oregon, 1712","about":"Deserunt fugiat nisi voluptate quis ex nisi reprehenderit est eiusmod officia sunt quis elit ea. Quis non mollit consectetur amet nulla anim ipsum consequat aliqua reprehenderit tempor reprehenderit. Sint pariatur pariatur laboris sint dolor sint voluptate pariatur ut adipisicing officia Lorem deserunt. Ullamco minim duis sit consequat aliqua in sunt nostrud amet nisi deserunt voluptate pariatur. Qui ullamco aliquip sunt veniam occaecat nulla ex incididunt. Aute mollit incididunt ad in dolor culpa pariatur non commodo magna nostrud non.\r\n","registered":"2025-07-22T10:26:43 -07:00","latitude":-3.647469,"longitude":69.437957,"tags":["qui","est","aliqua","ullamco","eiusmod","eu","velit"],"friends":[{"id":0,"name":"Lakisha Turner"},{"id":1,"name":"Leonor Ewing"},{"id":2,"name":"Frazier Irwin"}],"greeting":"Hello, Hart Booth! You have 6 unread messages.","favoriteFruit":"banana"},{"_id":"6973b4f2021b2c498f984bb6","index":1,"guid":"25034ff5-fbc8-4d09-8c63-e60f919e9ad4","isActive":false,"balance":"$2,141.41","picture":"http://placehold.it/32x32","age":33,"eyeColor":"brown","name":"Bethany Davenport","gender":"female","company":"STROZEN","email":"bethanydavenport@strozen.com","phone":"+1 (818) 499-2890","address":"506 Quay Street, Springville, Texas, 5249","about":"Consectetur occaecat sunt consectetur culpa nulla qui sunt labore proident et irure fugiat eu nulla. Voluptate est dolore labore aliqua velit duis. Proident occaecat culpa esse deserunt nulla aliqua. Nulla ea minim esse consectetur fugiat nostrud in anim esse cillum. Quis dolor quis reprehenderit ex ex ad esse minim est.\r\n","registered":"2020-08-03T11:36:56 -07:00","latitude":-10.479417,"longitude":28.341411,"tags":["non","eiusmod","amet","cillum","incididunt","elit","exercitation"],"friends":[{"id":0,"name":"Phyllis Bryan"},{"id":1,"name":"Mallory Gill"},{"id":2,"name":"Schneider Mercado"}],"greeting":"Hello, Bethany Davenport! You have 9 unread messages.","favoriteFruit":"apple"},{"_id":"6973b4f2220a607ed78df07a","index":2,"guid":"871776e1-d133-4ae7-8343-4fca929bb75c","isActive":true,"balance":"$2,630.76","picture":"http://placehold.it/32x32","age":35,"eyeColor":"brown","name":"Bertie Peters","gender":"female","company":"PERKLE","email":"bertiepeters@perkle.com","phone":"+1 (841) 517-3179","address":"972 Nassau Street, Ventress, Wyoming, 585","about":"Fugiat reprehenderit excepteur deserunt magna laboris aute culpa. Voluptate amet nisi commodo nostrud ipsum do nisi consectetur sint deserunt qui sint proident. Consequat cupidatat deserunt aliquip aliqua occaecat velit ad nostrud deserunt qui. Sunt amet sunt voluptate reprehenderit nisi eiusmod deserunt ea cillum Lorem qui anim aute duis.\r\n","registered":"2025-08-27T03:50:35 -07:00","latitude":32.284355,"longitude":-78.324272,"tags":["consequat","id","voluptate","tempor","proident","non","occaecat"],"friends":[{"id":0,"name":"Foreman Quinn"},{"id":1,"name":"Rojas Kemp"},{"id":2,"name":"Hanson Buck"}],"greeting":"Hello, Bertie Peters! You have 3 unread messages.","favoriteFruit":"banana"},{"_id":"6973b4f2bd348f1a31aca9f8","index":3,"guid":"fc19928a-b4f9-441e-ade9-efd7bad779af","isActive":false,"balance":"$1,273.51","picture":"http://placehold.it/32x32","age":40,"eyeColor":"blue","name":"Rosales Sims","gender":"male","company":"CANOPOLY","email":"rosalessims@canopoly.com","phone":"+1 (869) 427-3088","address":"153 Guernsey Street, Ona, New Jersey, 183","about":"Laborum sit sunt ad deserunt velit nisi culpa pariatur est ea mollit. Velit excepteur nulla cupidatat reprehenderit laboris sint amet duis exercitation dolore voluptate commodo sit Lorem. Velit exercitation ea incididunt adipisicing voluptate occaecat.\r\n","registered":"2017-11-04T12:50:51 -07:00","latitude":0.771702,"longitude":82.991386,"tags":["cillum","qui","sint","consectetur","eiusmod","consequat","ex"],"friends":[{"id":0,"name":"Imogene Lamb"},{"id":1,"name":"Key Burris"},{"id":2,"name":"Reyna Mayer"}],"greeting":"Hello, Rosales Sims! You have 7 unread messages.","favoriteFruit":"apple"},{"_id":"6973b4f207f65e544265d355","index":4,"guid":"abc15ba6-3214-4ef8-ac30-8afb642b82c2","isActive":true,"balance":"$2,945.08","picture":"http://placehold.it/32x32","age":26,"eyeColor":"brown","name":"Gretchen Jordan","gender":"female","company":"ANOCHA","email":"gretchenjordan@anocha.com","phone":"+1 (948) 454-2346","address":"890 Monaco Place, Loomis, Alabama, 4341","about":"Nulla excepteur est consequat velit aute laborum sunt voluptate aute eu eiusmod. Anim sunt eu veniam in commodo sit eu. Anim commodo incididunt ad reprehenderit anim culpa sit ut. Laborum esse in pariatur velit. Ut elit adipisicing magna ut officia non magna exercitation incididunt enim esse magna ad veniam. Deserunt irure ut dolor dolore labore pariatur veniam ipsum voluptate sint enim dolor. Ad exercitation sint sit occaecat excepteur eiusmod et sit.\r\n","registered":"2022-03-30T04:45:03 -07:00","latitude":29.089418,"longitude":-41.417872,"tags":["minim","amet","aliqua","excepteur","irure","et","culpa"],"friends":[{"id":0,"name":"Sonia Hayden"},{"id":1,"name":"Levy Burks"},{"id":2,"name":"Wolfe Hancock"}],"greeting":"Hello, Gretchen Jordan! You have 5 unread messages.","favoriteFruit":"banana"},{"_id":"6973b4f250d546997251a3d3","index":5,"guid":"ec6af806-cb4c-4ecc-b208-68b4b3cafd93","isActive":false,"balance":"$2,535.09","picture":"http://placehold.it/32x32","age":33,"eyeColor":"brown","name":"Ruiz Tran","gender":"male","company":"AQUASURE","email":"ruiztran@aquasure.com","phone":"+1 (962) 478-3889","address":"705 Farragut Road, Broadlands, Michigan, 1955","about":"Sunt enim Lorem excepteur sint mollit deserunt. Aute commodo amet sit cillum voluptate mollit exercitation ad cillum aute fugiat mollit. Quis ad cupidatat veniam reprehenderit aute cupidatat quis. Minim laboris est fugiat et in cillum id nisi dolore reprehenderit fugiat minim. Nulla ut consequat do sit dolor laboris.\r\n","registered":"2018-09-27T01:51:44 -07:00","latitude":-59.151306,"longitude":143.667188,"tags":["laboris","anim","eiusmod","quis","nulla","aliqua","duis"],"friends":[{"id":0,"name":"Ramona Brock"},{"id":1,"name":"Kayla Webb"},{"id":2,"name":"Rutledge Mcclure"}],"greeting":"Hello, Ruiz Tran! You have 6 unread messages.","favoriteFruit":"apple"},{"_id":"6973b4f20e1b602ed3098386","index":6,"guid":"d63efc08-287a-487e-a159-bbb4f231c37a","isActive":false,"balance":"$3,941.09","picture":"http://placehold.it/32x32","age":37,"eyeColor":"blue","name":"Reba Oneal","gender":"female","company":"NIQUENT","email":"rebaoneal@niquent.com","phone":"+1 (994) 443-2343","address":"229 Clifton Place, Cochranville, Pennsylvania, 6468","about":"Officia veniam do minim elit duis in nulla. Deserunt veniam in ut esse duis nulla cillum magna ullamco dolore veniam exercitation Lorem. Eiusmod pariatur irure dolor aute dolore minim adipisicing sint ullamco. Ipsum id voluptate est ipsum adipisicing est. Do anim exercitation commodo mollit fugiat minim est ea.\r\n","registered":"2020-05-02T09:23:38 -07:00","latitude":-45.360857,"longitude":-17.113491,"tags":["laborum","dolor","incididunt","elit","adipisicing","adipisicing","veniam"],"friends":[{"id":0,"name":"Janine Russell"},{"id":1,"name":"Abby Short"},{"id":2,"name":"Savannah Grant"}],"greeting":"Hello, Reba Oneal! You have 7 unread messages.","favoriteFruit":"banana"},{"_id":"6973b4f208d0cb4b5c53bb93","index":7,"guid":"152d21f2-05cd-41d2-a048-ba499944b2bf","isActive":false,"balance":"$1,155.29","picture":"http://placehold.it/32x32","age":25,"eyeColor":"green","name":"Vanessa Rodriquez","gender":"female","company":"OPTYK","email":"vanessarodriquez@optyk.com","phone":"+1 (873) 492-2951","address":"102 Thatford Avenue, Keyport, Minnesota, 7404","about":"Adipisicing sit dolor reprehenderit Lorem et voluptate culpa ullamco cillum officia dolor culpa et sit. Lorem minim eu laboris nulla do est labore nulla eu occaecat. Est occaecat sit id irure. Enim sint consequat amet mollit occaecat in mollit duis. Eiusmod et tempor laborum est tempor fugiat anim ullamco.\r\n","registered":"2019-11-03T12:06:46 -07:00","latitude":30.194412,"longitude":155.798353,"tags":["aliqua","cupidatat","quis","eiusmod","ad","veniam","velit"],"friends":[{"id":0,"name":"June Little"},{"id":1,"name":"Lamb Marsh"},{"id":2,"name":"Richards Conley"}],"greeting":"Hello, Vanessa Rodriquez! You have 5 unread messages.","favoriteFruit":"banana"},{"_id":"6973b4f244d84ce83babb8e8","index":8,"guid":"2d743ccf-cffe-46ac-821a-1a32180f8401","isActive":true,"balance":"$1,531.94","picture":"http://placehold.it/32x32","age":33,"eyeColor":"brown","name":"Kathy Warren","gender":"female","company":"NURPLEX","email":"kathywarren@nurplex.com","phone":"+1 (958) 514-2617","address":"211 Cypress Court, Rosewood, Massachusetts, 2530","about":"Cillum nisi ad esse aliqua do amet eu dolor. Culpa culpa adipisicing officia nisi magna occaecat aliquip labore nostrud anim. Eu esse in aliquip exercitation reprehenderit cupidatat eu veniam deserunt cillum excepteur id. Id dolor reprehenderit do deserunt. Enim eiusmod nostrud exercitation fugiat consequat. Nulla deserunt cillum mollit excepteur ea ad veniam. Id nostrud laboris ipsum cupidatat ex non commodo ad sunt.\r\n","registered":"2017-06-14T06:01:20 -07:00","latitude":34.733711,"longitude":-163.804679,"tags":["reprehenderit","elit","exercitation","voluptate","aute","adipisicing","occaecat"],"friends":[{"id":0,"name":"Allie Holcomb"},{"id":1,"name":"Muriel Weaver"},{"id":2,"name":"Tommie Carr"}],"greeting":"Hello, Kathy Warren! You have 4 unread messages.","favoriteFruit":"apple"},{"_id":"6973b4f297dc750e1750eb58","index":9,"guid":"1ccffd92-ff08-429d-9232-e86c565e70da","isActive":true,"balance":"$1,662.22","picture":"http://placehold.it/32x32","age":31,"eyeColor":"green","name":"Hodges Richardson","gender":"male","company":"POSHOME","email":"hodgesrichardson@poshome.com","phone":"+1 (970) 590-2870","address":"686 Anna Court, Elizaville, Marshall Islands, 5762","about":"Ullamco cupidatat nisi do ut nostrud est pariatur labore dolore exercitation quis minim ipsum quis. Aute enim excepteur voluptate ullamco laboris adipisicing commodo. Do amet sunt occaecat id commodo. Consectetur veniam excepteur nisi in commodo. Laborum anim dolor velit deserunt culpa. Exercitation do amet laborum do ipsum voluptate commodo. Aliqua exercitation consequat nisi pariatur ea veniam officia nostrud excepteur tempor.\r\n","registered":"2016-07-28T02:54:58 -07:00","latitude":-14.492817,"longitude":134.965697,"tags":["qui","aliquip","pariatur","magna","ad","et","irure"],"friends":[{"id":0,"name":"Robyn Willis"},{"id":1,"name":"Gabrielle Holt"},{"id":2,"name":"Wilkerson Bailey"}],"greeting":"Hello, Hodges Richardson! You have 5 unread messages.","favoriteFruit":"strawberry"},{"_id":"6973b4f277b5d5924e85e34c","index":10,"guid":"24b6f2d6-f153-4826-9c2f-f50c42d907e5","isActive":true,"balance":"$1,715.09","picture":"http://placehold.it/32x32","age":33,"eyeColor":"blue","name":"Floyd Petty","gender":"male","company":"GINKOGENE","email":"floydpetty@ginkogene.com","phone":"+1 (832) 499-3944","address":"910 Canal Avenue, Mammoth, North Dakota, 1708","about":"Eiusmod velit proident sit proident ipsum irure. Excepteur dolore consequat labore duis tempor nulla laborum do amet labore. Amet nisi ullamco ad enim officia id exercitation aliquip in fugiat in. Anim quis et enim proident dolore magna mollit consectetur ea elit velit nostrud. Consectetur ea labore esse esse est nisi consectetur magna ut ullamco nulla ipsum exercitation.\r\n","registered":"2023-09-18T01:14:07 -07:00","latitude":-51.559233,"longitude":134.694377,"tags":["cupidatat","elit","commodo","ullamco","do","reprehenderit","nostrud"],"friends":[{"id":0,"name":"Naomi Garrison"},{"id":1,"name":"Betty English"},{"id":2,"name":"Prince Singleton"}],"greeting":"Hello, Floyd Petty! You have 7 unread messages.","favoriteFruit":"strawberry"},{"_id":"6973b4f2e7ad4e31bb2510c3","index":11,"guid":"74ccb6d9-7f24-4451-a3a0-e632bab385b3","isActive":true,"balance":"$1,490.53","picture":"http://placehold.it/32x32","age":36,"eyeColor":"blue","name":"Johnnie Simon","gender":"female","company":"JAMNATION","email":"johnniesimon@jamnation.com","phone":"+1 (868) 417-3009","address":"125 Chester Street, Whipholt, Maryland, 9466","about":"Dolor anim esse enim Lorem nostrud id officia incididunt amet laboris laboris sunt. Pariatur dolor aute Lorem aute magna. Laborum enim non voluptate commodo.\r\n","registered":"2015-03-03T08:28:39 -07:00","latitude":-19.882734,"longitude":-45.527604,"tags":["qui","sit","culpa","id","dolor","irure","id"],"friends":[{"id":0,"name":"Fry Ayers"},{"id":1,"name":"Donovan Foley"},{"id":2,"name":"Finch Dunn"}],"greeting":"Hello, Johnnie Simon! You have 9 unread messages.","favoriteFruit":"apple"},{"_id":"6973b4f24d0d75c705a54911","index":12,"guid":"f972a1ef-0d3a-41ab-b080-eaf7d26184f1","isActive":true,"balance":"$3,429.83","picture":"http://placehold.it/32x32","age":36,"eyeColor":"brown","name":"Rosalind Duran","gender":"female","company":"ZORROMOP","email":"rosalindduran@zorromop.com","phone":"+1 (987) 589-3940","address":"720 Mill Avenue, Gardiner, Nebraska, 6579","about":"Dolor deserunt nisi laborum eiusmod aliquip adipisicing ullamco eiusmod non qui consectetur est nostrud. Incididunt nostrud labore minim Lorem elit qui commodo consectetur ex culpa veniam. Quis ad quis cillum esse nisi ea ipsum consequat dolore. Non minim minim sint excepteur anim consectetur est voluptate laborum commodo. Mollit ea magna dolor sit ut magna voluptate ipsum reprehenderit id ex cillum deserunt.\r\n","registered":"2014-07-03T10:56:30 -07:00","latitude":-39.091253,"longitude":-131.671201,"tags":["labore","enim","nulla","est","exercitation","ipsum","deserunt"],"friends":[{"id":0,"name":"Emerson Dalton"},{"id":1,"name":"Owen Bass"},{"id":2,"name":"Bell Shannon"}],"greeting":"Hello, Rosalind Duran! You have 1 unread messages.","favoriteFruit":"banana"},{"_id":"6973b4f2edea35a5d2cea142","index":13,"guid":"18cf8f91-9735-4fa6-9381-ae8c1060890c","isActive":true,"balance":"$1,811.69","picture":"http://placehold.it/32x32","age":26,"eyeColor":"blue","name":"Pauline Cain","gender":"female","company":"ENAUT","email":"paulinecain@enaut.com","phone":"+1 (972) 408-3809","address":"508 Rapelye Street, Thynedale, Maine, 9391","about":"Qui enim fugiat do eu in eu aliqua elit cillum occaecat. Consectetur amet pariatur ea dolore Lorem eu ipsum veniam commodo commodo. Mollit dolore velit aliqua cillum elit consequat sunt nostrud in qui ipsum sit eu irure. Cupidatat reprehenderit labore in officia laborum.\r\n","registered":"2023-12-23T01:34:49 -07:00","latitude":-18.152332,"longitude":6.220963,"tags":["quis","sint","proident","officia","dolore","elit","quis"],"friends":[{"id":0,"name":"Kristie Shepherd"},{"id":1,"name":"Jensen Webster"},{"id":2,"name":"Langley Young"}],"greeting":"Hello, Pauline Cain! You have 7 unread messages.","favoriteFruit":"apple"}]',
            workerCount: WORKER_COUNT,
        });
        printResults(
            "Test 3: Large Messages (50K msgs, 10KB)",
            test3Results,
        );

        await cleanup();



        // Summary
        log("bright", `\n${"=".repeat(60)}`);
        log("magenta", "  📈 BENCHMARK SUMMARY");
        log("bright", "=".repeat(60));

        console.log("\n  Regular Queue Performance:");
        console.log(
            `    - Small messages (100B):   ${test1Results.throughput.toFixed(0)} msg/s (p50: ${test1Results.p50.toFixed(2)}ms)`,
        );
        console.log(
            `    - Medium messages (1KB):   ${test2Results.throughput.toFixed(0)} msg/s (p50: ${test2Results.p50.toFixed(2)}ms)`,
        );
        console.log(
            `    - Large messages (100KB):  ${test3Results.throughput.toFixed(0)} msg/s (p50: ${test3Results.p50.toFixed(2)}ms)`,
        );

        log("bright", "\n" + "=".repeat(60));

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
