# RedisQ

A lightweight, type-safe Redis-based message queue for Bun with support for delayed messages, retries, and batch processing.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Built by PingPolls](https://img.shields.io/badge/Built%20by-PingPolls-orange)](https://pingpolls.com)

## Features

- ✨ **Type-safe** - Full TypeScript support with generic types
- 🚀 **Fast** - Built on top of Bun's native Redis client, processing thousands of messages per second
- ⏰ **Delayed Messages** - Schedule messages for future processing
- 🔄 **Automatic Retries** - Exponential backoff with configurable limits
- 📦 **Batch Processing** - Collect and process messages in batches at scheduled intervals
- 🎯 **Simple API** - Easy to use with minimal configuration
- 🔧 **Flexible** - Works with any Bun-based framework

## Installation

```bash
bun install @pingpolls/redisq
```

> **Note:** This package is built for Bun and requires Bun runtime to work.

## Quick Start

```typescript
import RedisQueue from '@pingpolls/redisq';

// Initialize the queue
const queue = new RedisQueue({
  host: '127.0.0.1',
  port: '6379',
  namespace: 'myapp'
});

// Create a queue
await queue.createQueue({ qname: 'emails' });

// Send a message
await queue.sendMessage({
  qname: 'emails',
  message: JSON.stringify({ to: 'user@example.com', subject: 'Hello!' })
});

// Start a worker to process messages
await queue.startWorker('emails', async (message) => {
  const email = JSON.parse(message.message);
  console.log(`Sending email to ${email.to}`);
  
  // Process your message here
  await sendEmail(email);
  
  return { success: true };
});
```

## Core Concepts

### Regular Queues

Regular queues process messages individually as they arrive. Perfect for real-time processing like sending emails, webhooks, or notifications.

### Batch Queues

Batch queues (suffix `:batch`) collect messages over a time period and process them together. Ideal for bulk operations like database imports, report generation, or aggregating analytics.

## Usage Examples

### SvelteKit

#### Option 1: Direct Import (Recommended for Simple Apps)

Create a queue service in `src/lib/server/queue.ts`:

```typescript
import RedisQueue from '@pingpolls/redisq';

export const queue = new RedisQueue({
  host: import.meta.env.REDIS_HOST || '127.0.0.1',
  port: import.meta.env.REDIS_PORT || '6379',
  namespace: 'sveltekit-app'
});

// Initialize queues
export async function initQueues() {
  await queue.createQueue({ qname: 'emails', maxRetries: 3 });
  await queue.createQueue({ 
    qname: 'analytics:batch', 
    every: 60, // Process every 60 seconds
    maxRetries: 2 
  });
}
```

Start workers in `src/hooks.server.ts`:

```typescript
import { queue, initQueues } from '$lib/server/queue';

await initQueues();

// Start email worker
queue.startWorker('emails', async (message) => {
  const data = JSON.parse(message.message);
  await sendEmail(data);
  return { success: true };
}, { silent: false });

// Start analytics batch worker
queue.startWorker('analytics:batch', async (batch) => {
  console.log(`Processing ${batch.messages.length} analytics events`);
  await saveToDB(batch.messages);
  return { success: true };
});
```

Use in your routes by importing directly:

```typescript
// src/routes/api/signup/+server.ts
import { queue } from '$lib/server/queue';
import { json } from '@sveltejs/kit';

export async function POST({ request }) {
  const { email, name } = await request.json();
  
  // Send welcome email
  await queue.sendMessage({
    qname: 'emails',
    message: JSON.stringify({ to: email, template: 'welcome', name })
  });
  
  // Track signup event
  await queue.sendBatchMessage({
    qname: 'analytics:batch',
    batchId: 'signups',
    message: JSON.stringify({ event: 'signup', email, timestamp: Date.now() })
  });
  
  return json({ success: true });
}
```

```typescript
// src/routes/dashboard/+page.server.ts
import { queue } from '$lib/server/queue';

export async function load() {
  // Queue a background task
  await queue.sendMessage({
    qname: 'emails',
    message: JSON.stringify({ type: 'daily-report' })
  });
  
  return {
    message: 'Report queued'
  };
}
```

#### Option 2: Using event.locals (Recommended for Larger Apps)

This approach provides better dependency injection and testing capabilities with SvelteKit 5.

Create the queue service in `src/lib/server/queue.ts`:

```typescript
import RedisQueue from '@pingpolls/redisq';

export function createQueue() {
  return new RedisQueue({
    host: import.meta.env.REDIS_HOST || '127.0.0.1',
    port: import.meta.env.REDIS_PORT || '6379',
    namespace: 'sveltekit-app'
  });
}

export async function initQueues(queue: RedisQueue) {
  await queue.createQueue({ qname: 'emails', maxRetries: 3 });
  await queue.createQueue({ 
    qname: 'analytics:batch', 
    every: 60,
    maxRetries: 2 
  });
}
```

Setup in `src/hooks.server.ts`:

```typescript
import type { Handle } from '@sveltejs/kit';
import { createQueue, initQueues } from '$lib/server/queue';

// Create single queue instance
const queue = createQueue();
await initQueues(queue);

// Start workers
queue.startWorker('emails', async (message) => {
  const data = JSON.parse(message.message);
  await sendEmail(data);
  return { success: true };
});

queue.startWorker('analytics:batch', async (batch) => {
  await saveToDB(batch.messages);
  return { success: true };
});

export const handle: Handle = async ({ event, resolve }) => {
  // Attach queue to event.locals
  event.locals.queue = queue;
  
  return resolve(event);
};
```

Add TypeScript types in `src/app.d.ts`:

```typescript
import type RedisQueue from '@pingpolls/redisq';

declare global {
  namespace App {
    interface Locals {
      queue: RedisQueue;
    }
  }
}

export {};
```

Now use `event.locals.queue` in your routes:

```typescript
// src/routes/api/signup/+server.ts
import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';

export const POST: RequestHandler = async ({ request, locals }) => {
  const { email, name } = await request.json();
  
  // Access queue from locals
  await locals.queue.sendMessage({
    qname: 'emails',
    message: JSON.stringify({ to: email, template: 'welcome', name })
  });
  
  await locals.queue.sendBatchMessage({
    qname: 'analytics:batch',
    batchId: 'signups',
    message: JSON.stringify({ event: 'signup', email, timestamp: Date.now() })
  });
  
  return json({ success: true });
};
```

```typescript
// src/routes/orders/[id]/+page.server.ts
import type { PageServerLoad, Actions } from './$types';

export const load: PageServerLoad = async ({ params, locals }) => {
  // Queue background task
  await locals.queue.sendMessage({
    qname: 'emails',
    message: JSON.stringify({ type: 'order-confirmation', orderId: params.id })
  });
  
  return {
    orderId: params.id
  };
};

export const actions: Actions = {
  cancel: async ({ params, locals }) => {
    await locals.queue.sendMessage({
      qname: 'emails',
      message: JSON.stringify({ type: 'order-cancellation', orderId: params.id })
    });
    
    return { success: true };
  }
};
```

**Benefits of using `event.locals`:**
- Single queue instance across your app
- Better for dependency injection and testing
- Cleaner imports in routes
- Type-safe access with TypeScript
- Follows SvelteKit 5 best practices

### Next.js (App Router)

Create queue in `lib/queue.ts`:

```typescript
import RedisQueue from '@pingpolls/redisq';

export const queue = new RedisQueue({
  host: process.env.REDIS_HOST!,
  port: process.env.REDIS_PORT!,
  namespace: 'nextjs-app'
});
```

Initialize in `app/api/worker/route.ts`:

```typescript
import { queue } from '@/lib/queue';

// Create queues on startup
await queue.createQueue({ qname: 'notifications', maxRetries: 3 });

// Start worker
queue.startWorker('notifications', async (message) => {
  const notification = JSON.parse(message.message);
  await sendPushNotification(notification);
  return { success: true };
});

export async function GET() {
  return Response.json({ status: 'Worker running' });
}
```

Use in API routes:

```typescript
// app/api/orders/route.ts
import { queue } from '@/lib/queue';

export async function POST(request: Request) {
  const order = await request.json();
  
  // Queue order confirmation email
  await queue.sendMessage({
    qname: 'notifications',
    message: JSON.stringify({ type: 'order', orderId: order.id }),
    delay: 5000 // Send after 5 seconds
  });
  
  return Response.json({ success: true });
}
```

### Nuxt

Create queue plugin in `server/plugins/queue.ts`:

```typescript
import RedisQueue from '@pingpolls/redisq';

export default defineNitroPlugin(async (nitroApp) => {
  const queue = new RedisQueue({
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: process.env.REDIS_PORT || '6379',
    namespace: 'nuxt-app'
  });

  await queue.createQueue({ qname: 'tasks', maxRetries: 3 });

  // Start worker
  queue.startWorker('tasks', async (message) => {
    const task = JSON.parse(message.message);
    await processTask(task);
    return { success: true };
  });

  // Make queue available globally
  nitroApp.queue = queue;
});
```

Use in API routes:

```typescript
// server/api/process.post.ts
export default defineEventHandler(async (event) => {
  const body = await readBody(event);
  
  await event.context.nitroApp.queue.sendMessage({
    qname: 'tasks',
    message: JSON.stringify(body)
  });
  
  return { queued: true };
});
```

### Hono

```typescript
import { Hono } from 'hono';
import RedisQueue from '@pingpolls/redisq';

const app = new Hono();

const queue = new RedisQueue({
  host: '127.0.0.1',
  port: '6379',
  namespace: 'hono-app'
});

// Initialize queue
await queue.createQueue({ qname: 'webhooks', maxRetries: 3 });
await queue.createQueue({ 
  qname: 'logs:batch', 
  every: 30,
  maxRetries: 2 
});

// Start regular queue worker
queue.startWorker('webhooks', async (message) => {
  const webhook = JSON.parse(message.message);
  await fetch(webhook.url, {
    method: 'POST',
    body: JSON.stringify(webhook.data),
    headers: { 'Content-Type': 'application/json' }
  });
  return { success: true };
});

// Start batch queue worker
queue.startWorker('logs:batch', async (batch) => {
  console.log(`Processing ${batch.messages.length} logs`);
  await saveLogsToDatabase(batch.messages);
  return { success: true };
});

// API endpoints
app.post('/webhook', async (c) => {
  const body = await c.req.json();
  
  await queue.sendMessage({
    qname: 'webhooks',
    message: JSON.stringify(body),
    delay: 1000 // Delay 1 second
  });
  
  return c.json({ queued: true });
});

app.post('/log', async (c) => {
  const log = await c.req.json();
  
  await queue.sendBatchMessage({
    qname: 'logs:batch',
    batchId: 'app-logs',
    message: JSON.stringify(log)
  });
  
  return c.json({ logged: true });
});

export default app;
```

## API Reference

### Constructor

```typescript
new RedisQueue(options: QueueOptions)
```

**Options:**

```typescript
type QueueOptions = {
  host: string;
  port: string;
  user?: string;
  password?: string;
  namespace?: string;
  tls?: boolean;
} | {
  redis: BunRedisClient;
}
```

### createQueue

Creates a new queue with specified configuration.

```typescript
await queue.createQueue({
  qname: 'my-queue',
  maxsize?: 65536,
  maxRetries?: 0,
  maxBackoffSeconds?: 30
});

// For batch queues
await queue.createQueue({
  qname: 'my-queue:batch',
  every?: 60, // Process every 60 seconds
  maxRetries?: 0,
  maxBackoffSeconds?: 30
});
```

**Parameters:**
- `qname` - Queue name (must end with `:batch` for batch queues)
- `maxsize` - Maximum message size in bytes (default: 65536)
- `maxRetries` - Max retry attempts: `0` = no retry, `-1` = unlimited, `n` = max retries (default: 0)
- `maxBackoffSeconds` - Maximum backoff time for exponential backoff (default: 30)
- `every` - Batch processing interval in seconds (batch queues only, default: 60)

### sendMessage

Sends a message to a regular queue.

```typescript
const id = await queue.sendMessage({
  qname: 'my-queue',
  message: 'Hello, World!',
  delay?: 5000 // Optional delay in milliseconds
});
```

### sendBatchMessage

Sends a message to a batch queue.

```typescript
await queue.sendBatchMessage({
  qname: 'analytics:batch',
  batchId: 'user-events',
  message: JSON.stringify({ event: 'click', timestamp: Date.now() })
});
```

**Note:** Messages with the same `batchId` are processed together in the same batch.

### startWorker

Starts a worker to process messages.

```typescript
// Regular queue worker
await queue.startWorker('my-queue', async (message) => {
  console.log(message.id);
  console.log(message.message);
  console.log(message.attempt);
  console.log(message.sent);
  
  return { success: true }; // or { success: false } to retry
}, {
  concurrency?: 1,
  silent?: false
});

// Batch queue worker
await queue.startWorker('analytics:batch', async (batch) => {
  console.log(batch.batchId);
  console.log(batch.messages); // Array of messages
  console.log(batch.attempt);
  console.log(batch.sent);
  
  return { success: true };
});
```

### deleteMessage

Manually delete a message from the queue.

```typescript
await queue.deleteMessage('my-queue', messageId);
```

### getQueue

Get queue attributes and message count.

```typescript
const attrs = await queue.getQueue('my-queue');
console.log(attrs.msgs); // Number of pending messages
```

### listQueues

List all queue names.

```typescript
const queues = await queue.listQueues();
```

### deleteQueue

Delete a queue and all its data.

```typescript
await queue.deleteQueue('my-queue');
```

### stopWorker

Stop a specific worker.

```typescript
queue.stopWorker('my-queue');
```

### close

Close all workers and Redis connection.

```typescript
await queue.close();
```

## Message Types

### Message (Regular Queue)

```typescript
interface Message {
  id: string;
  message: string;
  sent: number; // Timestamp
  attempt: number; // Current attempt (1-based)
}
```

### BatchMessage (Batch Queue)

```typescript
interface BatchMessage {
  batchId: string;
  messages: Array<{
    id: string;
    message: string;
    sent: number;
  }>;
  sent: number; // Batch creation timestamp
  attempt: number; // Batch-level attempt counter
}
```

## Retry Behavior

When a worker returns `{ success: false }` or throws an error:

1. Message is retried with exponential backoff: `2^attempt + random(0-1000ms)`
2. Backoff is capped at `maxBackoffSeconds`
3. After `maxRetries` attempts, the message is deleted
4. Set `maxRetries: -1` for unlimited retries

**Example backoff times (maxBackoffSeconds = 30):**
- Attempt 1: ~2 seconds
- Attempt 2: ~4 seconds
- Attempt 3: ~8 seconds
- Attempt 4: ~16 seconds
- Attempt 5+: 30 seconds (capped)

## Batch Processing

Batch queues collect messages over time and process them together:

1. Messages are grouped by `batchId`
2. Every `every` seconds, all pending batches are processed
3. Each batch is processed independently
4. If a batch fails, only that batch is retried
5. Successful batches are deleted immediately

**Use cases:**
- Bulk database inserts
- Aggregating analytics events
- Processing CSV uploads in chunks
- Generating periodic reports
- Batching API calls to external services

## Environment Variables

```bash
# Redis Configuration
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
REDIS_PASSWORD=your-password
REDIS_USER=your-username
REDIS_TLS=false
```

## Best Practices

1. **Message Format**: Always use JSON for complex data
   ```typescript
   message: JSON.stringify({ userId: 123, action: 'signup' })
   ```

2. **Error Handling**: Return `{ success: false }` for retriable errors, throw for fatal errors
   ```typescript
   try {
     await sendEmail(data);
     return { success: true };
   } catch (error) {
     if (error.code === 'RATE_LIMIT') {
       return { success: false }; // Retry
     }
     throw error; // Fatal error
   }
   ```

3. **Idempotency**: Make sure your workers can handle duplicate messages safely

4. **Monitoring**: Use `silent: false` in production to log errors

5. **Graceful Shutdown**: Always call `queue.close()` on app shutdown

6. **Batch Sizing**: Choose appropriate `every` intervals based on your workload
   - High frequency: 10-30 seconds
   - Medium frequency: 60-300 seconds
   - Low frequency: 300-3600 seconds

## Testing

```typescript
import { describe, test, expect } from 'bun:test';
import RedisQueue from '@pingpolls/redisq';

describe('Queue Tests', () => {
  test('processes messages', async () => {
    const queue = new RedisQueue({ host: '127.0.0.1', port: '6379' });
    
    await queue.createQueue({ qname: 'test' });
    
    const received: string[] = [];
    
    const promise = new Promise<void>((resolve) => {
      queue.startWorker('test', async (msg) => {
        received.push(msg.message);
        resolve();
        return { success: true };
      });
    });
    
    await queue.sendMessage({ qname: 'test', message: 'Hello' });
    await promise;
    
    expect(received).toContain('Hello');
    await queue.close();
  });
});
```

## Performance

Run the stress test to benchmark on your hardware:

```bash
bun stress
```

📈 BENCHMARK SUMMARY

Regular Queue Performance:
- Small messages (100B):   46943 msg/s (p50: 53.72ms)
- Medium messages (1KB):   34260 msg/s (p50: 81.21ms)
- Large messages (10KB):  4533 msg/s (p50: 253.26ms)

✅ All stress tests completed successfully!

💡 To update README.md Performance section, use these values:
   - **Throughput**: ~46,943 messages/second
   - **Latency (p50)**: 53.72 ms
   - **Latency (p95)**: 69.70 ms
   - **Latency (p99)**: 75.17 ms

*Results may vary based on hardware, Redis configuration, and network conditions.*

## License

Apache License 2.0

## Credits

Built with ❤️ by [PingPolls](https://pingpolls.com/?ref=redisq)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

- 🐛 [Report bugs](https://github.com/pingpolls/redisq/issues)
- 💡 [Request features](https://github.com/pingpolls/redisq/issues)
- 📧 [Contact us](https://pingpolls.com/contact?ref=redisq)
