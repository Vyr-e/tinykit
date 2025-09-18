# Usage Guide

This guide will walk you through the main workflows for using TinyKit to interact with Tinybird, from basic setup to data queries and ingestion.

## 1. Defining Your Schema

TinyKit allows you to define your Tinybird DataSource schema in TypeScript with full type safety.

```typescript
import { defineSchema, string, int64 } from 'tinykit';

export const eventsSchema = defineSchema({
  id: string('id'),
  userId: string('userId'),
  event: string('event'),
  timestamp: int64('timestamp'),
  properties: string('properties'),
});
```

## 2. Creating Your DataSource

Define your DataSource with the appropriate engine and sorting key for optimal query performance.

```typescript
import { defineDataSource } from 'tinykit';
import { eventsSchema } from './schema';

export const eventsDataSource = defineDataSource({
  name: 'events__v1',
  schema: eventsSchema,
  engine: 'MergeTree',
  sortingKey: ['timestamp', 'userId'],
});
```

## 3. Building Query Pipes

Create reusable query pipes with parameters for common analytics operations.

```typescript
import { definePipe, defineParameters, stringParam, int64Param, query, count, param } from 'tinykit';
import { eventsSchema } from './schema';

const userEventsParams = defineParameters({
  userId: stringParam('userId', { required: true }),
  limit: int64Param('limit', { default: 100 }),
});

export const getEventsByUser = definePipe({
  name: 'get_events_by_user__v1',
  schema: eventsSchema,
  parameters: userEventsParams,
}).endpoint((q, params) =>
  query(eventsSchema)
    .select('id', 'userId', 'event', 'timestamp')
    .from('events__v1')
    .where(`userId = ${param('userId', 'String', true)}`)
    .orderBy('timestamp DESC')
    .limit(params.limit)
);

const eventCountsParams = defineParameters({
  startTime: int64Param('startTime', { required: true }),
  endTime: int64Param('endTime', { required: true }),
});

export const getEventCounts = definePipe({
  name: 'get_event_counts__v1',
  schema: eventsSchema,
  parameters: eventCountsParams,
}).endpoint((q, params) =>
  query(eventsSchema)
    .selectRaw(`event, ${count()} as event_count`)
    .from('events__v1')
    .where(`timestamp >= ${param('startTime', 'Int64', true)}`)
    .and(`timestamp <= ${param('endTime', 'Int64', true)}`)
    .groupBy('event')
    .orderBy('event_count DESC')
);
```

## 4. Setting Up the Client

Configure your Tinybird client with your datasources and pipes.

```typescript
import { Tinybird } from 'tinykit';
import { eventsDataSource } from './datasource';
import { getEventsByUser, getEventCounts } from './pipes';

const tb = new Tinybird({
  token: process.env.TINYBIRD_TOKEN!,
  datasources: {
    events: eventsDataSource,
  },
  pipes: {
    getEventsByUser,
    getEventCounts,
  },
});
```

## 5. Executing Queries

Use the configured pipes to execute type-safe queries.

```typescript
import { z } from 'zod';

const getUserEvents = tb.pipe({
  pipe: 'get_events_by_user__v1',
  data: z.object({
    id: z.string(),
    userId: z.string(),
    event: z.string(),
    timestamp: z.number(),
  }),
});

const getEventCountsPipe = tb.pipe({
  pipe: 'get_event_counts__v1',
  data: z.object({
    event: z.string(),
    event_count: z.number(),
  }),
});

// Execute queries
const userEvents = await getUserEvents({ userId: 'user-123' });
console.log(`Found ${userEvents.data.length} events for user`);

const eventCounts = await getEventCountsPipe({
  startTime: Date.now() - 24 * 60 * 60 * 1000, // 24 hours ago
  endTime: Date.now(),
});
console.log('Event counts:', eventCounts.data);
```

## 6. Data Ingestion

TinyKit supports type-safe data ingestion to your DataSources.

```typescript
import { defineIngest } from 'tinykit';

const eventsIngest = defineIngest({
  datasource: 'events__v1',
  schema: eventsSchema,
});

async function ingestEvents() {
  const ingest = tb.ingest(eventsIngest);
  
  await ingest([
    {
      id: 'evt-1',
      userId: 'user-456',
      event: 'page_view',
      timestamp: Date.now(),
      properties: JSON.stringify({ page: '/home' }),
    },
    {
      id: 'evt-2',
      userId: 'user-456',
      event: 'button_click',
      timestamp: Date.now(),
      properties: JSON.stringify({ button: 'signup' }),
    },
  ]);
  
  console.log('Events ingested successfully');
}
```

## 7. Complete Example

Here's how all the pieces fit together:

```typescript
async function runDemo() {
  // Execute user events query
  const userEvents = await getUserEvents({ userId: 'user-123', limit: 50 });
  
  // Execute event counts query
  const eventCounts = await getEventCountsPipe({
    startTime: Date.now() - 24 * 60 * 60 * 1000,
    endTime: Date.now(),
  });
  
  // Ingest new events
  await ingestEvents();
  
  return {
    userEvents: userEvents.data,
    eventCounts: eventCounts.data,
  };
}

runDemo().then(console.log);
```

This workflow provides type safety from schema definition through query execution and data ingestion, making it easy to build reliable analytics applications with Tinybird.