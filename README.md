# TinyKit - TypeSafe Functional Query Client for Tinybird

A TypeScript library that provides a functional, composable query API for building typesafe SQL queries, pipes, and data ingestion for Tinybird.

## Features

- **Functional Query Building**: Compose queries using a chainable, functional API
- **Full Type Safety**: End-to-end type safety from schema definition to query results
- **Schema Definition**: Define your data sources with typed schemas and column definitions
- **Pipe Creation**: Build reusable query pipes with parameter validation
- **Data Ingestion**: Multiple ingestion strategies with validation and error handling
- **SQL Generation**: Automatically generate Tinybird-compatible SQL and .datasource files
- **Parameter Validation**: Runtime validation of query parameters using Zod
- **CLI Tools**: Command-line tools for code generation and datasource management

## Documentation

- [**Usage Guide**](./usage.md) - A guide to the main workflows for using TinyKit.
- [**API Reference**](./reference.md) - A detailed reference for the TinyKit API.

## Installation

```bash
bun add zod
# The library is included in this repository
```

## Quick Start

For a detailed guide on how to get started, see the [Usage Guide](./usage.md).

### 1. Define Your Schema and DataSource

```typescript
import { defineSchema, defineDataSource, string, int64 } from 'tinykit';

const eventsSchema = defineSchema({
  id: string('id'),
  userId: string('userId'),
  event: string('event'),
  timestamp: int64('timestamp'),
  properties: string('properties'),
});

const eventsDataSource = defineDataSource({
  name: 'events__v1',
  schema: eventsSchema,
  engine: 'MergeTree',
  sortingKey: ['timestamp', 'userId'],
});
```

### 2. Build Pipes with Parameters

```typescript
import { definePipe, defineParameters, stringParam, int64Param, query, count, param } from 'tinykit';

const getEventsByUser = definePipe({
  name: 'get_events_by_user__v1',
  schema: eventsSchema,
  parameters: defineParameters({
    userId: stringParam('userId', { required: true }),
    limit: int64Param('limit', { default: 100 }),
  }),
}).endpoint((q, params) =>
  query(eventsSchema)
    .select('id', 'userId', 'event', 'timestamp')
    .from('events__v1')
    .where(`userId = ${param('userId', 'String', true)}`)
    .orderBy('timestamp DESC')
    .limit(params.limit)
);
```

### 3. Setup Client and Execute Queries

```typescript
import { Tinybird } from 'tinykit';
import { z } from 'zod';

const tb = new Tinybird({
  token: process.env.TINYBIRD_TOKEN,
  datasources: { events: eventsDataSource },
  pipes: { getEventsByUser },
});

const getUserEvents = tb.pipe({
  pipe: 'get_events_by_user__v1',
  data: z.object({
    id: z.string(),
    userId: z.string(),
    event: z.string(),
    timestamp: z.number(),
  }),
});

const result = await getUserEvents({ userId: 'user-123' });
console.log(result.data); // Array of user events
```

### 4. Data Ingestion

```typescript
import { defineIngest } from 'tinykit';

const eventsIngest = defineIngest({
  datasource: 'events__v1',
  schema: eventsSchema,
});

const ingest = tb.ingest(eventsIngest);
await ingest([
  {
    id: 'evt-1',
    userId: 'user-456',
    event: 'page_view',
    timestamp: Date.now(),
    properties: JSON.stringify({ page: '/home' }),
  },
]);
```

## Core APIs

### Schema Column Types

TinyKit supports all major ClickHouse/Tinybird column types:

```typescript
import { string, int32, int64, float64, boolean, dateTime, date, uuid, array, map, tuple, nested, lowCardinality, nullable, json, ipv4, ipv6 } from 'tinykit';

const schema = defineSchema({
  id: string('id'),
  count: int64('count'),
  revenue: float64('revenue'),
  active: boolean('active'),
  createdAt: dateTime('createdAt'),
  tags: array('tags', z.string(), { innerType: 'String' }),
  metadata: json('metadata'),
  status: lowCardinality('status', z.string(), { innerType: 'String' }),
  optionalField: nullable('optionalField', z.string(), { innerType: 'String' }),
});
```

### Query Functions

Build complex queries with type-safe functions:

```typescript
import { count, sum, avg, min, max, timeGranularity, fromUnixTimestamp64Milli, conditional, rowNumber, lag, firstValue } from 'tinykit';

const analyticsQuery = query(schema)
  .selectRaw(`
    ${timeGranularity(fromUnixTimestamp64Milli('timestamp'), '1h')} as hour,
    ${count()} as event_count,
    ${sum('revenue')} as total_revenue,
    ${avg('revenue')} as avg_revenue
  `)
  .from('events__v1')
  .groupBy('hour')
  .orderBy('hour');
```

### Parameter Types

Define typed parameters for your pipes:

```typescript
import { stringParam, int64Param, float64Param, dateTimeParam, booleanParam, enumParam } from 'tinykit';

const params = defineParameters({
  userId: stringParam('userId', { required: true }),
  limit: int64Param('limit', { default: 100 }),
  minRevenue: float64Param('minRevenue', { default: 0.0 }),
  active: booleanParam('active', { default: true }),
  period: enumParam('period', ['1h', '1d', '1w'] as const, { default: '1d' }),
});
```

## Examples

See the `examples` directory for complete working examples:

- `examples/events.ts` - Simple event tracking with basic analytics
- Complete schema definitions, pipe creation, and data ingestion

To run examples:

```bash
bun run examples/events.ts
```

## Development

To install dependencies:

```bash
bun install
```

To run tests:

```bash
bun test
```

To build:

```bash
bun run build
```