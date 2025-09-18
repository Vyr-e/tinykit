# API Reference

This document provides a detailed reference for the TinyKit API.

## Top-Level Functions

### `defineSchema`

Defines a schema for a Tinybird DataSource.

**Signature:**
```typescript
defineSchema<T extends Record<string, ColumnDefinition>>(schema: T): T
```

**Parameters:**
- `schema`: An object where the keys are the column names and the values are column definitions created with functions like `string()`, `int64()`, etc.

**Example:**
```typescript
import { defineSchema, string, int64 } from 'tinykit';

export const eventsSchema = defineSchema({
  id: string('id'),
  timestamp: int64('timestamp'),
  event_type: string('event_type'),
});
```

### `defineDataSource`

Defines a Tinybird DataSource.

**Signature:**
```typescript
defineDataSource<TSchema extends SchemaDefinition>(
  config: DataSourceConfig<TSchema>
): DataSourceConfig<TSchema>
```

**Parameters:**
- `config`: A configuration object for the DataSource.
  - `name`: The name of the DataSource in Tinybird.
  - `schema`: The schema definition for the DataSource, created with `defineSchema`.
  - `engine`: The table engine to use for the DataSource (e.g., `MergeTree`).
  - `sortingKey`: An array of column names to use as the sorting key.
  - `partitionBy`: The partitioning scheme for the table.
  - `ttl`: The Time-to-Live (TTL) expression for data in the table.
  - `version`: The version of the DataSource, used for migrations.

**Example:**
```typescript
import { defineDataSource } from 'tinykit';
import { eventsSchema } from './schema';

export const eventsDataSource = defineDataSource({
  name: 'events__v1',
  version: 1,
  schema: eventsSchema,
  engine: 'MergeTree',
  sortingKey: ['timestamp', 'id'],
});
```

### `definePipe`

Defines a Tinybird Pipe.

**Signature:**
```typescript
definePipe<TSchema extends SchemaDefinition, TParams extends QueryParameters, const TName extends string>(
  config: {
    name: TName;
    version?: number;
    schema: TSchema;
    parameters: TParams;
  }
): PipeBuilder<TSchema, TParams, TName>
```

**Parameters:**
- `config`: A configuration object for the Pipe.
  - `name`: The name of the Pipe in Tinybird.
  - `version`: The version of the Pipe, used for migrations.
  - `schema`: The schema definition for the primary DataSource used in the pipe.
  - `parameters`: The parameters definition for the Pipe, created with `defineParameters`.

**Example:**
```typescript
import { definePipe, defineParameters, stringParam, query, count, param } from 'tinykit';
import { eventsSchema } from './schema';

const eventCountsParams = defineParameters({
  startTime: int64Param('startTime', { required: true }),
  endTime: int64Param('endTime', { required: true }),
});

export const getEventCountsPipe = definePipe({
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

### `defineParameters`

Defines a set of parameters for a Tinybird Pipe.

**Signature:**
```typescript
defineParameters<T extends QueryParameters>(params: T): T
```

**Parameters:**
- `params`: An object where the keys are the parameter names and the values are parameter definitions created with functions like `stringParam()`, `int64Param()`, etc.

**Example:**
```typescript
import { defineParameters, stringParam, int64Param } from 'tinykit';

export const eventCountsParams = defineParameters({
  userId: stringParam('userId', { required: true }),
  startTime: int64Param('startTime', { required: true }),
  endTime: int64Param('endTime', { required: true }),
  limit: int64Param('limit', { default: 100 }),
});
```

### `query`

Creates a new query builder instance.

**Signature:**
```typescript
query<T extends SchemaDefinition>(schema: T): QueryBuilder<T>
```

**Parameters:**
- `schema`: The schema definition to use for the query.

**Example:**
```typescript
import { query } from 'tinykit';
import { eventsSchema } from './schema';

const q = query(eventsSchema);
```

## Schema Building Functions

These functions are used to define the columns in a DataSource schema.

- `string(name, options)`: Defines a `String` column.
- `int32(name, options)`: Defines an `Int32` column.
- `int64(name, options)`: Defines an `Int64` column.
- `float64(name, options)`: Defines a `Float64` column.
- `boolean(name, options)`: Defines a `Boolean` column.
- `dateTime(name, options)`: Defines a `DateTime64` column.
- `date(name, options)`: Defines a `Date` column.
- `uuid(name, options)`: Defines a `UUID` column.
- `array(name, schema, options)`: Defines an `Array` column. The `innerType` option is required to specify the underlying array type (e.g., `Array(String)`).
- `map(name, keySchema, valueSchema, options)`: Defines a `Map` column.
- `tuple(name, schema, options)`: Defines a `Tuple` column.
- `nested(name, schema, options)`: Defines a `Nested` column.
- `lowCardinality(name, schema, options)`: Defines a `LowCardinality` column. This is useful for columns with a small number of unique values. The `innerType` option is required.
- `nullable(name, baseSchema, options)`: Defines a `Nullable` column. The `innerType` option is required.
- `json(name, options)`: Defines a `JSON` column.
- `ipv4(name, options)`: Defines an `IPv4` column.
- `ipv6(name, options)`: Defines an `IPv6` column.

## Parameter Building Functions

These functions are used to define the parameters for a Pipe.

- `stringParam(name, options)`: Defines a `String` parameter.
  - `name`: Parameter name
  - `options`: `{ required?: boolean; default?: string }`
- `int64Param(name, options)`: Defines an `Int64` parameter.
  - `name`: Parameter name  
  - `options`: `{ required?: boolean; default?: number }`
- `float64Param(name, options)`: Defines a `Float64` parameter.
  - `name`: Parameter name
  - `options`: `{ required?: boolean; default?: number }`
- `dateTimeParam(name, options)`: Defines a `DateTime` parameter.
  - `name`: Parameter name
  - `options`: `{ required?: boolean; default?: string | Date | number }`
- `dateParam(name, options)`: Defines a `Date` parameter.
  - `name`: Parameter name
  - `options`: `{ required?: boolean; default?: string | Date }`
- `booleanParam(name, options)`: Defines a `Boolean` parameter.
  - `name`: Parameter name
  - `options`: `{ required?: boolean; default?: boolean }`
- `enumParam(name, values, options)`: Defines an `Enum` parameter.
  - `name`: Parameter name
  - `values`: Array of string literals (e.g., `['1h', '1d', '1w'] as const`)
  - `options`: `{ required?: boolean; default?: T[number] }`

## Query Builder Functions

The query builder provides a fluent API for building SQL queries.

- `.select(...columns)`: Select specific columns.
- `.selectRaw(sql)`: Raw SQL select.
- `.from(table)`: FROM clause.
- `.where(column, operator)`: WHERE clause.
- `.and(column, operator)`: AND condition.
- `.or(column, operator)`: OR condition.
- `.groupBy(...columns)`: GROUP BY clause.
- `.orderBy(...columns)`: ORDER BY clause.
- `.limit(limit)`: LIMIT clause.
- `.offset(offset)`: OFFSET clause.
- `.join(table, condition, type)`: JOIN clause.
- `.union(query, type)`: UNION clause.
- `.with(alias, query)`: Adds a Common Table Expression (CTE) to the query.
- `.subquery(alias, query)`: Uses a subquery in the FROM clause.
- `.raw(sql)`: Replaces the entire query with a raw SQL string.

### Window Functions

- `rowNumber(partitionBy, orderBy)`: `ROW_NUMBER()` window function.
- `rank(partitionBy, orderBy)`: `RANK()` window function.
- `denseRank(partitionBy, orderBy)`: `DENSE_RANK()` window function.
- `lag(column, offset, defaultValue, partitionBy, orderBy)`: `LAG()` window function.
- `lead(column, offset, defaultValue, partitionBy, orderBy)`: `LEAD()` window function.
- `firstValue(column, partitionBy, orderBy)`: `FIRST_VALUE()` window function.
- `lastValue(column, partitionBy, orderBy)`: `LAST_VALUE()` window function.

### Conditional Aggregation

- `conditional(condition, whenTrue, whenFalse)`: `if(condition, whenTrue, whenFalse)` expression.

## Ingestion Functions

These functions are used to define and perform data ingestion.

- `defineIngest(config)`
- `streamingIngest(config)`
- `syncIngest(config)`
- `batchIngest(config)`
- `robustIngest(config)`

## Client

The `Tinybird` class is the main entry point for interacting with the Tinybird API.

### `new Tinybird(config)`

Creates a new Tinybird client.

**Config:**
- `token`: Your Tinybird authentication token.
- `datasources` (optional): A record of DataSource configurations, keyed by a custom name.
- `pipes` (optional): A record of Pipe configurations, keyed by a custom name.

**Example:**
```typescript
import { Tinybird } from 'tinykit';
import { eventsDataSource } from './datasources';
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

### `.from(name)`

Creates a query builder instance for a specific DataSource that has been defined in the client config.

**Example:**
```typescript
const q = tb.from('events');
```

### `.pipe(config)`

Creates a function to execute a Tinybird Pipe query.

**Example:**
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

const result = await getUserEvents({ userId: 'user-123', limit: 50 });
```

### `.ingest(ingestDef)`

Creates a function to ingest data into a Tinybird DataSource.

**Example:**
```typescript
import { defineIngest } from 'tinykit';

const myIngest = defineIngest({
  datasource: 'events__v2',
  schema: eventsSchema,
});

const ingest = tb.ingest(myIngest);
await ingest([{ /* ... event data ... */ }]);
```
