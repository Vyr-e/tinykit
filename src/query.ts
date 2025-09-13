import type { SchemaDefinition, InferSchemaType, Granularity } from './types';

export type QueryBuilder<T extends SchemaDefinition> = {
  select: <K extends keyof InferSchemaType<T>>(
    ...columns: K[]
  ) => QueryBuilder<T>;

  selectRaw: (sql: string) => QueryBuilder<T>;

  from: (table: string) => QueryBuilder<T>;

  where: (condition: string | ((schema: T) => string)) => QueryBuilder<T>;

  and: (condition: string | ((schema: T) => string)) => QueryBuilder<T>;

  or: (condition: string | ((schema: T) => string)) => QueryBuilder<T>;

  groupBy: <K extends keyof InferSchemaType<T>>(
    ...columns: K[]
  ) => QueryBuilder<T>;

  orderBy: (
    column: string,
    direction?: 'ASC' | 'DESC'
  ) => QueryBuilder<T>;

  limit: (limit: number) => QueryBuilder<T>;

  offset: (offset: number) => QueryBuilder<T>;

  build: () => string;

  _query: QueryState<T>;
};

type QueryState<T extends SchemaDefinition> = {
  select: string[];
  from?: string;
  where: string[];
  groupBy: string[];
  orderBy: string[];
  limit?: number;
  offset?: number;
  schema: T;
};

export function query<T extends SchemaDefinition>(schema: T): QueryBuilder<T> {
  const state: QueryState<T> = {
    select: [],
    where: [],
    groupBy: [],
    orderBy: [],
    schema,
  };

  const builder: QueryBuilder<T> = {
    select(...columns) {
      state.select.push(...columns.map(col => String(col)));
      return builder;
    },

    selectRaw(sql) {
      state.select.push(sql);
      return builder;
    },

    from(table) {
      state.from = table;
      return builder as QueryBuilder<T> & { _from: string };
    },

    where(condition) {
      const condStr = typeof condition === 'function' ? condition(schema) : condition;
      state.where.push(state.where.length === 0 ? condStr : `AND ${condStr}`);
      return builder;
    },

    and(condition) {
      const condStr = typeof condition === 'function' ? condition(schema) : condition;
      state.where.push(`AND ${condStr}`);
      return builder;
    },

    or(condition) {
      const condStr = typeof condition === 'function' ? condition(schema) : condition;
      state.where.push(`OR ${condStr}`);
      return builder;
    },

    groupBy(...columns) {
      state.groupBy.push(...columns.map(col => String(col)));
      return builder;
    },

    orderBy(column, direction = 'ASC') {
      state.orderBy.push(`${column} ${direction}`);
      return builder;
    },

    limit(limit) {
      state.limit = limit;
      return builder;
    },

    offset(offset) {
      state.offset = offset;
      return builder;
    },

    build() {
      const parts: string[] = [];

      if (state.select.length > 0) {
        parts.push(`SELECT ${state.select.join(', ')}`);
      } else {
        parts.push('SELECT *');
      }

      if (state.from) {
        parts.push(`FROM ${state.from}`);
      }

      if (state.where.length > 0) {
        parts.push(`WHERE ${state.where.join(' ')}`);
      }

      if (state.groupBy.length > 0) {
        parts.push(`GROUP BY ${state.groupBy.join(', ')}`);
      }

      if (state.orderBy.length > 0) {
        parts.push(`ORDER BY ${state.orderBy.join(', ')}`);
      }

      if (state.limit !== undefined) {
        parts.push(`LIMIT ${state.limit}`);
      }

      if (state.offset !== undefined) {
        parts.push(`OFFSET ${state.offset}`);
      }

      return parts.join('\n');
    },

    _query: state,
  };

  return builder;
}

export const count = () => 'count()';
export const sum = (column: string) => `sum(${column})`;
export const avg = (column: string) => `avg(${column})`;
export const min = (column: string) => `min(${column})`;
export const max = (column: string) => `max(${column})`;

export const toStartOfMinute = (column: string) => `toStartOfMinute(${column})`;
export const toStartOfHour = (column: string) => `toStartOfHour(${column})`;
export const toStartOfDay = (column: string) => `toStartOfDay(${column})`;
export const toStartOfWeek = (column: string) => `toStartOfWeek(${column})`;
export const toStartOfMonth = (column: string) => `toStartOfMonth(${column})`;

export const fromUnixTimestamp64Milli = (column: string) => `fromUnixTimestamp64Milli(${column})`;
export const toUnixTimestamp64Milli = (column: string) => `toUnixTimestamp64Milli(${column})`;

export function timeGranularity(column: string, granularity: Granularity): string {
  switch (granularity) {
    case '1m':
      return toStartOfMinute(column);
    case '1h':
      return toStartOfHour(column);
    case '1d':
      return toStartOfDay(column);
    case '1w':
      return toStartOfWeek(column);
    case '1M':
      return toStartOfMonth(column);
    default:
      return column;
  }
}

export function conditional(
  condition: string,
  whenTrue: string,
  whenFalse: string
): string {
  return `if(${condition}, ${whenTrue}, ${whenFalse})`;
}

export function param(name: string, type: 'String' | 'Int64' | 'Float64', required = false): string {
  return `{{ ${type}(${name}${required ? ', required=True' : ''}) }}`;
}