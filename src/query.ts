import type { SchemaDefinition, InferSchemaType, Granularity } from './types';

/**
 * Represents a SQL operator and its value for a WHERE clause.
 */
export type Operator = { op: string; value: any };

/**
 * Creates an equality (=) operator.
 *
 * @example
 * ```
 * query(mySchema).where('id', eq(1))
 * ```
 */
export const eq = (value: any): Operator => ({ op: '=', value });
/**
 * Creates an inequality (!=) operator.
 *
 * @example
 * ```
 * query(mySchema).where('id', neq(1))
 * ```
 */
export const neq = (value: any): Operator => ({ op: '!=', value });
/**
 * Creates a greater than (>) operator.
 *
 * @example
 * ```
 * query(mySchema).where('value', gt(10))
 * ```
 */
export const gt = (value: any): Operator => ({ op: '>', value });
/**
 * Creates a greater than or equal to (>=) operator.
 *
 * @example
 * ```
 * query(mySchema).where('value', gte(10))
 * ```
 */
export const gte = (value: any): Operator => ({ op: '>=', value });
/**
 * Creates a less than (<) operator.
 *
 * @example
 * ```
 * query(mySchema).where('value', lt(10))
 * ```
 */
export const lt = (value: any): Operator => ({ op: '<', value });
/**
 * Creates a less than or equal to (<=) operator.
 *
 * @example
 * ```
 * query(mySchema).where('value', lte(10))
 * ```
 */
export const lte = (value: any): Operator => ({ op: '<=', value });

/**
 * The main query builder interface.
 * @template T The schema definition for the primary DataSource.
 */
export type QueryBuilder<T extends SchemaDefinition> = {
  /**
   * Adds columns to the SELECT clause.
   * @param columns The names of the columns to select.
   *
   * @example
   * ```
   * query(mySchema).select('id', 'name')
   * ```
   */
  select: <K extends keyof InferSchemaType<T>>(
    ...columns: K[]
  ) => QueryBuilder<T>;

  /**
   * Adds a raw SQL string to the SELECT clause.
   * @param sql The raw SQL string.
   *
   * @example
   * ```
   * query(mySchema).selectRaw('count(*) as count')
   * ```
   */
  selectRaw: (sql: string) => QueryBuilder<T>;

  /**
   * Adds a Common Table Expression (CTE) to the query.
   * @param alias The alias for the CTE.
   * @param query The subquery for the CTE, as a query builder instance or a raw string.
   *
   * @example
   * ```
   * const sub = query(mySchema).select('id').where('name', eq('test'));
   * query(mySchema).with('my_cte', sub).selectRaw('*').from('my_cte')
   * ```
   */
  with: (alias: string, query: QueryBuilder<any> | string) => QueryBuilder<T>;

  /**
   * Sets the FROM clause of the query.
   * @param table The name of the table to select from.
   *
   * @example
   * ```
   * query(mySchema).from('my_table')
   * ```
   */
  from: (table: string) => QueryBuilder<T>;

  /**
   * Adds a WHERE clause to the query.
   * @param args Either a raw string for the condition, or a column name and an operator.
   *
   * @example
   * ```
   * query(mySchema).where('id', eq(1))
   * ```
   */
  where: <K extends keyof InferSchemaType<T>>(
    ...args: [K, Operator] | [string]
  ) => QueryBuilder<T>;

  /**
   * Adds an AND condition to the WHERE clause.
   * @param args Either a raw string for the condition, or a column name and an operator.
   *
   * @example
   * ```
   * query(mySchema).where('id', eq(1)).and('name', eq('test'))
   * ```
   */
  and: <K extends keyof InferSchemaType<T>>(
    ...args: [K, Operator] | [string]
  ) => QueryBuilder<T>;

  /**
   * Adds an OR condition to the WHERE clause.
   * @param args Either a raw string for the condition, or a column name and an operator.
   *
   * @example
   * ```
   * query(mySchema).where('id', eq(1)).or('id', eq(2))
   * ```
   */
  or: <K extends keyof InferSchemaType<T>>(
    ...args: [K, Operator] | [string]
  ) => QueryBuilder<T>;

  /**
   * Adds a JOIN clause to the query.
   * @param table The table to join with.
   * @param condition The join condition.
   * @param type The type of join (e.g., INNER, LEFT).
   *
   * @example
   * ```
   * query(mySchema).join('other_table', 'my_table.id = other_table.id', 'LEFT')
   * ```
   */
  join: (
    table: string,
    condition: string,
    type?: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL'
  ) => QueryBuilder<T>;

  /**
   * Adds a GROUP BY clause to the query.
   * @param columns The columns to group by.
   *
   * @example
   * ```
   * query(mySchema).groupBy('name')
   * ```
   */
  groupBy: <K extends keyof InferSchemaType<T>>(
    ...columns: (K | string)[]
  ) => QueryBuilder<T>;

  /**
   * Adds a HAVING clause to the query.
   * @param condition The having condition.
   *
   * @example
   * ```
   * query(mySchema).groupBy('name').having('count(*) > 1')
   * ```
   */
  having: (condition: string) => QueryBuilder<T>;

  /**
   * Adds a UNION clause to the query.
   * @param query The query to unite with.
   * @param type The type of union.
   *
   * @example
   * ```
   * const q1 = query(mySchema).where('id', eq(1));
   * const q2 = query(mySchema).where('id', eq(2));
   * q1.union(q2)
   * ```
   */
  union: (query: QueryBuilder<T>, type?: 'UNION' | 'UNION ALL' | 'INTERSECT' | 'EXCEPT') => QueryBuilder<T>;

  /** Adds a UNION ALL clause to the query. */
  unionAll: (query: QueryBuilder<T>) => QueryBuilder<T>;

  /** Adds a UNION DISTINCT clause to the query. */
  unionDistinct: (query: QueryBuilder<T>) => QueryBuilder<T>;

  /** Adds an INTERSECT clause to the query. */
  intersect: (query: QueryBuilder<T>) => QueryBuilder<T>;

  /** Adds an EXCEPT clause to the query. */
  except: (query: QueryBuilder<T>) => QueryBuilder<T>;

  /**
   * Uses a subquery in the FROM clause.
   * @param alias The alias for the subquery.
   * @param query The subquery.
   *
   * @example
   * ```
   * const sub = query(mySchema).select('id').where('name', eq('test'));
   * query(mySchema).subquery('my_sub', sub)
   * ```
   */
  subquery: (alias: string, query: QueryBuilder<any>) => QueryBuilder<T>;

  /** Creates an EXISTS subquery string. */
  existsSubquery: (query: QueryBuilder<any>) => string;

  /** Creates a NOT EXISTS subquery string. */
  notExistsSubquery: (query: QueryBuilder<any>) => string;

  /** Creates an IN subquery string. */
  inSubquery: <K extends keyof InferSchemaType<T>>(column: K, query: QueryBuilder<any>) => string;

  /** Creates a NOT IN subquery string. */
  notInSubquery: <K extends keyof InferSchemaType<T>>(column: K, query: QueryBuilder<any>) => string;

  /**
   * Adds an ORDER BY clause to the query.
   * @param columns The columns to order by, optionally with direction (e.g., 'column_name DESC').
   *
   * @example
   * ```
   * query(mySchema).orderBy('name DESC')
   * ```
   */
  orderBy: <K extends keyof InferSchemaType<T>>(
    ...columns: (K | string)[]
  ) => QueryBuilder<T>;

  /**
   * Adds a LIMIT clause to the query.
   * @param limit The maximum number of rows to return.
   *
   * @example
   * ```
   * query(mySchema).limit(10)
   * ```
   */
  limit: (limit: number) => QueryBuilder<T>;

  /**
   * Adds an OFFSET clause to the query.
   * @param offset The number of rows to skip.
   *
   * @example
   * ```
   * query(mySchema).offset(10)
   * ```
   */
  offset: (offset: number) => QueryBuilder<T>;

  /**
   * Conditionally applies a chain of query builder methods.
   * @param condition The condition to check.
   * @param chain A function that receives the query builder and applies methods to it.
   *
   * @example
   * ```
   * query(mySchema).if(true, qb => qb.limit(1))
   * ```
   */
  if: (
    condition: any,
    chain: (qb: QueryBuilder<T>) => QueryBuilder<T>
  ) => QueryBuilder<T>;

  /**
   * Replaces the entire query with a raw SQL string.
   * @param sql The raw SQL string.
   *
   * @example
   * ```
   * query(mySchema).raw('SELECT * FROM my_table')
   * ```
   */
  raw: (sql: string) => QueryBuilder<T>;

  /**
   * Builds the final SQL string.
   * @returns The generated SQL string.
   */
  build: () => string;

  /** @internal */
  _query: QueryState<T>;
};

type QueryState<T extends SchemaDefinition> = {
  select: string[];
  from?: string;
  joins: string[];
  where: string[];
  groupBy: string[];
  having: string[];
  unions: Array<{ query: QueryBuilder<T>; type: 'UNION' | 'UNION ALL' | 'INTERSECT' | 'EXCEPT' }>;
  orderBy: string[];
  limit?: number;
  offset?: number;
  schema: T;
  ctes: { alias: string; sql: string }[];
  rawSql?: string;
};

/**
 * Creates a new query builder instance.
 * @param schema The schema definition to use for the query.
 * @returns A new QueryBuilder instance.
 *
 * @example
 * ```
 * const q = query(mySchema)
 * ```
 */
export function query<T extends SchemaDefinition>(schema: T): QueryBuilder<T> {
  const state: QueryState<T> = {
    select: [],
    where: [],
    groupBy: [],
    orderBy: [],
    joins: [],
    having: [],
    unions: [],
    schema,
    ctes: [],
    rawSql: undefined,
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

    with(alias, query) {
      const sql = typeof query === 'string' ? query : query.build();
      state.ctes.push({ alias, sql });
      return builder;
    },

    from(table) {
      state.from = table;
      return builder as QueryBuilder<T> & { _from: string };
    },

    where(...args) {
      let condStr: string;
      if (args.length === 1) {
        condStr = args[0];
      } else {
        const [column, operator] = args;
        condStr = `${String(column)} ${operator.op} ${escapeValue(operator.value)}`;
      }

      state.where.push(state.where.length === 0 ? condStr : `AND ${condStr}`);
      return builder;
    },

    and(...args) {
      let condStr: string;
      if (args.length === 1) {
        condStr = args[0];
      } else {
        const [column, operator] = args;
        condStr = `${String(column)} ${operator.op} ${escapeValue(operator.value)}`;
      }

      state.where.push(`AND ${condStr}`);
      return builder;
    },

    or(...args) {
      let condStr: string;
      if (args.length === 1) {
        condStr = args[0];
      } else {
        const [column, operator] = args;
        condStr = `${String(column)} ${operator.op} ${escapeValue(operator.value)}`;
      }

      state.where.push(`OR ${condStr}`);
      return builder;
    },

    join(table, condition, type = 'INNER') {
      state.joins.push(`${type} JOIN ${table} ON ${condition}`);
      return builder;
    },

    groupBy(...columns) {
      state.groupBy.push(...columns.map(col => String(col)));
      return builder;
    },

    having(condition) {
      state.having.push(condition);
      return builder;
    },

    union(query, type = 'UNION ALL') {
      state.unions.push({ query, type });
      return builder;
    },

    unionAll(query) {
      return builder.union(query, 'UNION ALL');
    },

    unionDistinct(query) {
      return builder.union(query, 'UNION');
    },

    intersect(query) {
      return builder.union(query, 'INTERSECT');
    },

    except(query) {
      return builder.union(query, 'EXCEPT');
    },

    subquery(alias, query) {
      const subquerySQL = `(${query.build()}) AS ${alias}`;
      state.from = subquerySQL;
      return builder;
    },

    existsSubquery(query) {
      return `EXISTS (${query.build()})`;
    },

    notExistsSubquery(query) {
      return `NOT EXISTS (${query.build()})`;
    },

    inSubquery(column, query) {
      return `${String(column)} IN (${query.build()})`;
    },

    notInSubquery(column, query) {
      return `${String(column)} NOT IN (${query.build()})`;
    },

    orderBy(...columns) {
      state.orderBy.push(...columns.map(col => String(col)));
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

    if(condition, chain) {
      if (condition) {
        return chain(builder);
      }
      return builder;
    },

    raw(sql) {
      state.rawSql = sql;
      return builder;
    },

    build() {
      if (state.rawSql) {
        return state.rawSql;
      }

      let withClause = '';
      if (state.ctes.length > 0) {
        const withClauses = state.ctes.map(
          (cte) => `${cte.alias} AS (${cte.sql})`
        );
        withClause = `WITH ${withClauses.join(',\n')}\n`;
      }

      const parts: string[] = [];

      if (state.select.length > 0) {
        parts.push(`SELECT ${state.select.join(', ')}`);
      } else {
        parts.push('SELECT *');
      }

      if (state.from) {
        parts.push(`FROM ${state.from}`);
      }

      if (state.joins.length > 0) {
        parts.push(state.joins.join('\n'));
      }

      if (state.where.length > 0) {
        parts.push(`WHERE ${state.where.join(' ')}`);
      }

      if (state.groupBy.length > 0) {
        parts.push(`GROUP BY ${state.groupBy.join(', ')}`);
      }

      if (state.having.length > 0) {
        parts.push(`HAVING ${state.having.join(' AND ')}`);
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

      let mainSql = parts.join('\n');

      if (state.unions.length > 0) {
        const unionParts = state.unions.map(union => {
          return `${union.type}\n${union.query.build()}`;
        });
        mainSql = `${mainSql}\n${unionParts.join('\n')}`;
      }

      return withClause + mainSql;
    },

    _query: state,
  };

  return builder;
}

/** Returns a `count()` aggregation. */
export const count = () => 'count()';
/** Returns a `sum(column)` aggregation. */
export const sum = (column: string) => `sum(${column})`;
/** Returns an `avg(column)` aggregation. */
export const avg = (column: string) => `avg(${column})`;
/** Returns a `min(column)` aggregation. */
export const min = (column: string) => `min(${column})`;
/** Returns a `max(column)` aggregation. */
export const max = (column: string) => `max(${column})`;

/** Returns a `toStartOfMinute(column)` function. */
export const toStartOfMinute = (column: string) => `toStartOfMinute(${column})`;
/** Returns a `toStartOfHour(column)` function. */
export const toStartOfHour = (column: string) => `toStartOfHour(${column})`;
/** Returns a `toStartOfDay(column)` function. */
export const toStartOfDay = (column: string) => `toStartOfDay(${column})`;
/** Returns a `toStartOfWeek(column)` function. */
export const toStartOfWeek = (column: string) => `toStartOfWeek(${column})`;
/** Returns a `toStartOfMonth(column)` function. */
export const toStartOfMonth = (column: string) => `toStartOfMonth(${column})`;

/** Returns a `fromUnixTimestamp64Milli(column)` function. */
export const fromUnixTimestamp64Milli = (column: string) => `fromUnixTimestamp64Milli(${column})`;
/** Returns a `toUnixTimestamp64Milli(column)` function. */
export const toUnixTimestamp64Milli = (column: string) => `toUnixTimestamp64Milli(${column})`;

/**
 * Returns a time granularity function based on the provided granularity.
 * @param column The column to apply the granularity to.
 * @param granularity The desired granularity.
 */
export function timeGranularity(column: string, granularity: Granularity | string): string {
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

/**
 * Creates a conditional `if(condition, whenTrue, whenFalse)` expression.
 */
export function conditional(
  condition: string,
  whenTrue: string,
  whenFalse: string
): string {
  return `if(${condition}, ${whenTrue}, ${whenFalse})`;
}

/**
 * Creates a Tinybird parameter placeholder.
 * @param name The name of the parameter.
 * @param type The type of the parameter.
 * @param required Whether the parameter is required.
 */
export function param(name: string, type: 'String' | 'Int64' | 'Float64' | 'DateTime' | 'Date' | 'Boolean', required = false): string {
  return `{{ ${type}(${name}${required ? ', required=True' : ''}) }}`;
}

/** Creates a `ROW_NUMBER()` window function. */
export function rowNumber(partitionBy?: string, orderBy?: string): string {
  let windowClause = 'ROW_NUMBER() OVER (';
  
  const clauses: string[] = [];
  if (partitionBy) clauses.push(`PARTITION BY ${partitionBy}`);
  if (orderBy) clauses.push(`ORDER BY ${orderBy}`);
  
  windowClause += clauses.join(' ') + ')';
  return windowClause;
}

/** Creates a `RANK()` window function. */
export function rank(partitionBy?: string, orderBy?: string): string {
  let windowClause = 'RANK() OVER (';
  
  const clauses: string[] = [];
  if (partitionBy) clauses.push(`PARTITION BY ${partitionBy}`);
  if (orderBy) clauses.push(`ORDER BY ${orderBy}`);
  
  windowClause += clauses.join(' ') + ')';
  return windowClause;
}

/** Creates a `DENSE_RANK()` window function. */
export function denseRank(partitionBy?: string, orderBy?: string): string {
  let windowClause = 'DENSE_RANK() OVER (';
  
  const clauses: string[] = [];
  if (partitionBy) clauses.push(`PARTITION BY ${partitionBy}`);
  if (orderBy) clauses.push(`ORDER BY ${orderBy}`);
  
  windowClause += clauses.join(' ') + ')';
  return windowClause;
}

/** Creates a `LAG()` window function. */
export function lag(column: string, offset = 1, defaultValue?: string, partitionBy?: string, orderBy?: string): string {
  let windowClause = `LAG(${column}`;
  if (offset !== 1) windowClause += `, ${offset}`;
  if (defaultValue !== undefined) windowClause += `, ${defaultValue}`;
  windowClause += ') OVER (';
  
  const clauses: string[] = [];
  if (partitionBy) clauses.push(`PARTITION BY ${partitionBy}`);
  if (orderBy) clauses.push(`ORDER BY ${orderBy}`);
  
  windowClause += clauses.join(' ') + ')';
  return windowClause;
}

/** Creates a `LEAD()` window function. */
export function lead(column: string, offset = 1, defaultValue?: string, partitionBy?: string, orderBy?: string): string {
  let windowClause = `LEAD(${column}`;
  if (offset !== 1) windowClause += `, ${offset}`;
  if (defaultValue !== undefined) windowClause += `, ${defaultValue}`;
  windowClause += ') OVER (';
  
  const clauses: string[] = [];
  if (partitionBy) clauses.push(`PARTITION BY ${partitionBy}`);
  if (orderBy) clauses.push(`ORDER BY ${orderBy}`);
  
  windowClause += clauses.join(' ') + ')';
  return windowClause;
}

/** Creates a `FIRST_VALUE()` window function. */
export function firstValue(column: string, partitionBy?: string, orderBy?: string): string {
  let windowClause = `FIRST_VALUE(${column}) OVER (`;
  
  const clauses: string[] = [];
  if (partitionBy) clauses.push(`PARTITION BY ${partitionBy}`);
  if (orderBy) clauses.push(`ORDER BY ${orderBy}`);
  
  windowClause += clauses.join(' ') + ')';
  return windowClause;
}

/** Creates a `LAST_VALUE()` window function. */
export function lastValue(column: string, partitionBy?: string, orderBy?: string): string {
  let windowClause = `LAST_VALUE(${column}) OVER (`;
  
  const clauses: string[] = [];
  if (partitionBy) clauses.push(`PARTITION BY ${partitionBy}`);
  if (orderBy) clauses.push(`ORDER BY ${orderBy}`);
  
  windowClause += clauses.join(' ') + ')';
  return windowClause;
}

/**
 * @internal
 */
function escapeValue(value: any): string {
  if (value === null) return 'NULL';
  if (typeof value === 'number') return String(value);
  if (typeof value === 'boolean') return value ? '1' : '0';
  if (typeof value === 'string') {
    // Heuristic: If the string contains characters common in SQL functions or parameters,
    // assume it's a safe expression and don't quote it.
    if (/[(){}]/.test(value)) {
      return value;
    }
    // Otherwise, treat it as a literal string and escape it.
    return `'${value.replace(/'/g, "''")}'`;
  }
  throw new Error(`Unsupported value type for escaping: ${typeof value}`);
}
