import { z } from 'zod';
import type {
  QueryParameters,
  InferParametersType,
  SchemaDefinition,
  InferSchemaType,
  DataSourceConfig,
} from './types';
import {
  type PipeErrorResponse,
  eventIngestReponseData,
  pipeResponseWithoutData,
} from './util';
import {
  defineIngest,
  type IngestDefinition,
  type IngestResult,
} from './ingest';
import { query, QueryBuilder } from './query';

export class TinybirdError extends Error {
  public override readonly cause?: unknown;

  constructor(message: string, cause?: unknown) {
    super(message);
    this.name = 'TinybirdError';
    this.cause = cause;
  }
}

export class TinybirdTimeoutError extends TinybirdError {
  constructor(timeout: number) {
    super(`Request timed out after ${timeout}ms`);
    this.name = 'TinybirdTimeoutError';
  }
}

export class TinybirdUnauthorizedError extends TinybirdError {
  constructor() {
    super('Unauthorized - check your Tinybird token');
    this.name = 'TinybirdUnauthorizedError';
  }
}

export class TinybirdRetryExhaustedError extends TinybirdError {
  constructor(attempts: number, lastError?: unknown) {
    super(`Request failed after ${attempts} attempts`, lastError);
    this.name = 'TinybirdRetryExhaustedError';
  }
}

export class TinybirdValidationError extends TinybirdError {
  constructor(message: string, public readonly validationErrors?: unknown) {
    super(message);
    this.name = 'TinybirdValidationError';
  }
}

/**
 * A unique symbol to identify Tinybird client instances.
 * @internal
 */
export const isTinybirdClientSymbol = Symbol.for('isTinybirdClient');

/**
 * Internal symbols for CLI analyzer access to client configuration.
 * These allow the CLI to access datasources and pipes without exposing them publicly.
 * @internal
 */
export const datasourcesSymbol = Symbol.for('tinybirdDatasources');
export const pipesSymbol = Symbol.for('tinybirdPipes');

type RequestCache =
  | 'default'
  | 'no-store'
  | 'reload'
  | 'no-cache'
  | 'force-cache'
  | 'only-if-cached';

export type Config<
  TDatasources extends Record<string, DataSourceConfig<any>>,
  TPipes extends Record<string, any> = {}
> = {
  datasources?: TDatasources;
  pipes?: TPipes;
  baseUrl?: string;
} & (
  | {
      token: string;
      noop?: never;
    }
  | {
      token?: never;
      noop: true;
    }
);

type ExtractPipeNames<T extends Record<string, any>> = {
  [K in keyof T]: T[K] extends { name: infer N extends string } ? N : never;
}[keyof T];

type FindPipeByName<
  TName extends string,
  TPipes extends Record<string, any>
> = {
  [K in keyof TPipes]: TPipes[K] extends { name: TName } ? TPipes[K] : never;
}[keyof TPipes];

type ExtractDatasourceNames<T extends Record<string, any>> = {
  [K in keyof T]: T[K] extends { name: infer N extends string } ? N : never;
}[keyof T];

type FindDatasourceByName<
  TName extends string,
  TDatasources extends Record<string, any>
> = TDatasources[keyof TDatasources] & { name: TName };

/**
 * The main Tinybird client for interacting with your DataSources and Pipes.
 *
 * @example
 * ```
 * import { Tinybird } from '@tinybird/sdk';
 *
 * const tb = new Tinybird({
 *   token: 'YOUR_TOKEN',
 *   datasources: {
 *     my_datasource: {
 *       name: 'my_datasource',
 *       schema: z.object({
 *         id: z.string(),
 *         name: z.string(),
 *       }),
 *     },
 *   },
 * });
 * ```
 */
export class Tinybird<
  TDatasources extends Record<string, DataSourceConfig<any>>,
  TPipes extends Record<string, any> = {}
> {
  private readonly baseUrl: string;
  public readonly token: string;
  public readonly noop: boolean;
  private readonly datasources: TDatasources | undefined;
  private readonly pipes: TPipes | undefined;
  public readonly [isTinybirdClientSymbol] = true;

  // Symbol-keyed properties for CLI analyzer access only
  public readonly [datasourcesSymbol]: TDatasources | undefined;
  public readonly [pipesSymbol]: TPipes | undefined;

  constructor(config: Config<TDatasources, TPipes>) {
    this.baseUrl = config.baseUrl ?? 'https://api.tinybird.co';
    this.datasources = config.datasources;
    this.pipes = config.pipes;

    // Store config in symbol-keyed properties for CLI analyzer access
    this[datasourcesSymbol] = config.datasources;
    this[pipesSymbol] = config.pipes;

    if (config.noop) {
      this.token = '';
      this.noop = true;
    } else {
      this.token = config.token;
      this.noop = false;
    }
  }

  /**
   * Creates a query builder instance for a specific DataSource.
   *
   * @param name The key of the DataSource in the client's `datasources` configuration.
   * @returns A QueryBuilder instance.
   *
   * @example
   * ```
   * const result = await tb.from('my_datasource').select('*').where('id', '=', '1').fetch();
   * ```
   */
  from<TName extends keyof TDatasources>(
    name: TName
  ): QueryBuilder<TDatasources[TName]['schema']> {
    const datasource = this.datasources?.[name];
    if (!datasource) {
      throw new Error(`Unknown datasource: ${String(name)}`);
    }

    return query(datasource.schema).from(datasource.name);
  }

  private async fetchWithTimeout(
    url: string | URL,
    opts: {
      method: string;
      headers?: Record<string, string>;
      body?: string;
      cache?: RequestCache;
      next?: {
        revalidate?: number;
      };
    },
    timeout: number = 30000
  ): Promise<Response> {
    // Check if AbortController is available
    if (typeof AbortController === 'undefined') {
      // Fallback to simple timeout without cancellation
      const timeoutPromise = new Promise<never>((_, reject) => {
        setTimeout(() => reject(new TinybirdTimeoutError(timeout)), timeout);
      });

      const fetchPromise = fetch(url, opts);
      return Promise.race([fetchPromise, timeoutPromise]);
    }

    const controller = new AbortController();
    const timeoutId = setTimeout(() => (controller as any).abort(), timeout);

    try {
      const response = await fetch(url, {
        ...opts,
        signal: (controller as any).signal,
      });
      clearTimeout(timeoutId);
      return response;
    } catch (error) {
      clearTimeout(timeoutId);
      if (error instanceof Error && error.name === 'AbortError') {
        throw new TinybirdTimeoutError(timeout);
      }
      throw error;
    }
  }

  private isRetryableStatus(status: number): boolean {
    if (status === 429) return true;
    if (status >= 500) {
      // Avoid retrying client errors that won't succeed
      const nonRetryable = [501, 505, 511]; // Not Implemented, HTTP Version Not Supported, Network Authentication Required
      return !nonRetryable.includes(status);
    }
    return false;
  }

  private async fetch(
    url: string | URL,
    opts: {
      method: string;
      headers?: Record<string, string>;
      body?: string;
      cache?: RequestCache;
      next?: {
        revalidate?: number;
      };
    }
  ): Promise<unknown> {
    const maxAttempts = 10;
    let lastError: unknown;

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      try {
        const res = await this.fetchWithTimeout(url, opts);

        if (res.ok) {
          return res.json();
        }

        if (res.status === 403) {
          throw new TinybirdUnauthorizedError();
        }

        if (this.isRetryableStatus(res.status)) {
          const backoffMs = 1000 + attempt ** 2 * 50;
          await new Promise((resolve) => setTimeout(resolve, backoffMs));
          continue;
        }

        // Non-retryable error - parse error response and throw immediately
        try {
          const errorBody = (await res.json()) as PipeErrorResponse;
          throw new TinybirdError(
            errorBody.error || `HTTP ${res.status}: ${res.statusText}`
          );
        } catch (parseError) {
          throw new TinybirdError(`HTTP ${res.status}: ${res.statusText}`);
        }
      } catch (error) {
        lastError = error;

        // Don't retry non-retryable errors
        if (
          error instanceof TinybirdUnauthorizedError ||
          error instanceof TinybirdError ||
          error instanceof TinybirdValidationError
        ) {
          throw error;
        }

        // Retry timeouts and network errors
        if (attempt === maxAttempts - 1) {
          break;
        }

        const backoffMs = 1000 + attempt ** 2 * 50;
        await new Promise((resolve) => setTimeout(resolve, backoffMs));
      }
    }

    throw new TinybirdRetryExhaustedError(maxAttempts, lastError);
  }

  /**
   * Creates a type-safe function to execute a Tinybird Pipe query.
   *
   * @param req The request configuration, including the pipe name and a Zod schema for the response data.
   * @returns An async function that takes the pipe parameters and returns the query result.
   *
   * @example
   * ```
   * const analytics = tb.pipe({
   *   pipe: 'get_daily_activity',
   *   data: z.object({
   *     time_bucket: z.string(),
   *     event_count: z.number(),
   *   }),
   * });
   *
   * const result = await analytics({ tenantId: 'my-tenant' });
   * ```
   */
  public pipe<
    TName extends ExtractPipeNames<TPipes>,
    TData extends z.ZodSchema<any>
  >(req: {
    pipe: TName;
    data: TData;
    parameters?: any;
    opts?: {
      cache?: RequestCache;
      next?: {
        revalidate?: number;
      };
    };
  }): FindPipeByName<TName, TPipes> extends { parameters: infer TParams }
    ? TParams extends QueryParameters
      ? (
          params: InferParametersType<TParams>
        ) => Promise<{ data: z.output<TData>[]; meta: any[]; statistics?: any }>
      : () => Promise<{
          data: z.output<TData>[];
          meta: any[];
          statistics?: any;
        }>
    : () => Promise<{ data: z.output<TData>[]; meta: any[]; statistics?: any }>;

  public pipe<
    TData extends z.ZodSchema<any>,
    TParameters extends QueryParameters | z.ZodSchema<any> = any
  >(req: {
    pipe: string;
    parameters: TParameters;
    data: TData;
    opts?: {
      cache?: RequestCache;
      next?: {
        revalidate?: number;
      };
    };
  }): TParameters extends QueryParameters
    ? (
        params: InferParametersType<TParameters>
      ) => Promise<{ data: z.output<TData>[]; meta: any[]; statistics?: any }>
    : TParameters extends z.ZodSchema<any>
    ? (
        params: z.input<TParameters>
      ) => Promise<{ data: z.output<TData>[]; meta: any[]; statistics?: any }>
    : () => Promise<{ data: z.output<TData>[]; meta: any[]; statistics?: any }>;

  public pipe<
    TData extends z.ZodSchema<any>,
    TParameters extends QueryParameters | z.ZodSchema<any> = any
  >(req: {
    pipe: string;
    parameters?: TParameters;
    data: TData;
    opts?: {
      cache?: RequestCache;
      next?: {
        revalidate?: number;
      };
    };
  }) {
    return async (params: any) => {
      let validatedParams: any = undefined;
      let pipeDefinition: any;

      if (typeof req.pipe === 'string' && this.pipes) {
        pipeDefinition = Object.values(this.pipes).find(
          (p) => (p as any).name === req.pipe
        );
      }

      if (pipeDefinition?.parameters) {
        validatedParams = this.validateParameters(
          pipeDefinition.parameters,
          params || {}
        );
      } else if (req.parameters) {
        if (this.isQueryParameters(req.parameters)) {
          const zodSchema = createZodSchemaFromParameters(req.parameters);
          const v = zodSchema.safeParse(params);
          if (!v.success)
            throw new TinybirdValidationError(
              `Parameter validation failed: ${v.error.message}`,
              v.error
            );
          validatedParams = v.data;
        } else {
          const v = (req.parameters as z.ZodSchema<any>).safeParse(params);
          if (!v.success)
            throw new TinybirdValidationError(
              `Parameter validation failed: ${v.error.message}`,
              v.error
            );
          validatedParams = v.data;
        }
      }

      if (this.noop) {
        return { data: [], meta: [] };
      }

      const url = new URL(
        `/v0/pipes/${pipeDefinition.name}.json`,
        this.baseUrl
      );
      if (validatedParams) {
        for (const [key, value] of Object.entries(validatedParams)) {
          if (typeof value === 'undefined' || value === null) {
            continue;
          }
          url.searchParams.set(key, value.toString());
        }
      }

      const res = await this.fetch(url, {
        ...req.opts,
        method: 'GET',
        headers: { Authorization: `Bearer ${this.token}` },
      });

      const outputSchema = pipeResponseWithoutData.setKey(
        'data',
        z.array(req.data)
      );
      const validatedResponse = outputSchema.safeParse(res);
      if (!validatedResponse.success) {
        throw new TinybirdValidationError(
          `Response validation failed: ${validatedResponse.error.message}`,
          validatedResponse.error
        );
      }

      return validatedResponse.data;
    };
  }

  /**
   * Creates a function to ingest data into a Tinybird DataSource.
   *
   * @param ingestDef The ingestion definition created with `defineIngest`.
   * @returns An async function that takes an array of events and sends them to the Tinybird Events API.
   *
   * @example
   * ```
   * import { Tinybird, defineIngest } from '@tinybird/sdk';
   * import { z } from 'zod';
   *
   * const tb = new Tinybird({ token: 'YOUR_TOKEN' });
   *
   * const eventsSchema = z.object({
   *  id: z.string(),
   *  name: z.string(),
   * });
   *
   * const eventsIngest = defineIngest({ datasource: 'my_events', schema: eventsSchema });
   * const ingest = tb.ingest(eventsIngest);
   * await ingest([{ id: '1', name: 'test' }]);
   * ```
   */
  public ingest<
    TDatasourceName extends ExtractDatasourceNames<TDatasources>
  >(req: {
    datasource: TDatasourceName;
    wait?: boolean;
  }): (
    events:
      | InferSchemaType<
          FindDatasourceByName<TDatasourceName, TDatasources>['schema']
        >
      | InferSchemaType<
          FindDatasourceByName<TDatasourceName, TDatasources>['schema']
        >[]
  ) => Promise<IngestResult>;

  public ingest<T extends SchemaDefinition>(
    ingestDef: IngestDefinition<T>
  ): (
    events: InferSchemaType<T> | InferSchemaType<T>[]
  ) => Promise<IngestResult>;

  public ingest<T extends SchemaDefinition>(
    req: IngestDefinition<T> | { datasource: string; wait?: boolean }
  ) {
    let ingestDef: IngestDefinition<any>;

    if ('schema' in req) {
      // This is an IngestDefinition
      ingestDef = req;
    } else {
      // This is a request object with datasource name
      const datasource = Object.values(this.datasources ?? {}).find(
        (d) => d.name === req.datasource
      );
      if (!datasource) {
        throw new Error(`Unknown datasource: ${req.datasource}`);
      }
      ingestDef = defineIngest({
        datasource: datasource.name,
        schema: datasource.schema,
        options: {
          wait: req.wait,
        },
      });
    }

    return async (
      events: InferSchemaType<T> | InferSchemaType<T>[]
    ): Promise<IngestResult> => {
      const eventsArray = Array.isArray(events) ? events : [events];

      const validationResult = ingestDef.validateEvents(eventsArray);

      if (this.noop) {
        return {
          successful_rows: validationResult.validEvents.length,
          quarantined_rows: validationResult.errors.length,
        } as IngestResult;
      }

      const url = new URL('/v0/events', this.baseUrl);
      url.searchParams.set('name', ingestDef.datasource);

      if (ingestDef.options.wait) {
        url.searchParams.set('wait', 'true');
      }

      const body = validationResult.validEvents
        .map((event: any) => JSON.stringify(event))
        .join('\n');

      const response = await this.fetch(url, {
        method: 'POST',
        body,
        headers: {
          Authorization: `Bearer ${this.token}`,
          'Content-Type': 'application/json',
        },
      }).catch((err) => {
        throw new TinybirdError(
          `Unable to ingest to ${ingestDef.datasource}: ${err.message}`,
          err
        );
      });

      const validatedResponse = eventIngestReponseData.safeParse(response);

      if (!validatedResponse.success) {
        throw new TinybirdValidationError(
          `Ingestion response validation failed: ${validatedResponse.error.message}`,
          validatedResponse.error
        );
      }

      return validatedResponse.data as IngestResult;
    };
  }

  private validateParameters<T extends QueryParameters>(
    parameters: T,
    values: Record<string, unknown>
  ): InferParametersType<T> {
    const validated: Record<string, unknown> = {};

    for (const [key, param] of Object.entries(parameters)) {
      const value = values[key];

      if (param.required && (value === undefined || value === null)) {
        if (param.default !== undefined) {
          validated[key] = param.default;
        } else {
          throw new TinybirdValidationError(
            `Required parameter "${key}" is missing`
          );
        }
      } else if (value !== undefined) {
        const result = param.schema.safeParse(value);
        if (!result.success) {
          throw new TinybirdValidationError(
            `Invalid parameter "${key}": ${result.error.message}`,
            result.error
          );
        }
        validated[key] = result.data;
      } else if (param.default !== undefined) {
        validated[key] = param.default;
      }
    }

    return validated as InferParametersType<T>;
  }

  private isQueryParameters(obj: any): obj is QueryParameters {
    if (!obj || typeof obj !== 'object') return false;

    for (const key in obj) {
      const value = obj[key];
      if (
        value &&
        typeof value === 'object' &&
        'name' in value &&
        'type' in value &&
        'schema' in value
      ) {
        return true;
      }
    }

    return false;
  }
}

/**
 * A standalone helper function to create a Zod schema from a TinyKit parameters definition.
 * @param params A record of query parameters.
 * @returns A Zod schema.
 */
export function createZodSchemaFromParameters<T extends QueryParameters>(
  params: T
) {
  const shape = (Object.keys(params) as Array<keyof T>).reduce((acc, key) => {
    const param = params[key];
    if (param) {
      let schema = param.schema;

      if (!param.required && param.default === undefined) {
        schema = schema.optional();
      }
      if (param.default !== undefined) {
        schema = schema.default(param.default) as typeof schema;
      }
      acc[key] = schema;
    }
    return acc;
  }, {} as { [K in keyof T]: z.ZodTypeAny });

  return z.object(shape);
}
