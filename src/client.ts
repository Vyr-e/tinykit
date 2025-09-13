import { z } from 'zod';
import type {
  PipeConfig,
  PipeResult,
  QueryParameters,
  InferParametersType,
  SchemaDefinition,
  InferSchemaType,
} from './types';
import { type PipeErrorResponse, eventIngestReponseData, pipeResponseWithoutData } from './util';
import type { IngestDefinition, IngestResult } from './ingest';

type RequestCache = 'default' | 'no-store' | 'reload' | 'no-cache' | 'force-cache' | 'only-if-cached';

export type Config = {
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

export class Tinybird {
  private readonly baseUrl: string;
  private readonly token: string;
  private readonly noop: boolean;

  constructor(config: Config) {
    this.baseUrl = config.baseUrl ?? 'https://api.tinybird.co';
    if (config.noop) {
      this.token = '';
      this.noop = true;
    } else {
      this.token = config.token;
      this.noop = false;
    }
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
    for (let i = 0; i < 10; i++) {
      const res = await fetch(url, opts);
      if (res.ok) {
        return res.json();
      }

      if (res.status === 429 || res.status >= 500) {
        await new Promise((r) => setTimeout(r, 1000 + i ** 2 * 50));
        continue;
      }

      if (res.status === 403) {
        throw new Error('Unauthorized');
      }

      if (!res.ok) {
        const error = (await res.json()) as PipeErrorResponse;
        throw new Error(error.error);
      }
    }
  }

  public buildPipe<TParameters extends z.ZodSchema<any>, TData extends z.ZodSchema<any>>(req: {
    pipe: string;
    parameters?: TParameters;
    data: TData;
    opts?: {
      cache?: RequestCache;
      next?: {
        revalidate?: number;
      };
    };
  }): (
    params: z.input<TParameters>
  ) => Promise<z.infer<typeof pipeResponseWithoutData> & { data: z.output<TData>[] }> {
    const outputSchema = pipeResponseWithoutData.setKey('data', z.array(req.data));
    return async (params: z.input<TParameters>) => {
      let validatedParams: z.input<TParameters> | undefined = undefined;
      if (req.parameters) {
        const v = req.parameters.safeParse(params);
        if (!v.success) {
          throw new Error(v.error.message);
        }
        validatedParams = v.data;
      }
      if (this.noop) {
        return { meta: [], data: [] };
      }
      const url = new URL(`/v0/pipes/${req.pipe}.json`, this.baseUrl);
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
      const validatedResponse = outputSchema.safeParse(res);
      if (!validatedResponse.success) {
        throw new Error(validatedResponse.error.message);
      }

      return validatedResponse.data;
    };
  }

  public buildIngestEndpoint<TSchema extends z.ZodSchema<any>>(req: {
    datasource: string;
    event: TSchema;
    wait?: boolean;
  }): (
    events: z.input<TSchema> | z.input<TSchema>[]
  ) => Promise<z.infer<typeof eventIngestReponseData>> {
    return async (events: z.input<TSchema> | z.input<TSchema>[]) => {
      let validatedEvents: z.output<TSchema> | z.output<TSchema>[] | undefined = undefined;
      if (req.event) {
        const v = Array.isArray(events)
          ? req.event.array().safeParse(events)
          : req.event.safeParse(events);
        if (!v.success) {
          throw new Error(v.error.message);
        }
        validatedEvents = v.data;
      }

      if (this.noop) {
        return {
          successful_rows: Array.isArray(validatedEvents) ? validatedEvents.length : 1,
          quarantined_rows: 0,
        };
      }
      const url = new URL('/v0/events', this.baseUrl);
      url.searchParams.set('name', req.datasource);

      if (req.wait) {
        url.searchParams.set('wait', 'true');
      }

      const body = (Array.isArray(validatedEvents) ? validatedEvents : [validatedEvents])
        .map((p) => JSON.stringify(p))
        .join('\n');

      const res = await this.fetch(url, {
        method: 'POST',
        body,
        headers: { Authorization: `Bearer ${this.token}`, 'Content-Type': 'application/json' },
      }).catch((err) => {
        throw new Error(`Unable to ingest to ${req.datasource}: ${err.message}`);
      });

      const validatedResponse = eventIngestReponseData.safeParse(res);

      if (!validatedResponse.success) {
        throw new Error(validatedResponse.error.message);
      }

      return validatedResponse.data;
    };
  }

  // Functional ingest API using schema definitions
  public ingest<T extends SchemaDefinition>(
    ingestDef: IngestDefinition<T>
  ) {
    return async (
      events: InferSchemaType<T> | InferSchemaType<T>[]
    ): Promise<IngestResult> => {
      const eventsArray = Array.isArray(events) ? events : [events];

      // Validate all events using the schema definition (runtime validation)
      const validatedEvents = ingestDef.validateEvents(eventsArray);

      if (this.noop) {
        return {
          successful_rows: validatedEvents.length,
          quarantined_rows: 0,
        };
      }

      const url = new URL('/v0/events', this.baseUrl);
      url.searchParams.set('name', ingestDef.datasource);

      if (ingestDef.options.wait) {
        url.searchParams.set('wait', 'true');
      }

      const body = validatedEvents
        .map(event => JSON.stringify(event))
        .join('\n');

      const response = await this.fetch(url, {
        method: 'POST',
        body,
        headers: {
          Authorization: `Bearer ${this.token}`,
          'Content-Type': 'application/json',
        },
      }).catch((err) => {
        throw new Error(`Unable to ingest to ${ingestDef.datasource}: ${err.message}`);
      });

      const validatedResponse = eventIngestReponseData.safeParse(response);

      if (!validatedResponse.success) {
        throw new Error(validatedResponse.error.message);
      }

      return validatedResponse.data;
    };
  }

  // Simpler ingest method using Zod schema directly
  public ingestRaw<T extends z.ZodSchema>(config: {
    datasource: string;
    schema: T;
    wait?: boolean;
  }) {
    return async (events: z.input<T> | z.input<T>[]) => {
      const validatedEvents = Array.isArray(events)
        ? events.map(e => config.schema.parse(e))
        : [config.schema.parse(events)];

      if (this.noop) {
        return {
          successful_rows: validatedEvents.length,
          quarantined_rows: 0,
        };
      }

      const url = new URL('/v0/events', this.baseUrl);
      url.searchParams.set('name', config.datasource);

      if (config.wait) {
        url.searchParams.set('wait', 'true');
      }

      const body = validatedEvents
        .map(event => JSON.stringify(event))
        .join('\n');

      const response = await this.fetch(url, {
        method: 'POST',
        body,
        headers: {
          Authorization: `Bearer ${this.token}`,
          'Content-Type': 'application/json',
        },
      }).catch((err) => {
        throw new Error(`Unable to ingest to ${config.datasource}: ${err.message}`);
      });

      const validatedResponse = eventIngestReponseData.safeParse(response);

      if (!validatedResponse.success) {
        throw new Error(validatedResponse.error.message);
      }

      return validatedResponse.data;
    };
  }

  // Extended functional query building methods
  public buildQuery<
    TParams extends QueryParameters,
    TOutput extends z.ZodSchema
  >(pipe: PipeConfig<TParams, TOutput>) {
    const executeQuery = async (
      params: InferParametersType<TParams>
    ): Promise<PipeResult<z.infer<TOutput>>> => {
      if (this.noop) {
        return { meta: [], data: [] };
      }

      const validatedParams = this.validateParameters(pipe.parameters, params);

      const url = new URL(`/v0/pipes/${pipe.name}.json`, this.baseUrl);

      for (const [key, value] of Object.entries(validatedParams)) {
        if (value !== undefined && value !== null) {
          url.searchParams.set(key, String(value));
        }
      }

      const response = await this.fetch(url, {
        method: 'GET',
        headers: { Authorization: `Bearer ${this.token}` },
      });

      const outputSchema = pipeResponseWithoutData.setKey('data', z.array(pipe.outputSchema));
      const parsed = outputSchema.parse(response);

      return {
        meta: parsed.meta,
        data: parsed.data,
        statistics: parsed.statistics,
      };
    };

    executeQuery.pipe = pipe;
    executeQuery.getSql = (params: InferParametersType<TParams>) => pipe.sql(params);

    return executeQuery;
  }

  private validateParameters<T extends QueryParameters>(
    parameters: T,
    values: any
  ): InferParametersType<T> {
    const validated: any = {};

    for (const [key, param] of Object.entries(parameters)) {
      const value = values[key];

      if (param.required && (value === undefined || value === null)) {
        if (param.default !== undefined) {
          validated[key] = param.default;
        } else {
          throw new Error(`Required parameter "${key}" is missing`);
        }
      } else if (value !== undefined) {
        const result = param.schema.safeParse(value);
        if (!result.success) {
          throw new Error(`Invalid parameter "${key}": ${result.error.message}`);
        }
        validated[key] = result.data;
      } else if (param.default !== undefined) {
        validated[key] = param.default;
      }
    }

    return validated as InferParametersType<T>;
  }
}