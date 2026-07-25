import { z } from 'zod';
import type {
  QueryParameters,
  QueryParameter,
  PipeConfig,
  InferParametersType,
  InferParametersWithDefaults,
  SchemaDefinition,
} from './types.js';
import type { QueryBuilder } from './query.js';
import { query } from './query.js';
import { createZodSchemaFromParameters } from './client.js';

type QueryParameterOptions<T> = {
  required?: boolean;
  default?: T;
};

type DefinedQueryParameter<
  T,
  TOptions extends QueryParameterOptions<T>
> = QueryParameter<T> & TOptions;

function defineQueryParameter<
  T,
  const TOptions extends QueryParameterOptions<T>
>(
  parameter: QueryParameter<T>,
  options?: TOptions
): DefinedQueryParameter<T, TOptions> {
  return {
    ...parameter,
    ...options,
  } as DefinedQueryParameter<T, TOptions>;
}

/**
 * Defines a String parameter for a Tinybird Pipe.
 * @param name The name of the parameter.
 * @param options Configuration options for the parameter.
 * @returns A query parameter definition.
 *
 * @example
 * ```
 * const params = defineParameters({
 *   myParam: stringParam('myParam', { default: 'hello' })
 * });
 * ```
 */
export const stringParam = <
  const TOptions extends QueryParameterOptions<string> = {}
>(
  name: string,
  options?: TOptions
): DefinedQueryParameter<string, TOptions> =>
  defineQueryParameter(
    {
      name,
      type: 'String',
      schema: z.string(),
    },
    options
  );

/**
 * Defines an Int64 parameter for a Tinybird Pipe.
 * @param name The name of the parameter.
 * @param options Configuration options for the parameter.
 * @returns A query parameter definition.
 *
 * @example
 * ```
 * const params = defineParameters({
 *   myParam: int64Param('myParam', { default: 123 })
 * });
 * ```
 */
export const int64Param = <
  const TOptions extends QueryParameterOptions<number> = {}
>(
  name: string,
  options?: TOptions
): DefinedQueryParameter<number, TOptions> =>
  defineQueryParameter(
    {
      name,
      type: 'Int64',
      schema: z.number().int(),
    },
    options
  );

/**
 * Defines a Float64 parameter for a Tinybird Pipe.
 * @param name The name of the parameter.
 * @param options Configuration options for the parameter.
 * @returns A query parameter definition.
 *
 * @example
 * ```
 * const params = defineParameters({
 *   myParam: float64Param('myParam', { default: 1.23 })
 * });
 * ```
 */
export const float64Param = <
  const TOptions extends QueryParameterOptions<number> = {}
>(
  name: string,
  options?: TOptions
): DefinedQueryParameter<number, TOptions> =>
  defineQueryParameter(
    {
      name,
      type: 'Float64',
      schema: z.number(),
    },
    options
  );

/**
 * Defines a DateTime parameter for a Tinybird Pipe.
 * @param name The name of the parameter.
 * @param options Configuration options for the parameter.
 * @returns A query parameter definition.
 *
 * @example
 * ```
 * const params = defineParameters({
 *   myParam: dateTimeParam('myParam', { default: '2023-01-01T00:00:00Z' })
 * });
 * ```
 */
export const dateTimeParam = <
  const TOptions extends QueryParameterOptions<string | Date | number> = {}
>(
  name: string,
  options?: TOptions
): DefinedQueryParameter<string | Date | number, TOptions> =>
  defineQueryParameter(
    {
      name,
      type: 'DateTime',
      schema: z.union([z.string(), z.date(), z.number()]),
    },
    options
  );

/**
 * Defines a Date parameter for a Tinybird Pipe.
 * @param name The name of the parameter.
 * @param options Configuration options for the parameter.
 * @returns A query parameter definition.
 *
 * @example
 * ```
 * const params = defineParameters({
 *   myParam: dateParam('myParam', { default: '2023-01-01' })
 * });
 * ```
 */
export const dateParam = <
  const TOptions extends QueryParameterOptions<string | Date> = {}
>(
  name: string,
  options?: TOptions
): DefinedQueryParameter<string | Date, TOptions> =>
  defineQueryParameter(
    {
      name,
      type: 'Date',
      schema: z.union([z.string(), z.date()]),
    },
    options
  );

/**
 * Defines a Boolean parameter for a Tinybird Pipe.
 * @param name The name of the parameter.
 * @param options Configuration options for the parameter.
 * @returns A query parameter definition.
 *
 * @example
 * ```
 * const params = defineParameters({
 *   myParam: booleanParam('myParam', { default: true })
 * });
 * ```
 */
export const booleanParam = <
  const TOptions extends QueryParameterOptions<boolean> = {}
>(
  name: string,
  options?: TOptions
): DefinedQueryParameter<boolean, TOptions> =>
  defineQueryParameter(
    {
      name,
      type: 'Boolean',
      schema: z.boolean(),
    },
    options
  );

/**
 * Defines an Enum parameter for a Tinybird Pipe.
 * @param name The name of the parameter.
 * @param values An array of possible string values for the enum.
 * @param options Configuration options for the parameter.
 * @returns A query parameter definition.
 *
 * @example
 * ```
 * const dailyActivityParams = defineParameters({
 *   granularity: enumParam('granularity', ['1h', '1d'] as const, { default: '1h' }),
 * });
 * ```
 */
export function enumParam<
  T extends readonly [string, ...string[]],
  const TOptions extends QueryParameterOptions<T[number]> = {}
>(
  name: string,
  values: T,
  options?: TOptions
): DefinedQueryParameter<T[number], TOptions> {
  return defineQueryParameter(
    {
      name,
      type: 'String',
      schema: z.enum(values),
    },
    options
  );
}

/**
 * A helper function to define a set of parameters for a Tinybird Pipe with type inference.
 * @param params A record of query parameters.
 * @returns The query parameters definition.
 *
 * @example
 * ```
 * const myPipeParams = defineParameters({
 *   tenantId: stringParam('tenantId', { required: true }),
 *   limit: int64Param('limit', { default: 10 })
 * });
 * ```
 */
export function defineParameters<T extends QueryParameters>(params: T): T {
  return params;
}

/**
 * Creates a Zod schema from a TinyKit parameters definition.
 * @param params A record of query parameters.
 * @returns A Zod schema.
 *
 * @example
 * ```
 * const zodSchema = createParameterSchema(myPipeParams);
 * ```
 */
export function createParameterSchema<T extends QueryParameters>(
  params: T
): z.ZodSchema {
  return createZodSchemaFromParameters(params);
}

/**
 * Validates a data object against a TinyKit parameters definition.
 * @param params The query parameters definition.
 * @param data The data to validate.
 * @returns An object containing the validation result.
 *
 * @example
 * ```
 * const { success, data, error } = validateParameters(myPipeParams, { tenantId: 'my-tenant' });
 * ```
 */
export function validateParameters<T extends QueryParameters>(
  params: T,
  data: unknown
): { success: boolean; data?: InferParametersType<T>; error?: any } {
  const zodSchema = createParameterSchema(params);
  const result = zodSchema.safeParse(data);
  return {
    success: result.success,
    data: result.success ? result.data : undefined,
    error: result.success ? undefined : result.error,
  };
}

/**
 * A builder class for creating Tinybird Pipe configurations.
 */
export class PipeBuilder<
  TSchema extends SchemaDefinition,
  TParams extends QueryParameters,
  TName extends string
> {
  constructor(
    private config: {
      name: TName;
      version?: number;
      schema: TSchema;
      parameters: TParams;
    }
  ) {}

  /**
   * Defines the endpoint of a Pipe using the query builder.
   * @param queryFn A function that receives a query builder instance and the pipe parameters, and returns a constructed query.
   * @returns A Pipe configuration object.
   */
  endpoint(
    queryFn: (
      q: QueryBuilder<TSchema>,
      params: InferParametersWithDefaults<TParams>
    ) => QueryBuilder<TSchema>
  ): PipeConfig<TParams, TName> {
    return {
      name: this.config.name,
      version: this.config.version,
      parameters: this.config.parameters,
      sql: (params: InferParametersType<TParams> | {}) => {
        const paramsWithDefaults = this.applyDefaults(params);
        const q = queryFn(query(this.config.schema), paramsWithDefaults);
        return this.applyConditionalParameters(q.build(), params);
      },
      isRaw: false,
    };
  }

  /**
   * Defines the endpoint of a Pipe using a raw SQL string.
   * @param sql The raw SQL string for the pipe.
   * @returns A Pipe configuration object.
   */
  raw(sql: string): PipeConfig<TParams, TName> {
    return {
      name: this.config.name,
      version: this.config.version,
      parameters: this.config.parameters,
      sql: () => sql,
      isRaw: true,
      rawSql: sql,
    };
  }

  private applyDefaults(
    params: InferParametersType<TParams> | {}
  ): InferParametersWithDefaults<TParams> {
    const result = { ...params } as any;

    for (const [key, param] of Object.entries(this.config.parameters)) {
      if (param.default !== undefined && result[key] === undefined) {
        result[key] = param.default;
      }
    }

    return result as InferParametersWithDefaults<TParams>;
  }

  private applyConditionalParameters(
    query: string,
    params: InferParametersType<TParams> | {}
  ): string {
    const hasConditionalParams = Object.entries(this.config.parameters).some(
      ([key, param]) => !param.required && param.default === undefined
    );

    if (!hasConditionalParams) {
      return query;
    }

    return this.addConditionalParameters(query, params);
  }

  private addConditionalParameters(
    sql: string,
    params: InferParametersType<TParams> | {}
  ): string {
    Object.entries(this.config.parameters).forEach(([key, param]) => {
      if (!param.required && param.default === undefined) {
        const condition = `{% if defined(${key}) %}`;
        const endCondition = '{% end %}';

        const regex = new RegExp(`{{\\s*\\w+\\(${key}\\)\\s*}}`, 'g');
        sql = sql.replace(regex, (match) => {
          return `${condition}${match}${endCondition}`;
        });
      }
    });

    return sql;
  }
}

/**
 * A helper function to define a Tinybird Pipe with type inference.
 * @param config The Pipe configuration.
 * @returns A PipeBuilder instance to continue defining the pipe.
 *
 * @example
 * ```
 * export const getDailyActivity = definePipe({
 *   name: 'get_daily_activity',
 *   schema: eventsSchema,
 *   parameters: dailyActivityParams,
 * }).endpoint((q, params) =>
 *   query(eventsSchema)
 *     .selectRaw('count() as event_count')
 *     .from('events')
 *     .where(`tenantId = ${param('tenantId', 'String', true)}`)
 * );
 * ```
 */
export function definePipe<
  TSchema extends SchemaDefinition,
  TParams extends QueryParameters,
  const TName extends string
>(config: {
  name: TName;
  version?: number;
  schema: TSchema;
  parameters: TParams;
}): PipeBuilder<TSchema, TParams, TName> {
  return new PipeBuilder(config);
}
