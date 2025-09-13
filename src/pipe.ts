import { z } from 'zod';
import type {
  QueryParameters,
  QueryParameter,
  PipeConfig,
  InferParametersType,
  SchemaDefinition,
} from './types';
import type { QueryBuilder } from './query';

export function defineParameter<T>(
  type: 'String' | 'Int64' | 'Float64' | 'DateTime' | 'Date' | 'Boolean',
  schema: z.ZodSchema<T>,
  options?: {
    required?: boolean;
    default?: T;
  }
): (name: string) => QueryParameter<T> {
  return (name: string) => ({
    name,
    type,
    schema,
    ...options,
  });
}

export const stringParam = (options?: { required?: boolean; default?: string }) =>
  defineParameter('String', z.string(), options);

export const int64Param = (options?: { required?: boolean; default?: number }) =>
  defineParameter('Int64', z.number().int(), options);

export const float64Param = (options?: { required?: boolean; default?: number }) =>
  defineParameter('Float64', z.number(), options);

export const dateTimeParam = (options?: { required?: boolean; default?: string | Date | number }) =>
  defineParameter('DateTime', z.union([z.string(), z.date(), z.number()]), options);

export const dateParam = (options?: { required?: boolean; default?: string | Date }) =>
  defineParameter('Date', z.union([z.string(), z.date()]), options);

export const booleanParam = (options?: { required?: boolean; default?: boolean }) =>
  defineParameter('Boolean', z.boolean(), options);

export const enumParam = <T extends readonly string[]>(
  values: T,
  options?: { required?: boolean; default?: T[number] }
) => defineParameter('String', z.enum(values as any), options);

export function defineParameters<T extends QueryParameters>(params: T): T {
  return params;
}

export class PipeBuilder<
  TSchema extends SchemaDefinition,
  TParams extends QueryParameters,
  TOutput extends z.ZodSchema
> {
  constructor(
    private config: {
      name: string;
      version?: number;
      schema: TSchema;
      parameters: TParams;
      outputSchema: TOutput;
    }
  ) {}

  node(
    name: string,
    queryFn: (
      q: QueryBuilder<TSchema>,
      params: InferParametersType<TParams>
    ) => QueryBuilder<TSchema>
  ): this {
    return this;
  }

  endpoint(
    queryFn: (
      q: QueryBuilder<TSchema>,
      params: InferParametersType<TParams>
    ) => QueryBuilder<TSchema>
  ): PipeConfig<TParams, TOutput> {
    return {
      name: this.config.name,
      version: this.config.version,
      parameters: this.config.parameters,
      outputSchema: this.config.outputSchema,
      sql: (params: InferParametersType<TParams>) => {
        const q = queryFn({} as QueryBuilder<TSchema>, params);
        return this.generatePipeSQL(q.build(), params);
      },
    };
  }

  private generatePipeSQL(query: string, params: any): string {
    let sql = '';

    if (this.config.version) {
      sql += `VERSION ${this.config.version}\n\n`;
    }

    sql += 'NODE endpoint\nSQL >\n    %\n';

    const lines = query.split('\n');
    const formattedQuery = lines.map(line => `    ${line}`).join('\n');

    sql += formattedQuery;

    const hasConditionalParams = Object.entries(this.config.parameters).some(
      ([key, param]) => !param.required && params[key] !== undefined
    );

    if (hasConditionalParams) {
      sql = this.addConditionalParameters(sql, params);
    }

    return sql;
  }

  private addConditionalParameters(sql: string, params: any): string {
    Object.entries(this.config.parameters).forEach(([key, param]) => {
      if (!param.required && params[key] !== undefined) {
        const condition = `{% if defined(${key}) %}`;
        const endCondition = '{% end %}';

        const regex = new RegExp(`{{.*?${key}.*?}}`, 'g');
        sql = sql.replace(regex, (match) => {
          return `${condition} ${match} ${endCondition}`;
        });
      }
    });

    return sql;
  }
}

export function definePipe<
  TSchema extends SchemaDefinition,
  TParams extends QueryParameters,
  TOutput extends z.ZodSchema
>(config: {
  name: string;
  version?: number;
  schema: TSchema;
  parameters: TParams;
  outputSchema: TOutput;
}): PipeBuilder<TSchema, TParams, TOutput> {
  return new PipeBuilder(config);
}