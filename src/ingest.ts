import { z } from 'zod';
import type { SchemaDefinition, InferSchemaType } from './types';

export type IngestConfig<T extends SchemaDefinition> = {
  datasource: string;
  schema: T;
  options?: {
    wait?: boolean;
    batchSize?: number;
  };
};

export type IngestResult = {
  successful_rows: number;
  quarantined_rows: number;
  bytes_ingested?: number;
  ingestion_time?: number;
};

// Helper to create Zod object schema from our schema definition
type CreateZodSchema<T extends SchemaDefinition> = z.ZodObject<{
  [K in keyof T]: T[K]['schema'];
}>;

export function defineIngest<T extends SchemaDefinition>(config: IngestConfig<T>) {
  // Create Zod schema from our schema definition
  const zodSchema = (() => {
    const schemaObj = {} as { [K in keyof T]: T[K]['schema'] };

    Object.entries(config.schema).forEach(([key, columnDef]) => {
      (schemaObj as any)[key] = columnDef.schema;
    });

    return z.object(schemaObj) as CreateZodSchema<T>;
  })();

  return {
    datasource: config.datasource,
    schema: config.schema,
    options: config.options || {},
    zodSchema,

    // Validate a single event with proper typing
    validateEvent(event: unknown): InferSchemaType<T> {
      return zodSchema.parse(event) as InferSchemaType<T>;
    },

    // Validate multiple events with proper typing
    validateEvents(events: unknown[]): InferSchemaType<T>[] {
      return events.map(event => this.validateEvent(event));
    }
  };
}

export type IngestDefinition<T extends SchemaDefinition> = ReturnType<typeof defineIngest<T>>;

// Helper functions for creating different types of ingest configurations
export const streamingIngest = <T extends SchemaDefinition>(config: {
  datasource: string;
  schema: T;
}) => defineIngest({
  ...config,
  options: { wait: false },
});

export const syncIngest = <T extends SchemaDefinition>(config: {
  datasource: string;
  schema: T;
}) => defineIngest({
  ...config,
  options: { wait: true },
});

export const batchIngest = <T extends SchemaDefinition>(config: {
  datasource: string;
  schema: T;
  batchSize: number;
}) => defineIngest({
  ...config,
  options: { batchSize: config.batchSize },
});