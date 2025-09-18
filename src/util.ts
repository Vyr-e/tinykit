import { z } from 'zod';

export const pipeResponseWithoutData = z.object({
  meta: z.array(z.object({
    name: z.string().optional(),
    type: z.string().optional(),
  })).optional(),
  statistics: z.object({
    elapsed: z.number().optional(),
    rows_read: z.number().optional(),
    bytes_read: z.number().optional(),
  }).optional(),
});

export const eventIngestReponseData = z.object({
  successful_rows: z.number(),
  quarantined_rows: z.number(),
});

export type PipeErrorResponse = {
  error: string;
  message?: string;
  code?: string;
};