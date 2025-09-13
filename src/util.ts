import { z } from 'zod';

export const pipeResponseWithoutData = z.object({
  meta: z.array(z.object({
    name: z.string(),
    type: z.string(),
  })),
  statistics: z.object({
    elapsed: z.number(),
    rows_read: z.number(),
    bytes_read: z.number(),
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