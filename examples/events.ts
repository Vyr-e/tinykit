import { z } from 'zod';
import {
  Tinybird,
  defineSchema,
  defineDataSource,
  definePipe,
  defineParameters,
  query,
  string,
  int64,
  stringParam,
  int64Param,
  count,
  param,
  defineIngest,
} from '../src';

const eventsSchema = defineSchema({
  id: string('id'),
  userId: string('userId'),
  event: string('event'),
  timestamp: int64('timestamp'),
  properties: string('properties'),
});

const eventsDataSource = defineDataSource({
  name: 'events__v1',
  schema: eventsSchema,
  engine: 'MergeTree',
  sortingKey: ['timestamp', 'userId'],
});

const eventsParams = defineParameters({
  userId: stringParam('userId', { required: true }),
  limit: int64Param('limit', { default: 100 }),
});

const getEventsByUser = definePipe({
  name: 'get_events_by_user__v1',
  schema: eventsSchema,
  parameters: eventsParams,
}).endpoint((q, params) =>
  query(eventsSchema)
    .select('id', 'userId', 'event', 'timestamp')
    .from('events__v1')
    .where(`userId = ${param('userId', 'String', true)}`)
    .orderBy('timestamp DESC')
    .limit(params.limit)
);

const getEventCounts = definePipe({
  name: 'get_event_counts__v1',
  schema: eventsSchema,
  parameters: defineParameters({
    startTime: int64Param('startTime', { required: true }),
    endTime: int64Param('endTime', { required: true }),
  }),
}).endpoint((q, params) =>
  query(eventsSchema)
    .selectRaw(`event, ${count()} as event_count`)
    .from('events__v1')
    .where(`timestamp >= ${param('startTime', 'Int64', true)}`)
    .and(`timestamp <= ${param('endTime', 'Int64', true)}`)
    .groupBy('event')
    .orderBy('event_count DESC')
);

const tb = new Tinybird({
  token: process.env.TINYBIRD_TOKEN || 'your-token-here',
  datasources: {
    events: eventsDataSource,
  },
  pipes: {
    getEventsByUser,
    getEventCounts,
  },
});

const eventsIngest = defineIngest({
  datasource: 'events__v1',
  schema: eventsSchema,
});

async function runDemo() {
  const getUserEvents = tb.pipe({
    pipe: 'get_events_by_user__v1',
    data: z.object({
      id: z.string(),
      userId: z.string(),
      event: z.string(),
      timestamp: z.number(),
    }),
  });

  const getEventCountsPipe = tb.pipe({
    pipe: 'get_event_counts__v1',
    data: z.object({
      event: z.string(),
      event_count: z.number(),
    }),
  });

  const userEvents = await getUserEvents({ userId: 'user-123' });
  console.log(`Found ${userEvents.data.length} events for user`);

  const eventCounts = await getEventCountsPipe({
    startTime: Date.now() - 24 * 60 * 60 * 1000,
    endTime: Date.now(),
  });
  console.log('Event counts:', eventCounts.data);

  const ingest = tb.ingest(eventsIngest);
  await ingest([
    {
      id: 'evt-1',
      userId: 'user-456',
      event: 'page_view',
      timestamp: Date.now(),
      properties: JSON.stringify({ page: '/home' }),
    },
  ]);

  return { userEvents: userEvents.data, eventCounts: eventCounts.data };
}

export {
  eventsSchema,
  eventsDataSource,
  getEventsByUser,
  getEventCounts,
  runDemo,
  tb,
};
