import {
  Tinybird,
  defineSchema,
  defineDataSource,
  syncIngest,
  string,
  int64,
} from '../src';

const eventsSchema = defineSchema({
  id: string('id', { jsonPath: '$.id' }),
  tenantId: string('tenantId', { jsonPath: '$.tenantId' }),
  channelId: string('channelId', { jsonPath: '$.channelId' }),
  time: int64('time', { jsonPath: '$.time' }),
  event: string('event', { jsonPath: '$.event' }),
  content: string('content', { jsonPath: '$.content' }),
  metadata: string('metadata', { jsonPath: '$.metadata' }),
});

const eventsDataSource = defineDataSource({
    name: 'events__v1',
    schema: eventsSchema,
    engine: 'MergeTree',
});

const eventsSync = syncIngest({
  datasource: 'events__v1',
  schema: eventsSchema,
});


const sampleEvents = [
  {
    id: 'evt_001',
    tenantId: 'tenant_123',
    channelId: 'general',
    time: Date.now(),
    event: 'user_signup',
    content: 'New user registered',
    metadata: JSON.stringify({ source: 'web', plan: 'free' }),
  },
];

const tb = new Tinybird({
  token: process.env.TINYBIRD_TOKEN || 'your-token-here',
  datasources: {
    events: eventsDataSource,
  },
});

const ingestEvents = tb.ingest(eventsSync);

async function demo() {
  console.log('Ingesting events...');

  try {
    const result = await ingestEvents(sampleEvents);
    console.log(`Ingested ${result.successful_rows} events successfully`);

    if (result.quarantined_rows > 0) {
      console.warn(`${result.quarantined_rows} events were quarantined`);
    }

    return result;
  } catch (error) {
    console.error('Ingestion failed:', error);
    throw error;
  }
}

console.log('=== Ingesting data ===');
demo();

export {
  eventsSchema,
  ingestEvents,
  sampleEvents,
  demo,
};