import prompts from 'prompts';
import { mkdirSync, writeFileSync, existsSync, rmSync } from 'fs';
import { join } from 'path';
import { log } from '../utils/terminal';

interface InitOptions {
  dir: string;
}

export async function initCommand(options: InitOptions): Promise<void> {
  log.info('🚀 Initializing TinyKit project...\n');

  // Check if directory already exists
  if (existsSync(options.dir)) {
    const { overwrite } = await prompts({
      type: 'confirm',
      name: 'overwrite',
      message: `Directory "${options.dir}" already exists. Overwrite?`,
      initial: false,
    });

    if (!overwrite) {
      log.warning('Initialization cancelled.');
      return;
    }
    rmSync(options.dir, { recursive: true, force: true });
  }

  // Interactive prompts for configuration
  const responses = await prompts([
    {
      type: 'text',
      name: 'projectName',
      message: 'Project name:',
      initial: 'my-tinybird-project',
      validate: (value: string) =>
        value.trim() ? true : 'Project name is required',
    },
    {
      type: 'multiselect',
      name: 'includeExamples',
      message: 'What examples would you like to include?',
      choices: [
        { title: 'Events tracking schema', value: 'events', selected: true },
        { title: 'User analytics pipes', value: 'analytics', selected: true },
        { title: 'Real-time dashboards', value: 'dashboards' },
        {
          title: 'Data ingestion patterns',
          value: 'ingestion',
          selected: true,
        },
      ],
      hint: '- Space to select. Return to submit',
    },
    {
      type: 'select',
      name: 'packageManager',
      message: 'Package manager:',
      choices: [
        { title: 'npm', value: 'npm' },
        { title: 'yarn', value: 'yarn' },
        { title: 'pnpm', value: 'pnpm' },
        { title: 'bun', value: 'bun' },
      ],
      initial: 3, // bun
    },
    {
      type: 'confirm',
      name: 'initializeGit',
      message: 'Initialize git repository?',
      initial: true,
    },
  ]);

  if (!responses.projectName) {
    log.warning('Initialization cancelled.');
    return;
  }

  // Create directory structure
  try {
    log.info(`📁 Creating directory structure in ${options.dir}...`);
    createDirectoryStructure(options.dir);

    log.info('📝 Generating configuration files...');
    await generateConfigFiles(options.dir, responses);

    if (responses.includeExamples?.length > 0) {
      log.info('📚 Creating example files...');
      await generateExamples(options.dir, responses.includeExamples);
    }

    log.success('TinyKit project initialized successfully!');
    log.plain('\nNext steps:');
    log.dim(`  cd ${options.dir}`);
    log.dim(`  ${responses.packageManager} install`);
    log.dim('  tinykit generate --watch');
  } catch (error) {
    log.error(`Failed to initialize project: ${error}`);
    process.exit(1);
  }
}

function createDirectoryStructure(baseDir: string): void {
  const dirs = [
    baseDir,
    join(baseDir, 'src'),
    join(baseDir, 'tinybird'),
    join(baseDir, 'tinybird', 'datasources'),
    join(baseDir, 'tinybird', 'pipes'),
  ];

  dirs.forEach((dir) => {
    mkdirSync(dir, { recursive: true });
  });
}

interface InitResponses {
  projectName: string;
  packageManager: 'npm' | 'yarn' | 'pnpm' | 'bun';
  includeExamples: string[];
  initializeGit: boolean;
}

async function generateConfigFiles(
  baseDir: string,
  responses: InitResponses
): Promise<void> {
  // package.json
  const packageJson = {
    name: responses.projectName,
    version: '0.1.0',
    description: 'TinyKit project for Tinybird',
    type: 'module',
    scripts: {
      build: 'tsc',
      dev: 'tsc --watch',
      generate: 'tinykit generate',
      'generate:watch': 'tinykit generate --watch',
      validate: 'tinykit validate',
    },
    dependencies: {
      zod: '^3.22.4',
      tinykit: 'latest',
    },
    devDependencies: {
      '@types/node': '^20.10.0',
      typescript: '^5.3.0',
    },
  };

  writeFileSync(
    join(baseDir, 'package.json'),
    JSON.stringify(packageJson, null, 2)
  );

  // tsconfig.json
  const tsConfig = {
    compilerOptions: {
      target: 'ES2022',
      module: 'ESNext',
      moduleResolution: 'node',
      esModuleInterop: true,
      allowSyntheticDefaultImports: true,
      strict: true,
      skipLibCheck: true,
      forceConsistentCasingInFileNames: true,
      outDir: './dist',
      rootDir: './src',
    },
    include: ['src/**/*'],
    exclude: ['node_modules', 'dist'],
  };

  writeFileSync(
    join(baseDir, 'tsconfig.json'),
    JSON.stringify(tsConfig, null, 2)
  );

  // .gitignore
  const gitignore = `
node_modules/
dist/
*.log
.env*
.DS_Store
*.tgz
*.tar.gz
.cache/
`;

  writeFileSync(join(baseDir, '.gitignore'), gitignore.trim());

  // README.md
  const readme = `# ${responses.projectName}

A TinyKit project for Tinybird analytics.

## Getting Started

Install dependencies:
\`\`\`bash
${responses.packageManager} install
\`\`\`

Generate Tinybird files:
\`\`\`bash
${responses.packageManager} run generate
\`\`\`

Watch for changes:
\`\`\`bash
${responses.packageManager} run generate:watch
\`\`\`

## Project Structure

- \`src/\` - TypeScript source files with TinyKit definitions
- \`tinybird/\` - Generated Tinybird datasources and pipes
- \`tinybird/datasources/\` - Data source definitions
- \`tinybird/pipes/\` - Pipe definitions

## Commands

- \`tinykit generate\` - Generate Tinybird files from TypeScript definitions
- \`tinykit validate\` - Validate generated Tinybird files
- \`tinykit init\` - Initialize a new TinyKit project
`;

  writeFileSync(join(baseDir, 'README.md'), readme);
}

async function generateExamples(
  baseDir: string,
  examples: string[]
): Promise<void> {
  const srcDir = join(baseDir, 'src');

  if (examples.includes('events')) {
    const eventsExample = `import { z } from 'zod';
import {
  defineSchema,
  defineDataSource,
  definePipe,
  defineParameters,
  streamingIngest,
  query,
  string,
  int64,
  stringParam,
  int64Param,
  count,
  timeGranularity,
  fromUnixTimestamp64Milli,
  param,
} from 'tinykit';

// Events schema definition
export const eventsSchema = defineSchema({
  id: string({ jsonPath: '$.id' })('id'),
  tenantId: string({ jsonPath: '$.tenantId' })('tenantId'),
  channelId: string({ jsonPath: '$.channelId' })('channelId'),
  time: int64({ jsonPath: '$.time' })('time'),
  event: string({ jsonPath: '$.event' })('event'),
  content: string({ jsonPath: '$.content' })('content'),
  metadata: string({ jsonPath: '$.metadata' })('metadata'),
});

// Data source
export const eventsDataSource = defineDataSource({
  name: 'events__v1',
  version: 1,
  schema: eventsSchema,
  engine: 'MergeTree',
  sortingKey: ['tenantId', 'channelId', 'time', 'id'],
});

// Event activity aggregation pipe
export const eventActivityPipe = definePipe({
  name: 'get_event_activity__v1',
  version: 1,
  schema: eventsSchema,
  parameters: defineParameters({
    tenantId: stringParam({ required: true })('tenantId'),
    start: int64Param({ required: true })('start'),
    end: int64Param()('end'),
  }),
  outputSchema: z.object({
    time: z.string().transform(t => new Date(t).getTime()),
    count: z.number().nullable().transform(v => v ?? 0),
  }),
}).endpoint((q, params) =>
  query(eventsSchema)
    .selectRaw(\`\${count()} AS count\`)
    .selectRaw(\`\${timeGranularity(fromUnixTimestamp64Milli('time'), '1h')} as time\`)
    .from('events__v1')
    .where(\`tenantId = \${param('tenantId', 'String', true)}\`)
    .and(params.end ? \`time <= \${fromUnixTimestamp64Milli(param('end', 'Int64'))}\` : '1=1')
    .and(\`time >= \${fromUnixTimestamp64Milli(param('start', 'Int64', true))}\`)
    .groupBy('time')
    .orderBy('time', 'ASC')
);

// Ingest configuration
export const eventsIngest = streamingIngest({
  datasource: 'events__v1',
  schema: eventsSchema,
});
`;

    writeFileSync(join(srcDir, 'events.ts'), eventsExample);
  }

  if (examples.includes('ingestion')) {
    const ingestionExample = `import { z } from 'zod';
import { Tinybird, syncIngest, streamingIngest } from 'tinykit';
import { eventsSchema } from './events';

// Initialize Tinybird client
const tb = new Tinybird({
  token: process.env.TINYBIRD_TOKEN!,
});

// Different ingestion strategies
export const syncEventsIngest = syncIngest({
  datasource: 'events__v1',
  schema: eventsSchema,
});

export const streamEventsIngest = streamingIngest({
  datasource: 'events__v1',
  schema: eventsSchema,
});

// Build ingest functions
export const ingestEventsSync = tb.ingest(syncEventsIngest);
export const ingestEventsStream = tb.ingest(streamEventsIngest);

// Usage example
export async function ingestSampleData() {
  const sampleEvents = [
    {
      id: 'evt-001',
      tenantId: 'tenant-123',
      channelId: 'web',
      time: Date.now(),
      event: 'page_view',
      content: 'User viewed homepage',
      metadata: JSON.stringify({ url: '/', referrer: 'google' }),
    },
    {
      id: 'evt-002',
      tenantId: 'tenant-123',
      channelId: 'web',
      time: Date.now() + 1000,
      event: 'button_click',
      content: 'User clicked CTA',
      metadata: JSON.stringify({ button_id: 'hero-cta', section: 'header' }),
    },
  ];

  try {
    const result = await ingestEventsSync(sampleEvents);
    console.log('Ingested', result.successful_rows, 'events');
  } catch (error) {
    console.error('Ingestion failed:', error);
  }
}
`;

    writeFileSync(join(srcDir, 'ingestion.ts'), ingestionExample);
  }
}
