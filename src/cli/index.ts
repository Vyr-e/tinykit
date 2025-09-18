#!/usr/bin/env node

import { z } from 'zod';
import { defineConfig, defineCommand, defineOptions } from 'zodest/config';
import { processConfig } from 'zodest';
import { generateCommand } from './commands/generate';
import { initCommand } from './commands/init';
import { validateCommand } from './commands/validate';
import { queryCommand } from './commands/query';
import { datasourceListCommand, datasourceInspectCommand, datasourceAnalyzeCommand, datasourceGenerateCommand, dependenciesCommand } from './commands/datasource';
import { deployCommand, localCommand, devCommand, createCommand, pushCommand, pullCommand } from './commands/deploy';
import { log, format } from './utils/terminal';

const globalOptions = defineOptions(
  z.object({
    verbose: z.boolean().default(false),
    help: z.boolean().default(false),
  }),
  {
    v: 'verbose',
    h: 'help',
  }
);

const config = defineConfig({
  name: 'tinykit',
  description: 'TypeSafe CLI for Tinybird - Generate datasources and pipes from functional definitions',
  version: '0.1.0',
  globalOptions,
  commands: {
    init: defineCommand({
      description: 'Initialize a new TinyKit project with example files',
      options: defineOptions(
        z.object({
          dir: z.string().default('./tinybird'),
        }),
        { d: 'dir' }
      ),
      action: initCommand,
    }),

    generate: defineCommand({
      description: 'Generate Tinybird files from TypeScript definitions',
      aliases: ['gen'],
      options: defineOptions(
        z.object({
          file: z.string().optional(),
          dir: z.string().default('./tinybird'),
          watch: z.boolean().default(false),
          dryRun: z.boolean().default(false),
        }),
        {
          f: 'file',
          d: 'dir',
          w: 'watch',
        }
      ),
      action: generateCommand,
    }),

    validate: defineCommand({
      description: 'Validate generated Tinybird files',
      options: defineOptions(
        z.object({
          dir: z.string().default('./tinybird'),
        }),
        { d: 'dir' }
      ),
      action: validateCommand,
    }),

    query: defineCommand({
      description: 'Execute SQL queries against Tinybird',
      aliases: ['sql'],
      options: defineOptions(
        z.object({
          sql: z.string().optional(),
          file: z.string().optional(),
          pipe: z.string().optional(),
          format: z.enum(['json', 'csv', 'human']).default('human'),
          limit: z.number().optional(),
          stats: z.boolean().default(true),
          token: z.string().optional(),
          config: z.string().optional(),
        }),
        {
          s: 'sql',
          f: 'file',
          p: 'pipe',
          l: 'limit',
          t: 'token',
          c: 'config',
        }
      ),
      action: queryCommand,
    }),

    'datasource:list': defineCommand({
      description: 'List all data sources',
      options: defineOptions(
        z.object({
          format: z.enum(['json', 'csv', 'human']).default('human'),
          token: z.string().optional(),
          config: z.string().optional(),
        }),
        { t: 'token', c: 'config' }
      ),
      action: datasourceListCommand,
    }),

    'datasource:inspect': defineCommand({
      description: 'Inspect a data source',
      options: defineOptions(
        z.object({
          name: z.string().optional(),
          format: z.enum(['json', 'csv', 'human']).default('human'),
          token: z.string().optional(),
          config: z.string().optional(),
        }),
        { n: 'name', t: 'token', c: 'config' }
      ),
      action: datasourceInspectCommand,
    }),

    'datasource:analyze': defineCommand({
      description: 'Analyze a file or URL for data source creation',
      options: defineOptions(
        z.object({
          file: z.string().optional(),
          url: z.string().optional(),
          sample_size: z.number().optional(),
          format: z.enum(['json', 'csv', 'human']).default('human'),
          token: z.string().optional(),
          config: z.string().optional(),
        }),
        { f: 'file', u: 'url', s: 'sample_size', t: 'token', c: 'config' }
      ),
      action: datasourceAnalyzeCommand,
    }),

    'datasource:generate': defineCommand({
      description: 'Generate a data source from file or URL',
      options: defineOptions(
        z.object({
          name: z.string().optional(),
          file: z.string().optional(),
          url: z.string().optional(),
          sample_size: z.number().optional(),
          format: z.enum(['json', 'csv', 'human']).default('human'),
          token: z.string().optional(),
          config: z.string().optional(),
        }),
        { n: 'name', f: 'file', u: 'url', s: 'sample_size', t: 'token', c: 'config' }
      ),
      action: datasourceGenerateCommand,
    }),

    dependencies: defineCommand({
      description: 'Show resource dependencies',
      aliases: ['deps'],
      options: defineOptions(
        z.object({
          format: z.enum(['json', 'csv', 'human']).default('human'),
          token: z.string().optional(),
          config: z.string().optional(),
          match: z.string().optional(),
          pipe: z.string().optional(),
          no_deps: z.boolean().default(false),
        }),
        {
          t: 'token',
          c: 'config',
          m: 'match',
          p: 'pipe',
        }
      ),
      action: dependenciesCommand,
    }),

    deploy: defineCommand({
      description: 'Deploy project to Tinybird Cloud',
      options: defineOptions(
        z.object({
          cloud: z.boolean().default(false),
          dry_run: z.boolean().default(false),
          force: z.boolean().default(false),
          token: z.string().optional(),
          config: z.string().optional(),
        }),
        { t: 'token', c: 'config', f: 'force' }
      ),
      action: deployCommand,
    }),

    'local:start': defineCommand({
      description: 'Start local Tinybird development environment',
      options: defineOptions(
        z.object({
          token: z.string().optional(),
          config: z.string().optional(),
        }),
        { t: 'token', c: 'config' }
      ),
      action: (options, args) => localCommand({ ...options, action: 'start' }, args),
    }),

    'local:stop': defineCommand({
      description: 'Stop local Tinybird development environment',
      options: defineOptions(
        z.object({
          token: z.string().optional(),
          config: z.string().optional(),
        }),
        { t: 'token', c: 'config' }
      ),
      action: (options, args) => localCommand({ ...options, action: 'stop' }, args),
    }),

    'local:status': defineCommand({
      description: 'Check local Tinybird development environment status',
      options: defineOptions(
        z.object({
          token: z.string().optional(),
          config: z.string().optional(),
        }),
        { t: 'token', c: 'config' }
      ),
      action: (options, args) => localCommand({ ...options, action: 'status' }, args),
    }),

    dev: defineCommand({
      description: 'Start Tinybird development server',
      options: defineOptions(
        z.object({
          token: z.string().optional(),
          config: z.string().optional(),
        }),
        { t: 'token', c: 'config' }
      ),
      action: devCommand,
    }),

    create: defineCommand({
      description: 'Create a new Tinybird project',
      options: defineOptions(
        z.object({
          name: z.string().optional(),
          token: z.string().optional(),
          config: z.string().optional(),
        }),
        { n: 'name', t: 'token', c: 'config' }
      ),
      action: createCommand,
    }),

    push: defineCommand({
      description: 'Push resources to Tinybird',
      options: defineOptions(
        z.object({
          force: z.boolean().default(false),
          token: z.string().optional(),
          config: z.string().optional(),
        }),
        { f: 'force', t: 'token', c: 'config' }
      ),
      action: pushCommand,
    }),

    pull: defineCommand({
      description: 'Pull resources from Tinybird',
      options: defineOptions(
        z.object({
          force: z.boolean().default(false),
          token: z.string().optional(),
          config: z.string().optional(),
        }),
        { f: 'force', t: 'token', c: 'config' }
      ),
      action: pullCommand,
    }),
  },
} as any);

// Process CLI arguments
try {
  const result = processConfig(config, process.argv.slice(2));

  if (result.globalOptions.help) {
    console.log(generateHelpText());
    process.exit(0);
  }

  if (result.globalOptions.verbose) {
    log.dim(`Running command: ${result._kind}`);
  }

  // Execute the command
  await result.command.action(result.options, result.args);
} catch (error) {
  if (error instanceof z.ZodError) {
    log.error('Validation Error:');
    error.issues.forEach(issue => {
      log.plain(`  • ${issue.message}`);
    });
  } else {
    log.error(error instanceof Error ? error.message : String(error));
  }
  process.exit(1);
}

function generateHelpText(): string {
  return `
${format.bold('TinyKit')} ${format.dim('v0.1.0')}
TypeSafe CLI for Tinybird - Generate datasources and pipes from functional definitions

${format.bold('Usage:')}
  tinykit [global options] <command> [options]

${format.bold('Global Options:')}
  -v, --verbose    Enable verbose output
  -h, --help       Show this help message

${format.bold('Commands:')}
  init                    Initialize a new TinyKit project with example files
  generate, gen           Generate Tinybird files from TypeScript definitions
  validate                Validate generated Tinybird files
  query, sql              Execute SQL queries against Tinybird
  datasource:list         List all data sources
  datasource:inspect      Inspect a data source
  datasource:analyze      Analyze a file or URL for data source creation
  datasource:generate     Generate a data source from file or URL
  dependencies, deps      Show resource dependencies
  deploy                  Deploy project to Tinybird Cloud
  local:start             Start local Tinybird development environment
  local:stop              Stop local Tinybird development environment
  local:status            Check local development environment status
  dev                     Start Tinybird development server
  create                  Create a new Tinybird project
  push                    Push resources to Tinybird
  pull                    Pull resources from Tinybird

${format.bold('Examples:')}
  tinykit init --dir ./my-project
  tinykit generate --file ./src/pipes.ts --dir ./tinybird
  tinykit gen --watch --dry-run
  tinykit validate --dir ./tinybird
  tinykit query "SELECT count() FROM events"
  tinykit sql --file query.sql --format json
  tinykit datasource:list --format human
  tinykit datasource:inspect events_v1
  tinykit dependencies --match "events*"
  tinykit local:start
  tinykit dev
  tinykit deploy --cloud
  tinykit push datasource1 pipe1

For command-specific help, use: tinykit <command> --help
`;
}

export { config };