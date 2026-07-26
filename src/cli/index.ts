#!/usr/bin/env node

import { Command, Option } from 'commander';
import { generateCommand } from './commands/generate.js';
import { initCommand } from './commands/init.js';
import { validateCommand } from './commands/validate.js';
import { queryCommand } from './commands/query.js';
import {
  datasourceListCommand,
  datasourceInspectCommand,
  datasourceAnalyzeCommand,
  datasourceGenerateCommand,
  dependenciesCommand,
} from './commands/datasource.js';
import {
  deployCommand,
  localCommand,
  devCommand,
  createCommand,
  pushCommand,
  pullCommand,
} from './commands/deploy.js';
import { log } from './utils/terminal.js';

const VERSION = '0.3.1';

const program = new Command()
  .name('tinykit')
  .description(
    'Type-safe Tinybird client and generator for functional TypeScript definitions'
  )
  .version(VERSION)
  .option('-v, --verbose', 'enable verbose output')
  .showHelpAfterError();

function number(value: string): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw new Error(`Expected a number, received "${value}"`);
  }
  return parsed;
}

function collect(value: string, previous: string[]): string[] {
  return [...previous, value];
}

function logCommand(command: string): void {
  if (program.opts<{ verbose?: boolean }>().verbose) {
    log.dim(`Running command: ${command}`);
  }
}

program
  .command('init')
  .description('initialize a new TinyKit project with example files')
  .option('-d, --dir <path>', 'project directory', './tinybird')
  .action(async (options) => {
    logCommand('init');
    await initCommand(options);
  });

program
  .command('generate')
  .alias('gen')
  .description('generate Tinybird files from TypeScript definitions')
  .option(
    '-f, --file <path>',
    'TypeScript entry file (repeat for multiple entries)',
    collect,
    []
  )
  .option('-d, --dir <path>', 'output directory', './tinybird')
  .option('-w, --watch', 'watch entry files for changes', false)
  .option('--dry-run', 'print the generation result without writing files', false)
  .action(async (options) => {
    logCommand('generate');
    await generateCommand(options);
  });

program
  .command('validate')
  .description('validate generated Tinybird files')
  .option('-d, --dir <path>', 'Tinybird project directory', './tinybird')
  .action(async (options) => {
    logCommand('validate');
    await validateCommand(options);
  });

program
  .command('query')
  .alias('sql')
  .description('execute SQL queries against Tinybird')
  .argument('[query...]', 'SQL query or endpoint parameters')
  .option('-s, --sql <query>', 'SQL query')
  .option('-f, --file <path>', 'SQL file')
  .option('-p, --pipe <name>', 'endpoint name')
  .addOption(
    new Option('--format <format>', 'output format')
      .choices(['json', 'csv', 'human'])
      .default('human')
  )
  .option('-l, --limit <number>', 'row limit', number)
  .option('--no-stats', 'hide query statistics')
  .option('-t, --token <token>', 'Tinybird token')
  .option('-c, --config <path>', 'TinyKit config file')
  .action(async (query, options) => {
    logCommand('query');
    await queryCommand(options, query);
  });

program
  .command('datasource:list')
  .description('list all data sources')
  .addOption(
    new Option('--format <format>', 'output format')
      .choices(['json', 'csv', 'human'])
      .default('human')
  )
  .option('-t, --token <token>', 'Tinybird token')
  .option('-c, --config <path>', 'TinyKit config file')
  .action(async (options) => {
    logCommand('datasource:list');
    await datasourceListCommand(options, []);
  });

program
  .command('datasource:inspect')
  .description('inspect a data source')
  .argument('[name]', 'data source name')
  .option('-n, --name <name>', 'data source name')
  .addOption(
    new Option('--format <format>', 'output format')
      .choices(['json', 'csv', 'human'])
      .default('human')
  )
  .option('-t, --token <token>', 'Tinybird token')
  .option('-c, --config <path>', 'TinyKit config file')
  .action(async (name, options) => {
    logCommand('datasource:inspect');
    await datasourceInspectCommand(options, name ? [name] : []);
  });

program
  .command('datasource:analyze')
  .description('analyze a file or URL for data source creation')
  .argument('[input]', 'file or URL')
  .option('-f, --file <path>', 'input file')
  .option('-u, --url <url>', 'input URL')
  .option('-s, --sample-size <number>', 'sample size', number)
  .addOption(
    new Option('--format <format>', 'output format')
      .choices(['json', 'csv', 'human'])
      .default('human')
  )
  .option('-t, --token <token>', 'Tinybird token')
  .option('-c, --config <path>', 'TinyKit config file')
  .action(async (input, options) => {
    logCommand('datasource:analyze');
    await datasourceAnalyzeCommand(
      { ...options, sample_size: options.sampleSize },
      input ? [input] : []
    );
  });

program
  .command('datasource:generate')
  .description('generate a data source from a file or URL')
  .argument('[input]', 'file or URL')
  .option('-n, --name <name>', 'data source name')
  .option('-f, --file <path>', 'input file')
  .option('-u, --url <url>', 'input URL')
  .option('-s, --sample-size <number>', 'sample size', number)
  .addOption(
    new Option('--format <format>', 'output format')
      .choices(['json', 'csv', 'human'])
      .default('human')
  )
  .option('-t, --token <token>', 'Tinybird token')
  .option('-c, --config <path>', 'TinyKit config file')
  .action(async (input, options) => {
    logCommand('datasource:generate');
    await datasourceGenerateCommand(
      { ...options, sample_size: options.sampleSize },
      input ? [input] : []
    );
  });

program
  .command('dependencies')
  .alias('deps')
  .description('show resource dependencies')
  .addOption(
    new Option('--format <format>', 'output format')
      .choices(['json', 'csv', 'human'])
      .default('human')
  )
  .option('-m, --match <pattern>', 'resource pattern')
  .option('-p, --pipe <name>', 'pipe name')
  .option('--no-deps', 'exclude dependencies')
  .option('-t, --token <token>', 'Tinybird token')
  .option('-c, --config <path>', 'TinyKit config file')
  .action(async (options) => {
    logCommand('dependencies');
    await dependenciesCommand(
      { ...options, no_deps: options.deps === false },
      []
    );
  });

program
  .command('deploy')
  .description('deploy the project with the installed Tinybird CLI')
  .option('--cloud', 'use Tinybird Cloud')
  .option('--check', 'validate the deployment without applying it')
  .option(
    '--allow-destructive-operations',
    'allow destructive schema changes'
  )
  .option('-t, --token <token>', 'Tinybird token')
  .option('-c, --config <path>', 'TinyKit config file')
  .action(async (options) => {
    logCommand('deploy');
    await deployCommand(options, []);
  });

for (const action of ['start', 'stop', 'status'] as const) {
  program
    .command(`local:${action}`)
    .description(`${action} the local Tinybird development environment`)
    .option('-t, --token <token>', 'Tinybird token')
    .option('-c, --config <path>', 'TinyKit config file')
    .action(async (options) => {
      logCommand(`local:${action}`);
      await localCommand({ ...options, action }, []);
    });
}

program
  .command('dev')
  .description('start the Tinybird development server')
  .option('-t, --token <token>', 'Tinybird token')
  .option('-c, --config <path>', 'TinyKit config file')
  .action(async (options) => {
    logCommand('dev');
    await devCommand(options, []);
  });

program
  .command('create')
  .description('create a Tinybird project with the installed Tinybird CLI')
  .argument('[name]', 'project name')
  .option('-n, --name <name>', 'project name')
  .option('-t, --token <token>', 'Tinybird token')
  .option('-c, --config <path>', 'TinyKit config file')
  .action(async (name, options) => {
    logCommand('create');
    await createCommand(options, name ? [name] : []);
  });

program
  .command('push')
  .description('push resources with the installed Tinybird CLI')
  .argument('[resources...]', 'resource names')
  .option('-f, --force', 'force the operation')
  .option('-t, --token <token>', 'Tinybird token')
  .option('-c, --config <path>', 'TinyKit config file')
  .action(async (resources, options) => {
    logCommand('push');
    await pushCommand(options, resources);
  });

program
  .command('pull')
  .description('pull resources with the installed Tinybird CLI')
  .argument('[resources...]', 'resource names')
  .option('-f, --force', 'force the operation')
  .option('-t, --token <token>', 'Tinybird token')
  .option('-c, --config <path>', 'TinyKit config file')
  .action(async (resources, options) => {
    logCommand('pull');
    await pullCommand(options, resources);
  });

try {
  await program.parseAsync(process.argv);
} catch (error) {
  log.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
}

export { program };
