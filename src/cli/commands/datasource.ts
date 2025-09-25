import { execSync, spawn } from 'child_process';
import { log } from '../utils/terminal';
import { readFileSync, existsSync } from 'fs';
import { join } from 'path';

export interface DatasourceOptions {
  name?: string;
  format: 'json' | 'csv' | 'human';
  token?: string;
  config?: string;
  file?: string;
  url?: string;
  sample_size?: number;
}

export interface DatasourceListOptions {
  format: 'json' | 'csv' | 'human';
  token?: string;
  config?: string;
}

export async function datasourceListCommand(options: DatasourceListOptions, args: string[]) {
  try {
    await ensureTinybirdCLI();
    const env = await prepareEnvironment(options);
    
    const tbArgs = ['datasource', 'ls'];
    tbArgs.push('--format', options.format);
    
    log.dim(`Running: tb ${tbArgs.join(' ')}`);
    await executeTbCommand(tbArgs, env);

  } catch (error) {
    log.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  }
}

export async function datasourceInspectCommand(options: DatasourceOptions, args: string[]) {
  try {
    await ensureTinybirdCLI();
    const env = await prepareEnvironment(options);
    
    const datasourceName = options.name || args[0];
    if (!datasourceName) {
      log.error('Datasource name is required. Use --name or provide as argument.');
      process.exit(1);
    }
    
    const tbArgs = ['datasource', 'get', datasourceName];
    tbArgs.push('--format', options.format);
    
    log.dim(`Running: tb ${tbArgs.join(' ')}`);
    await executeTbCommand(tbArgs, env);

  } catch (error) {
    log.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  }
}

export async function datasourceAnalyzeCommand(options: DatasourceOptions, args: string[]) {
  try {
    await ensureTinybirdCLI();
    const env = await prepareEnvironment(options);
    
    const tbArgs = ['datasource', 'analyze'];
    
    if (options.file) {
      tbArgs.push('--file', options.file);
    } else if (options.url) {
      tbArgs.push('--url', options.url);
    } else if (args.length > 0) {
      // Assume first arg is file or URL
      const input = args[0];
      if (input && input.startsWith('http://') || input && input.startsWith('https://')) {
        tbArgs.push('--url', input);
      } else if (input) {
        tbArgs.push('--file', input);
      }
    } else {
      log.error('File or URL is required for analysis. Use --file, --url, or provide as argument.');
      process.exit(1);
    }
    
    if (options.sample_size) {
      tbArgs.push('--sample-size', options.sample_size.toString());
    }
    
    tbArgs.push('--format', options.format);
    
    log.dim(`Running: tb ${tbArgs.join(' ')}`);
    await executeTbCommand(tbArgs, env);

  } catch (error) {
    log.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  }
}

export async function datasourceGenerateCommand(options: DatasourceOptions, args: string[]) {
  try {
    await ensureTinybirdCLI();
    const env = await prepareEnvironment(options);
    
    const tbArgs = ['datasource', 'generate'];
    
    if (options.file) {
      tbArgs.push('--file', options.file);
    } else if (options.url) {
      tbArgs.push('--url', options.url);
    } else if (args.length > 0) {
      const input = args[0];
      if (input && input.startsWith('http://') || input && input.startsWith('https://')) {
        tbArgs.push('--url', input);
      } else if (input) {
        tbArgs.push('--file', input);
      }
    } else {
      log.error('File or URL is required for generation. Use --file, --url, or provide as argument.');
      process.exit(1);
    }
    
    if (options.name) {
      tbArgs.push('--name', options.name);
    }
    
    if (options.sample_size) {
      tbArgs.push('--sample-size', options.sample_size.toString());
    }
    
    tbArgs.push('--format', options.format);
    
    log.dim(`Running: tb ${tbArgs.join(' ')}`);
    await executeTbCommand(tbArgs, env);

  } catch (error) {
    log.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  }
}

export async function dependenciesCommand(options: { format: 'json' | 'csv' | 'human'; token?: string; config?: string; match?: string; pipe?: string; no_deps?: boolean }, args: string[]) {
  try {
    await ensureTinybirdCLI();
    const env = await prepareEnvironment(options);
    
    const tbArgs = ['dependencies'];
    
    if (options.match) {
      tbArgs.push('--match', options.match);
    }
    
    if (options.pipe) {
      tbArgs.push('--pipe', options.pipe);
    }
    
    if (options.no_deps) {
      tbArgs.push('--no-deps');
    }
    
    tbArgs.push('--format', options.format);
    
    log.dim(`Running: tb ${tbArgs.join(' ')}`);
    await executeTbCommand(tbArgs, env);

  } catch (error) {
    log.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  }
}

async function ensureTinybirdCLI(): Promise<void> {
  try {
    execSync('tb --version', { stdio: 'ignore' });
  } catch {
    log.error('Tinybird CLI not found. Please install it first:');
    log.plain('  curl https://tinybird.co | sh');
    log.plain('');
    log.plain('Then authenticate with:');
    log.plain('  tb login');
    process.exit(1);
  }
}

async function prepareEnvironment(options: { token?: string; config?: string }): Promise<NodeJS.ProcessEnv> {
  const env = { ...process.env };
  
  const token = getToken(options);
  if (token) {
    env.TB_TOKEN = token;
  }
  
  return env;
}

async function executeTbCommand(args: string[], env: NodeJS.ProcessEnv): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = spawn('tb', args, {
      stdio: 'inherit',
      env,
      shell: process.platform === 'win32'
    });

    child.on('close', (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`Tinybird CLI exited with code ${code}`));
      }
    });

    child.on('error', (error) => {
      if (error.message.includes('ENOENT')) {
        reject(new Error('Tinybird CLI not found. Please install tinybird-cli first.'));
      } else {
        reject(error);
      }
    });
  });
}

function getToken(options: { token?: string; config?: string }): string | undefined {
  if (options.token) {
    return options.token;
  }

  if (process.env.TINYBIRD_TOKEN) {
    return process.env.TINYBIRD_TOKEN;
  }

  if (options.config) {
    return readTokenFromConfig(options.config);
  }

  const defaultConfigs = [
    '.tinykitrc',
    '.tinybird',
    ".tinyb",
    join(process.env.HOME || process.env.USERPROFILE || '', '.tinykitrc')
  ];

  for (const configPath of defaultConfigs) {
    if (existsSync(configPath)) {
      const token = readTokenFromConfig(configPath);
      if (token) return token;
    }
  }

  return undefined;
}

function readTokenFromConfig(configPath: string): string | undefined {
  try {
    const content = readFileSync(configPath, 'utf-8');
    
    try {
      const json = JSON.parse(content);
      return json.token || json.TINYBIRD_TOKEN;
    } catch {
      const lines = content.split('\n');
      for (const line of lines) {
        const trimmed = line.trim();
        if (trimmed.startsWith('token=') || trimmed.startsWith('TINYBIRD_TOKEN=')) {
          return trimmed.split('=')[1]?.trim();
        }
      }
    }
  } catch {
    // Ignore config read errors
  }
  
  return undefined;
}
