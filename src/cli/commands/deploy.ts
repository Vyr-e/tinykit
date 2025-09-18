import { execSync, spawn } from 'child_process';
import { log } from '../utils/terminal';
import { readFileSync, existsSync } from 'fs';
import { join } from 'path';

export interface DeployOptions {
  cloud?: boolean;
  local?: boolean;
  dry_run?: boolean;
  force?: boolean;
  token?: string;
  config?: string;
}

export interface LocalOptions {
  action: 'start' | 'stop' | 'status';
  token?: string;
  config?: string;
}

export async function deployCommand(options: DeployOptions, args: string[]) {
  try {
    await ensureTinybirdCLI();
    const env = await prepareEnvironment(options);
    
    const tbArgs = ['deploy'];
    
    if (options.cloud) {
      tbArgs.push('--cloud');
    }
    
    if (options.dry_run) {
      tbArgs.push('--dry-run');
    }
    
    if (options.force) {
      tbArgs.push('--force');
    }
    
    log.dim(`Running: tb ${tbArgs.join(' ')}`);
    await executeTbCommand(tbArgs, env);

  } catch (error) {
    log.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  }
}

export async function localCommand(options: LocalOptions, args: string[]) {
  try {
    await ensureTinybirdCLI();
    const env = await prepareEnvironment(options);
    
    const tbArgs = ['local', options.action];
    
    log.dim(`Running: tb ${tbArgs.join(' ')}`);
    await executeTbCommand(tbArgs, env);

  } catch (error) {
    log.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  }
}

export async function devCommand(options: { token?: string; config?: string }, args: string[]) {
  try {
    await ensureTinybirdCLI();
    const env = await prepareEnvironment(options);
    
    const tbArgs = ['dev'];
    
    log.dim(`Running: tb ${tbArgs.join(' ')}`);
    log.plain('Starting Tinybird development server...');
    log.plain('Press Ctrl+C to stop');
    
    await executeTbCommand(tbArgs, env);

  } catch (error) {
    log.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  }
}

export async function createCommand(options: { name?: string; token?: string; config?: string }, args: string[]) {
  try {
    await ensureTinybirdCLI();
    const env = await prepareEnvironment(options);
    
    const projectName = options.name || args[0];
    const tbArgs = ['create'];
    
    if (projectName) {
      tbArgs.push(projectName);
    }
    
    log.dim(`Running: tb ${tbArgs.join(' ')}`);
    await executeTbCommand(tbArgs, env);

  } catch (error) {
    log.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  }
}

export async function pushCommand(options: { force?: boolean; token?: string; config?: string }, args: string[]) {
  try {
    await ensureTinybirdCLI();
    const env = await prepareEnvironment(options);
    
    const tbArgs = ['push'];
    
    if (options.force) {
      tbArgs.push('--force');
    }
    
    // Add any resource names from args
    tbArgs.push(...args);
    
    log.dim(`Running: tb ${tbArgs.join(' ')}`);
    await executeTbCommand(tbArgs, env);

  } catch (error) {
    log.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  }
}

export async function pullCommand(options: { force?: boolean; token?: string; config?: string }, args: string[]) {
  try {
    await ensureTinybirdCLI();
    const env = await prepareEnvironment(options);
    
    const tbArgs = ['pull'];
    
    if (options.force) {
      tbArgs.push('--force');
    }
    
    // Add any resource names from args
    tbArgs.push(...args);
    
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