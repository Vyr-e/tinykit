// Simple terminal styling utilities without external dependencies
export const colors = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  dim: '\x1b[2m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  magenta: '\x1b[35m',
  cyan: '\x1b[36m',
  white: '\x1b[37m',
};

export const log = {
  info: (message: string) => console.log(`${colors.blue}ℹ${colors.reset} ${message}`),
  success: (message: string) => console.log(`${colors.green}✅${colors.reset} ${message}`),
  warning: (message: string) => console.log(`${colors.yellow}⚠${colors.reset} ${message}`),
  error: (message: string) => console.error(`${colors.red}❌${colors.reset} ${message}`),
  dim: (message: string) => console.log(`${colors.dim}${message}${colors.reset}`),
  bold: (message: string) => console.log(`${colors.bright}${message}${colors.reset}`),
  plain: (message: string) => console.log(message),
};

export const format = {
  bold: (text: string) => `${colors.bright}${text}${colors.reset}`,
  dim: (text: string) => `${colors.dim}${text}${colors.reset}`,
  red: (text: string) => `${colors.red}${text}${colors.reset}`,
  green: (text: string) => `${colors.green}${text}${colors.reset}`,
  yellow: (text: string) => `${colors.yellow}${text}${colors.reset}`,
  blue: (text: string) => `${colors.blue}${text}${colors.reset}`,
  cyan: (text: string) => `${colors.cyan}${text}${colors.reset}`,
};