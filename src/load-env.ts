import { config } from 'dotenv';

const quiet = process.env.DOTENV_QUIET ?? process.env.DOTENV_CONFIG_QUIET;
config({ quiet: quiet === undefined || !['false', '0', 'no', 'off', ''].includes(quiet.toLowerCase()) });
