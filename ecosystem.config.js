module.exports = {
  apps: [
    {
      name: 'enfyra-cluster',
      script: './dist/src/main.js',
      instances: 4,
      exec_mode: 'cluster',
      env: {
        // Database Settings
        DB_TYPE: 'mysql',
        DB_HOST: 'localhost',
        DB_PORT: 3306,
        DB_USERNAME: 'root',
        DB_PASSWORD: '1234',
        DB_NAME: 'enfyra',

        // MongoDB URI
        MONGO_URI: 'mongodb://enfyra_admin:enfyra_password_123@localhost:27017/enfyra?authSource=admin',

        // Database Connection Pool Settings
        DB_POOL_SIZE: 100,
        DB_CONNECTION_LIMIT: 100,
        DB_ACQUIRE_TIMEOUT: 60000,
        DB_IDLE_TIMEOUT: 30000,

        // Redis Settings
        REDIS_URI: 'redis://localhost:6379',
        DEFAULT_TTL: 5,

        // App Settings
        NODE_NAME: 'my_enfyra',
        PORT: 1105,
        DEFAULT_HANDLER_TIMEOUT: 10000,
        DEFAULT_PREHOOK_TIMEOUT: 10000,
        DEFAULT_AFTERHOOK_TIMEOUT: 10000,

        // Auth Settings
        SECRET_KEY: 'my_secret',
        SALT_ROUNDS: 10,
        ACCESS_TOKEN_EXP: '15m',
        REFRESH_TOKEN_NO_REMEMBER_EXP: '1d',
        REFRESH_TOKEN_REMEMBER_EXP: '7d',

        // Package Manager
        PACKAGE_MANAGER: 'yarn',

        // Node Environment
        NODE_ENV: 'production'
      },

      // PM2 Options
      max_memory_restart: '1G',
      error_file: './logs/err.log',
      out_file: './logs/out.log',
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true,

      // Auto-restart on file changes (disable in production)
      watch: false,

      // Graceful shutdown
      kill_timeout: 5000,
      wait_ready: true,
      listen_timeout: 10000,

      // Auto-restart settings
      autorestart: true,
      max_restarts: 10,
      min_uptime: '10s'
    }
  ]
};
