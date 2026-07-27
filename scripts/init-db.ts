import * as dotenv from 'dotenv';

dotenv.config();

export async function initializeDatabase(): Promise<void> {
  const [{ buildContainer }, { init, shutdown }] = await Promise.all([
    import('../src/container'),
    import('../src/init'),
  ]);
  const container = buildContainer();
  try {
    await init(container);
  } finally {
    await shutdown(container);
  }
}

if (require.main === module) {
  initializeDatabase()
    .then(() => {
      console.log('✅ Database initialization completed!');
      process.exit(0);
    })
    .catch((error) => {
      console.error('❌ Database initialization failed:', error);
      process.exit(1);
    });
}
