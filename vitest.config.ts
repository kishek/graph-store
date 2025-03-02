import { defineWorkersConfig } from '@cloudflare/vitest-pool-workers/config';

export default defineWorkersConfig({
  test: {
    poolOptions: {
      workers: {
        wrangler: { configPath: './example/wrangler.toml' },
        miniflare: {
          r2Buckets: ['GRAPH_STORAGE_BACKUP'],
        },
      },
    },
  },
});
