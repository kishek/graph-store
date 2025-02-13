import chunk from 'lodash.chunk';
import { InMemoryReadCache } from './cache/read-cache';

const ITEM_LIMIT = 128;

export class BatchedStorage {
  public constructor(
    private state: DurableObjectState,
    private cache: InMemoryReadCache,
  ) {}

  async doChunkedRead<T>(keys: string[]): Promise<Map<string, T | undefined>> {
    const chunks = keys.length > ITEM_LIMIT ? chunk(keys, ITEM_LIMIT) : [keys];
    const items = new Map<string, T | undefined>();

    await Promise.all(
      chunks.map(async (chunk) => {
        for (const key of chunk) {
          const cached = this.cache.get<T>(key);
          if (cached) {
            items.set(key, cached);
          }
        }

        const missing = chunk.filter((key) => !items.has(key));
        if (missing.length === 0) {
          return;
        }

        const results = await this.state.storage.get<T>(missing, {
          allowConcurrency: true,
        });
        for (const [k, v] of results) {
          items.set(k, v);
          this.cache.set(k, v);
        }
      }),
    );

    for (const key of keys) {
      if (!items.has(key)) {
        items.set(key, undefined);
        this.cache.set(key, undefined);
      }
    }

    return items;
  }

  async doChunkedWrite<T>(entries: Record<string, T>) {
    this.cache.deleteAll();

    const keysToUpdate = Object.keys(entries);

    if (keysToUpdate.length === 0) {
      return;
    }

    const updateChunks =
      keysToUpdate.length > ITEM_LIMIT ? chunk(keysToUpdate, ITEM_LIMIT) : [keysToUpdate];

    await Promise.all(
      updateChunks.map(async (chunk) => {
        const updates = Object.fromEntries(chunk.map((key) => [key, entries[key]]));
        await this.state.storage.put(updates);

        for (const [key, value] of Object.entries(updates)) {
          this.cache.set(key, value);
        }
      }),
    );
  }

  async doChunkedDelete(keys: Set<string>) {
    this.cache.deleteAll();

    const keysToDelete = Array.from(keys);

    if (keysToDelete.length === 0) {
      return;
    }

    const deleteChunks =
      keysToDelete.length > ITEM_LIMIT ? chunk(keysToDelete, ITEM_LIMIT) : [keysToDelete];

    await Promise.all(deleteChunks.map((chunk) => this.state.storage.delete(chunk)));
  }
}
