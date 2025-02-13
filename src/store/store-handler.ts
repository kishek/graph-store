import { Result } from '@badrap/result';
import { StorageNotFoundError, StorageUnknownOperationError } from '../storage-errors';
import { RequestInfo } from '../storage-request';
import { StorageEnvironment } from 'src/storage-environment';
import { isRestoreRequest } from './store-request';
import { BatchedStorage } from '../batched-storage';
import { InMemoryReadCache } from '../cache/read-cache';

export class StoreHandler {
  constructor(
    private state: DurableObjectState,
    private env: StorageEnvironment,
    cache: InMemoryReadCache,
    private batcher = new BatchedStorage(state, cache),
  ) {}

  async handle(info: RequestInfo) {
    switch (info.operation) {
      case 'backup': {
        return await this.backup();
      }
      case 'restore': {
        return await this.restore(info);
      }
      default: {
        return Result.err(new StorageUnknownOperationError());
      }
    }
  }

  private async backup(reason?: string) {
    const id = this.state.id;
    const time = Date.now();
    const suffix = reason ? `-${reason}` : '';

    const name = `${id}/graph-store-${time}${suffix}.json`;

    const data = await this.state.storage.list();
    const entries = Object.fromEntries(data.entries());

    console.info({
      msg: 'backing up data',
      name,
      entriesCount: data.size,
    });

    await this.env.GRAPH_STORAGE_BACKUP.put(name, JSON.stringify(entries));

    return Result.ok(name);
  }

  private async restore(info: RequestInfo) {
    const input = info.body(isRestoreRequest);

    const backup = await this.env.GRAPH_STORAGE_BACKUP.get(input.backupId);
    if (!backup) {
      return Result.err(new StorageNotFoundError());
    }

    const body = (await backup.json()) as Record<string, string>;

    // do a backup before restore in case of any issues
    await this.backup('before-restore');
    await this.state.storage.deleteAll();

    this.batcher.doChunkedWrite(body);

    return Result.ok({ count: Object.keys(body).length });
  }
}
