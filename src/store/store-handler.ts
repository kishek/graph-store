import { Result } from '@badrap/result';
import { StorageUnknownOperationError } from '../storage-errors';
import { RequestInfo } from '../storage-request';
import { StorageEnvironment } from 'src/storage-environment';

export class StoreHandler {
  constructor(private state: DurableObjectState, private env: StorageEnvironment) {}

  async handle(info: RequestInfo) {
    switch (info.operation) {
      case 'backup': {
        return await this.backup();
      }
      default: {
        return Result.err(new StorageUnknownOperationError());
      }
    }
  }

  private async backup() {
    const id = this.state.id;
    const time = Date.now();

    const name = `${id}/graph-store-${time}.json`;

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
}
