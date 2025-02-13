import { Result } from '@badrap/result';

import {
  CreateIndexResponse,
  Index,
  isCreateIndexRequest,
  isReadIndexRequest,
  isRemoveIndexRequest,
  isUpdateIndexRequest,
  ListIndexResponse,
  ReadIndexResponse,
  RemoveIndexResponse,
  UpdateIndexResponse,
} from './index-request';
import {
  StorageError,
  StorageNotFoundError,
  StorageUnknownOperationError,
} from '../storage-errors';
import { RequestInfo } from '../storage-request';

export class IndexHandler {
  public indexes: Map<string, Index>;

  constructor(private state: DurableObjectState) {
    this.indexes = new Map();
    this.syncIndexes();
  }

  async handle(info: RequestInfo) {
    switch (info.operation) {
      case 'create': {
        return await this.createIndex(info);
      }
      case 'update': {
        return await this.updateIndex(info);
      }
      case 'read': {
        return await this.readIndex(info);
      }
      case 'remove': {
        return await this.removeIndex(info);
      }
      case 'list': {
        return await this.listIndexes();
      }
      default: {
        return Result.err(new StorageUnknownOperationError());
      }
    }
  }

  private async createIndex(
    info: RequestInfo,
  ): Promise<Result<CreateIndexResponse | StorageError>> {
    const input = info.body(isCreateIndexRequest);
    const key = `idx:${input.property}`;
    const value = { ...input, id: key };

    await this.state.storage.put(key, value);

    this.syncIndexes();

    return Result.ok(value);
  }

  private async updateIndex(
    info: RequestInfo,
  ): Promise<Result<UpdateIndexResponse | StorageError>> {
    const input = info.body(isUpdateIndexRequest);
    const value = { ...input, id: `idx:${input.property}` };

    await this.state.storage.put(input.id, value);

    this.syncIndexes();

    return Result.ok(value);
  }

  private async readIndex(
    info: RequestInfo,
  ): Promise<Result<ReadIndexResponse | StorageError>> {
    const input = info.body(isReadIndexRequest);
    const index = await this.state.storage.get<Index>(input.id, {
      allowConcurrency: true,
    });
    if (!index) {
      return Result.err(new StorageNotFoundError());
    }

    return Result.ok(index);
  }

  private async removeIndex(
    info: RequestInfo,
  ): Promise<Result<RemoveIndexResponse | StorageError>> {
    const input = info.body(isRemoveIndexRequest);
    const deleted = await this.state.storage.delete(input.id);

    this.syncIndexes();

    return Result.ok({ success: deleted });
  }

  private async listIndexes(): Promise<Result<ListIndexResponse | StorageError>> {
    const indexes = await this.state.storage.list<Index>({
      prefix: 'idx:',
      allowConcurrency: true,
    });

    return Result.ok(Object.fromEntries(indexes));
  }

  syncIndexes() {
    this.state.blockConcurrencyWhile(async () => {
      const indexes = await this.state.storage.list<Index>({
        prefix: 'idx:',
      });

      this.indexes = indexes;
    });
  }

  enhanceWritePayload<T = Record<string, string | number>>(
    key: string,
    value: T,
  ): [string, T][] {
    const properties = Object.keys(value as object);
    const indexes = this.getApplicableIndexes(properties);

    const items: Record<string, typeof value> = { [key]: value };
    indexes.forEach((index) => {
      const indexKey = (value as any)[index.property];
      items[this.keyForIndex(index.property, indexKey)] = value;
    });

    return Object.entries(items);
  }

  getIndexedKeys<T = Record<string, string | number>>(value: T): string[] {
    const properties = Object.keys(value as object);
    const indexes = this.getApplicableIndexes(properties);

    return indexes.map((index) =>
      this.keyForIndex(index.property, (value as any)[index.property]),
    );
  }

  enhanceDeletePayload(key: string, toDelete: string[]) {
    this.indexes.forEach((index) => {
      toDelete.push(this.keyForIndex(index.property, key));
    });
  }

  computeDanglingKeys<T>(currentValue: T, updatedValue: T): string[] {
    const indexedKeysFromCurrent = this.getIndexedKeys(currentValue);
    const indexedKeysAfterUpdate = this.getIndexedKeys(updatedValue);

    return indexedKeysFromCurrent.filter((key) => !indexedKeysAfterUpdate.includes(key));
  }

  private getApplicableIndexes(properties: string[]) {
    return properties
      .map((property) => this.indexes.get(`idx:${property}`))
      .filter((v): v is Index => !!v);
  }

  private keyForIndex(indexName: string, key: string) {
    return `${indexName}--${key}`;
  }
}
