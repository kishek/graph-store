import { Result } from '@badrap/result';

import {
  CreateIndexRequest,
  CreateIndexResponse,
  Index,
  isCreateIndexRequest,
  isReadIndexRequest,
  isRemoveIndexRequest,
  isUpdateIndexRequest,
  ListIndexResponse,
  ReadIndexRequest,
  ReadIndexResponse,
  RemoveIndexRequest,
  RemoveIndexResponse,
  UpdateIndexRequest,
  UpdateIndexResponse,
} from './index-request';
import {
  StorageError,
  StorageNotFoundError,
  StorageUnknownOperationError,
} from '../storage-errors';
import { enforce, IndexStorageRequest } from '../storage-request';

export class IndexHandler {
  public indexes: Map<string, Index>;

  constructor(private state: DurableObjectState) {
    this.indexes = new Map();
    this.syncIndexes();
  }

  async handle(opts: IndexStorageRequest) {
    switch (opts.operation) {
      case 'create': {
        return await this.createIndex(opts.request);
      }
      case 'update': {
        return await this.updateIndex(opts.request);
      }
      case 'read': {
        return await this.readIndex(opts.request);
      }
      case 'remove': {
        return await this.removeIndex(opts.request);
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
    input: CreateIndexRequest,
  ): Promise<Result<CreateIndexResponse | StorageError>> {
    enforce(input, isCreateIndexRequest);

    const key = `idx:${input.property}`;
    const value = { ...input, id: key };

    await this.state.storage.put(key, value);

    this.syncIndexes();

    return Result.ok(value);
  }

  private async updateIndex(
    input: UpdateIndexRequest,
  ): Promise<Result<UpdateIndexResponse | StorageError>> {
    enforce(input, isUpdateIndexRequest);

    const value = { ...input, id: `idx:${input.property}` };

    await this.state.storage.put(input.id, value);

    this.syncIndexes();

    return Result.ok(value);
  }

  private async readIndex(
    input: ReadIndexRequest,
  ): Promise<Result<ReadIndexResponse | StorageError>> {
    enforce(input, isReadIndexRequest);

    const index = await this.state.storage.get<Index>(input.id, {
      allowConcurrency: true,
    });
    if (!index) {
      return Result.err(new StorageNotFoundError());
    }

    return Result.ok(index);
  }

  private async removeIndex(
    input: RemoveIndexRequest,
  ): Promise<Result<RemoveIndexResponse | StorageError>> {
    enforce(input, isRemoveIndexRequest);

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
