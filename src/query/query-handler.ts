import { Result } from '@badrap/result';

import {
  isCreateQueryRequest,
  isReadQueryRequest,
  isUpdateQueryRequest,
  isRemoveQueryRequest,
  isListQueryRequest,
  QueryResponse,
  CreateQueryRequest,
  UpdateQueryRequest,
  CreateQueryResponse,
  ReadQueryResponse,
  UpdateQueryResponse,
  RemoveQueryResponse,
  ListQueryResponse,
  BatchReadQueryResponse,
  isBatchReadQueryRequest,
  BatchCreateQueryResponse,
  isBatchCreateQueryRequest,
  BatchCreateQueryRequest,
  BatchUpdateQueryRequest,
  isBatchUpdateQueryRequest,
  BatchUpdateQueryResponse,
  BatchUpsertQueryResponse,
  BatchUpsertQueryRequest,
  isBatchUpsertQueryRequest,
  BatchRemoveQueryResponse,
  isBatchRemoveQueryRequest,
} from './query-request';
import { IndexHandler } from '../index/index-handler';
import { RelationshipHandler } from '../relationship/relationship-handler';
import {
  StorageDeleteFailedError,
  StorageError,
  StorageNotFoundError,
  StorageUnknownOperationError,
} from '../storage-errors';
import { RequestInfo } from '../storage-request';
import { BatchedStorage } from '../batched-storage';
import { InMemoryReadCache } from '../cache/read-cache';

export class QueryHandler {
  constructor(
    private state: DurableObjectState,
    private indexHandler: IndexHandler,
    private relationshipHandler: RelationshipHandler,
    private cache: InMemoryReadCache,
    private batcher = new BatchedStorage(state, cache),
  ) {}

  async handle<T extends { id?: string }>(info: RequestInfo) {
    switch (info.operation) {
      case 'create': {
        this.cache.deleteAll();
        return await this.createQuery<T>(info);
      }
      case 'batchCreate': {
        this.cache.deleteAll();
        return await this.batchCreateQuery<T>(info);
      }
      case 'read': {
        return await this.readQuery<T>(info);
      }
      case 'batchRead': {
        return await this.batchReadQuery<T>(info);
      }
      case 'update': {
        this.cache.deleteAll();
        return await this.updateQuery<T>(info);
      }
      case 'batchUpdate': {
        this.cache.deleteAll();
        return await this.batchUpdateQuery<T>(info);
      }
      case 'batchUpsert': {
        this.cache.deleteAll();
        return await this.batchUpsertQuery<T>(info);
      }
      case 'remove': {
        this.cache.deleteAll();
        return await this.removeQuery(info);
      }
      case 'batchRemove': {
        this.cache.deleteAll();
        return await this.batchRemoveQuery(info);
      }
      case 'list': {
        return await this.listQuery<T>(info);
      }
      case 'purge': {
        this.cache.deleteAll();
        return await this.purgeAllQuery();
      }
      default: {
        return Result.err(new StorageUnknownOperationError());
      }
    }
  }

  private async createQuery<T extends { id?: string }>(
    info: RequestInfo,
  ): Promise<Result<CreateQueryResponse<T>, StorageError>> {
    const input = info.body<CreateQueryRequest<T>>(isCreateQueryRequest);
    const value = { ...input.value, id: input.value.id ?? input.key };
    const items = this.indexHandler.enhanceWritePayload(input.key, value);

    await this.state.storage.transaction(async (transaction) => {
      items.forEach(([key, value]) => transaction.put(key, value));
    });

    return Result.ok(value);
  }

  private async batchCreateQuery<T extends { id?: string }>(
    info: RequestInfo,
  ): Promise<Result<BatchCreateQueryResponse<T>, StorageError>> {
    const query = info.body<BatchCreateQueryRequest<T>>(isBatchCreateQueryRequest);

    const entries: Record<string, T & { id: string }> = {};

    const keysFromInput = new Set(Object.keys(query.entries));
    const entriesFromInputKeys: Record<string, T & { id: string }> = {};

    for (const inputKey of keysFromInput) {
      const rows = this.indexHandler.enhanceWritePayload(
        inputKey,
        query.entries[inputKey],
      );
      for (const row of rows) {
        const id = row[0];
        const entry = { ...row[1], id: row[1].id ?? row[0] };

        if (keysFromInput.has(id)) {
          entries[id] = entry;
          entriesFromInputKeys[id] = entry;
        } else {
          entries[id] = entry;
        }
      }
    }

    await this.batcher.doChunkedWrite<T>(entries);

    return Result.ok(Object.values(entriesFromInputKeys));
  }

  private async readQuery<T>(
    info: RequestInfo,
  ): Promise<Result<ReadQueryResponse<T>, StorageError>> {
    const query = info.body(isReadQueryRequest);
    const key = this.keyFromQuery(query.key, query.index);

    const cached = this.cache.get<ReadQueryResponse<T>>(key);
    if (cached) {
      return Result.ok(cached);
    }

    const item = await this.state.storage.get<ReadQueryResponse<T>>(key, {
      allowConcurrency: true,
    });
    if (!item) {
      return Result.err(new StorageNotFoundError());
    }

    this.cache.set(key, item);
    return Result.ok(item);
  }

  private async batchReadQuery<T>(
    info: RequestInfo,
  ): Promise<Result<BatchReadQueryResponse<T>, StorageError>> {
    const query = info.body(isBatchReadQueryRequest);

    const inputKeys = query.keys;
    const keys = inputKeys.map((k) => this.keyFromQuery(k, query.index));

    const items = await this.batcher.doChunkedRead<ReadQueryResponse<T> | undefined>(
      keys,
    );

    return Result.ok(this.mapToOrderedArray(keys, items));
  }

  private async updateQuery<T>(
    info: RequestInfo,
  ): Promise<Result<UpdateQueryResponse<T>, StorageError>> {
    const query = info.body<UpdateQueryRequest<T>>(isUpdateQueryRequest);
    const key = query.key;
    const nextValue = query.value;

    const currentValue = await this.state.storage.get<UpdateQueryResponse<T>>(key);
    if (!currentValue) {
      return Result.err(new StorageNotFoundError());
    }

    const updateValue = { ...currentValue, ...nextValue };

    const items = this.indexHandler.enhanceWritePayload(key, updateValue);
    const dangling = this.indexHandler.computeDanglingKeys(currentValue, updateValue);

    await this.state.storage.transaction(async (transaction) => {
      items.forEach(([key]) => transaction.put(key, updateValue));
      dangling.forEach((key) => transaction.delete(key));
    });

    return Result.ok(updateValue);
  }

  private async batchUpdateQuery<T>(
    info: RequestInfo,
  ): Promise<Result<BatchUpdateQueryResponse<T>, StorageError>> {
    const query = info.body<BatchUpdateQueryRequest<T>>(isBatchUpdateQueryRequest);

    return this.doBatchUpdate<T>(query.entries, true);
  }

  private async batchUpsertQuery<T>(
    info: RequestInfo,
  ): Promise<Result<BatchUpsertQueryResponse<T>, StorageError>> {
    const query = info.body<BatchUpsertQueryRequest<T>>(isBatchUpsertQueryRequest);

    return this.doBatchUpdate<T>(query.entries, false);
  }

  private async removeQuery(
    info: RequestInfo,
  ): Promise<Result<RemoveQueryResponse, StorageError>> {
    const { key } = info.body(isRemoveQueryRequest);

    const keys: string[] = [key];
    this.indexHandler.enhanceDeletePayload(key, keys);

    const deleted = await this.state.storage.delete(keys);
    if (deleted === 0) {
      return Result.err(new StorageDeleteFailedError());
    }

    // When deleting an entity, delete all relationships too.
    await this.relationshipHandler.handle({
      type: 'relationship',
      operation: 'remove',
      body: <T>() => ({ node: key } as T),
    });

    return Result.ok({ success: true });
  }

  private async batchRemoveQuery(
    info: RequestInfo,
  ): Promise<Result<BatchRemoveQueryResponse, StorageError>> {
    const { keys: inputKeys } = info.body(isBatchRemoveQueryRequest);

    const keys: string[] = [...inputKeys];
    for (const inputKey of inputKeys) {
      this.indexHandler.enhanceDeletePayload(inputKey, keys);
    }

    await this.batcher.doChunkedDelete(new Set(keys));

    // When deleting an entity, delete all relationships too.
    await this.relationshipHandler.handle({
      type: 'relationship',
      operation: 'batchRemove',
      body: <T>() => inputKeys.map((key) => ({ node: key })) as T,
    });

    return Result.ok({ success: true });
  }

  private async listQuery<T>(
    info: RequestInfo,
  ): Promise<Result<ListQueryResponse<T>, StorageError>> {
    const query = info.body(isListQueryRequest);
    const prefix = this.keyFromQuery(query.key, query.index);

    const cached = this.cache.get<ListQueryResponse<T>>(prefix);
    if (cached) {
      return Result.ok(cached);
    }

    const items = await this.state.storage.list<QueryResponse<T>>({
      prefix,
      allowConcurrency: true,
    });

    const response = new Map<string, QueryResponse<T>>();
    items.forEach((value) => response.set(value.id, value));

    const entries = Object.fromEntries(response);

    this.cache.set(prefix, entries);
    return Result.ok(entries);
  }

  private async purgeAllQuery(): Promise<Result<boolean, StorageError>> {
    await this.state.storage.deleteAll();
    return Result.ok(true);
  }

  private async doBatchUpdate<T>(
    input: Record<string, Partial<T>>,
    throwOnMissingKey: boolean,
  ): Promise<Result<BatchUpsertQueryResponse<T>, StorageError>> {
    const keys = Object.keys(input);

    const items = await this.batcher.doChunkedRead(keys);

    if (throwOnMissingKey && items.size !== keys.length) {
      return Result.err(new StorageNotFoundError());
    }

    const entries: Record<string, T & { id: string }> = {};
    const entriesFromInputKeys: Record<string, T & { id: string }> = {};
    const dangling = new Set<string>();

    for (const key of keys) {
      const currentValue = items.get(key) ?? { id: key };
      const updateValue = input[key];
      const nextValue = { ...currentValue, ...updateValue } as T & { id: string };

      const itemsToUpdate = this.indexHandler.enhanceWritePayload(key, updateValue);
      const itemsDangling = this.indexHandler.computeDanglingKeys(
        currentValue,
        nextValue,
      );

      itemsToUpdate.forEach(([key]) => (entries[key] = nextValue));
      itemsDangling.forEach((key) => dangling.add(key));

      entriesFromInputKeys[key] = nextValue;
    }

    await this.batcher.doChunkedWrite<T>(entries);
    await this.batcher.doChunkedDelete(dangling);

    return Result.ok(this.recordToOrderedArray(keys, entries));
  }

  private keyFromQuery(key: string | undefined, index: string | undefined) {
    if (key && !index) {
      return key;
    }
    if (index && !key) {
      return index;
    }
    return `${index}--${key}`;
  }

  private mapToOrderedArray<T>(keys: string[], map: Map<string, T>) {
    return keys.map((key) => map.get(key) as T);
  }

  private recordToOrderedArray<T>(keys: string[], map: Record<string, T>) {
    return keys.map((key) => map[key] as T);
  }
}
