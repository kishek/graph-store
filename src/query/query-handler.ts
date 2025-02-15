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
  ReadQueryRequest,
  BatchReadQueryRequest,
  RemoveQueryRequest,
  BatchRemoveQueryRequest,
  ListQueryRequest,
  ListQueryRange,
} from './query-request';
import { IndexHandler } from '../index/index-handler';
import { RelationshipHandler } from '../relationship/relationship-handler';
import {
  StorageBadRequestError,
  StorageDeleteFailedError,
  StorageError,
  StorageNotFoundError,
  StorageUnknownOperationError,
} from '../storage-errors';
import { BatchedStorage } from '../batched-storage';
import { InMemoryReadCache } from '../cache/read-cache';
import { enforce, QueryStorageRequest } from '../storage-request';

export class QueryHandler {
  constructor(
    private state: DurableObjectState,
    private indexHandler: IndexHandler,
    private relationshipHandler: RelationshipHandler,
    private cache: InMemoryReadCache,
    private batcher = new BatchedStorage(state, cache),
  ) {}

  async handle<T extends { id?: string }>(opts: QueryStorageRequest<T>) {
    switch (opts.operation) {
      case 'create': {
        this.cache.deleteAll();
        return await this.createQuery<T>(opts.request);
      }
      case 'batchCreate': {
        this.cache.deleteAll();
        return await this.batchCreateQuery<T>(opts.request);
      }
      case 'read': {
        return await this.readQuery<T>(opts.request);
      }
      case 'batchRead': {
        return await this.batchReadQuery<T>(opts.request);
      }
      case 'update': {
        this.cache.deleteAll();
        return await this.updateQuery<T>(opts.request);
      }
      case 'batchUpdate': {
        this.cache.deleteAll();
        return await this.batchUpdateQuery<T>(opts.request);
      }
      case 'batchUpsert': {
        this.cache.deleteAll();
        return await this.batchUpsertQuery<T>(opts.request);
      }
      case 'remove': {
        this.cache.deleteAll();
        return await this.removeQuery(opts.request);
      }
      case 'batchRemove': {
        this.cache.deleteAll();
        return await this.batchRemoveQuery(opts.request);
      }
      case 'list': {
        return await this.listQuery<T>(opts.request);
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
    input: CreateQueryRequest<T>,
  ): Promise<Result<CreateQueryResponse<T>, StorageError>> {
    enforce(input, isCreateQueryRequest);

    const value = { ...input.value, id: input.value.id ?? input.key };
    const items = this.indexHandler.enhanceWritePayload(input.key, value);

    await this.state.storage.transaction(async (transaction) => {
      items.forEach(([key, value]) => transaction.put(key, value));
    });

    return Result.ok(value);
  }

  private async batchCreateQuery<T extends { id?: string }>(
    input: BatchCreateQueryRequest<T>,
  ): Promise<Result<BatchCreateQueryResponse<T>, StorageError>> {
    enforce(input, isBatchCreateQueryRequest);

    const entries: Record<string, T & { id: string }> = {};

    const keysFromInput = new Set(Object.keys(input.entries));
    const entriesFromInputKeys: Record<string, T & { id: string }> = {};

    for (const inputKey of keysFromInput) {
      const rows = this.indexHandler.enhanceWritePayload(
        inputKey,
        input.entries[inputKey],
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
    input: ReadQueryRequest,
  ): Promise<Result<ReadQueryResponse<T>, StorageError>> {
    enforce(input, isReadQueryRequest);

    const key = this.keyFromQuery(input.key, input.index);
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
    input: BatchReadQueryRequest,
  ): Promise<Result<BatchReadQueryResponse<T>, StorageError>> {
    enforce(input, isBatchReadQueryRequest);

    const inputKeys = input.keys;
    const keys = inputKeys.map((k) => this.keyFromQuery(k, input.index));

    const items = await this.batcher.doChunkedRead<ReadQueryResponse<T> | undefined>(
      keys,
    );

    return Result.ok(this.mapToOrderedArray(keys, items));
  }

  private async updateQuery<T>(
    input: UpdateQueryRequest<T>,
  ): Promise<Result<UpdateQueryResponse<T>, StorageError>> {
    enforce(input, isUpdateQueryRequest);

    const key = input.key;
    const nextValue = input.value;

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
    input: BatchUpdateQueryRequest<T>,
  ): Promise<Result<BatchUpdateQueryResponse<T>, StorageError>> {
    enforce(input, isBatchUpdateQueryRequest);

    return this.doBatchUpdate<T>(input.entries, true);
  }

  private async batchUpsertQuery<T>(
    input: BatchUpsertQueryRequest<T>,
  ): Promise<Result<BatchUpsertQueryResponse<T>, StorageError>> {
    enforce(input, isBatchUpsertQueryRequest);

    return this.doBatchUpdate<T>(input.entries, false);
  }

  private async removeQuery(
    input: RemoveQueryRequest,
  ): Promise<Result<RemoveQueryResponse, StorageError>> {
    enforce(input, isRemoveQueryRequest);

    const keys: string[] = [input.key];
    this.indexHandler.enhanceDeletePayload(input.key, keys);

    const deleted = await this.state.storage.delete(keys);
    if (deleted === 0) {
      return Result.err(new StorageDeleteFailedError());
    }

    // When deleting an entity, delete all relationships too.
    await this.relationshipHandler.handle({
      type: 'relationship',
      operation: 'removeNode',
      request: { node: input.key },
    });

    return Result.ok({ success: true });
  }

  private async batchRemoveQuery(
    input: BatchRemoveQueryRequest,
  ): Promise<Result<BatchRemoveQueryResponse, StorageError>> {
    enforce(input, isBatchRemoveQueryRequest);

    const keys: string[] = [...input.keys];
    for (const inputKey of input.keys) {
      this.indexHandler.enhanceDeletePayload(inputKey, keys);
    }

    await this.batcher.doChunkedDelete(new Set(keys));

    // When deleting an entity, delete all relationships too.
    await this.relationshipHandler.handle({
      type: 'relationship',
      operation: 'batchRemoveNode',
      request: input.keys.map((key) => ({ node: key })),
    });

    return Result.ok({ success: true });
  }

  private async listQuery<T>(
    input: ListQueryRequest<T>,
  ): Promise<Result<ListQueryResponse<T>, StorageError>> {
    enforce(input, isListQueryRequest);

    const prefix = this.keyFromQuery(input.key, input.index);
    const { first, last, before, after, query } = input;

    const items =
      query && query.length > 0
        ? await this.getQueryItems<T>(prefix, query)
        : await this.getPaginatedItems<T>(first, before, last, after, prefix);

    // no filtering - cache entire result
    if (!query && !first && !last && !before && !after) {
      this.cache.set(prefix, items);
    }

    const response = new Map<string, QueryResponse<T>>();
    items.forEach((value) => response.set(value.id, value));

    const entries = Object.fromEntries(response);
    return Result.ok(entries);
  }

  private async getQueryItems<T>(prefix: string, query: ListQueryRange<T>[]) {
    const items =
      this.cache.get<Map<string, QueryResponse<T>>>(prefix) ??
      (await this.state.storage.list<QueryResponse<T>>({
        prefix,
        allowConcurrency: true,
      }));
    this.cache.set(prefix, items);

    const result = new Map<string, QueryResponse<T>>();

    for (const [key, value] of items.entries()) {
      const matches = query.every((q) => {
        const property = value[q.property];
        return property >= q.min && property <= q.max;
      });

      if (!matches) {
        continue;
      }

      result.set(key, value);
    }

    return result;
  }

  private async getPaginatedItems<T>(
    first: number | undefined,
    before: string | undefined,
    last: number | undefined,
    after: string | undefined,
    prefix: string,
  ) {
    if (first && before) {
      throw new StorageBadRequestError('cannot supply `first` and `before` together');
    }
    if (last && after) {
      throw new StorageBadRequestError('cannot supply `last` and `after` together');
    }
    if (first && last) {
      throw new StorageBadRequestError('cannot supply `first` and `last` together');
    }

    if (before && after) {
      return await this.state.storage.list<QueryResponse<T>>({
        prefix,
        startAfter: after,
        end: before,
        allowConcurrency: true,
      });
    }

    if (before) {
      return await this.state.storage.list<QueryResponse<T>>({
        prefix,
        end: before,
        allowConcurrency: true,
        limit: last ?? 100,
        reverse: true,
      });
    }

    if (after) {
      return await this.state.storage.list<QueryResponse<T>>({
        prefix,
        startAfter: after,
        allowConcurrency: true,
        limit: first ?? 100,
      });
    }

    if (first) {
      return await this.state.storage.list<QueryResponse<T>>({
        prefix,
        limit: first,
        allowConcurrency: true,
      });
    }

    if (last) {
      return await this.state.storage.list<QueryResponse<T>>({
        prefix,
        limit: last,
        allowConcurrency: true,
        reverse: true,
      });
    }

    const items = await this.state.storage.list<QueryResponse<T>>({
      prefix,
      allowConcurrency: true,
    });
    return items;
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
