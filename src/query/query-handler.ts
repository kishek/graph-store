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

export class QueryHandler {
  constructor(
    private state: DurableObjectState,
    private indexHandler: IndexHandler,
    private relationshipHandler: RelationshipHandler,
  ) {}

  async handle<T>(info: RequestInfo) {
    switch (info.operation) {
      case 'create': {
        return await this.createQuery<T>(info);
      }
      case 'batchCreate': {
        return await this.batchCreateQuery<T>(info);
      }
      case 'read': {
        return await this.readQuery<T>(info);
      }
      case 'batchRead': {
        return await this.batchReadQuery<T>(info);
      }
      case 'update': {
        return await this.updateQuery<T>(info);
      }
      case 'remove': {
        return await this.removeQuery(info);
      }
      case 'list': {
        return await this.listQuery<T>(info);
      }
      default: {
        return Result.err(new StorageUnknownOperationError());
      }
    }
  }

  private async createQuery<T>(
    info: RequestInfo,
  ): Promise<Result<CreateQueryResponse<T>, StorageError>> {
    const input = info.body<CreateQueryRequest<T>>(isCreateQueryRequest);
    const value = { ...input.value, id: input.key };
    const items = this.indexHandler.enhanceWritePayload(input.key, value);

    await this.state.storage.transaction(async (transaction) => {
      items.forEach(([key, value]) => transaction.put(key, value));
    });

    return Result.ok(value);
  }

  private async batchCreateQuery<T>(
    info: RequestInfo,
  ): Promise<Result<BatchCreateQueryResponse<T>, StorageError>> {
    const query = info.body<BatchCreateQueryRequest<T>>(isBatchCreateQueryRequest);

    const entries: Record<string, T & { id: string }> = {};
    for (const inputKey of Object.keys(query.entries)) {
      const rows = this.indexHandler.enhanceWritePayload(
        inputKey,
        query.entries[inputKey],
      );
      for (const row of rows) {
        entries[row[0]] = { ...row[1], id: row[0] };
      }
    }

    await this.state.storage.transaction(async (transaction) => {
      await transaction.put(entries);
    });

    return Result.ok(Object.values(entries));
  }

  private async readQuery<T>(
    info: RequestInfo,
  ): Promise<Result<ReadQueryResponse<T>, StorageError>> {
    const query = info.body(isReadQueryRequest);
    const key = this.keyFromQuery(query.key, query.index);

    const item = await this.state.storage.get<ReadQueryResponse<T>>(key);
    if (!item) {
      return Result.err(new StorageNotFoundError());
    }

    return Result.ok(item);
  }

  private async batchReadQuery<T>(
    info: RequestInfo,
  ): Promise<Result<BatchReadQueryResponse<T>, StorageError>> {
    const query = info.body(isBatchReadQueryRequest);
    const keys = query.keys.map((k) => this.keyFromQuery(k, query.index));

    const items = await this.state.storage.get<ReadQueryResponse<T>>(keys);
    if (items.size !== keys.length) {
      return Result.err(new StorageNotFoundError());
    }

    return Result.ok(Array.from(items.values()));
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
    const dangling = this.indexHandler.computeDanglingItems(currentValue, updateValue);

    await this.state.storage.transaction(async (transaction) => {
      items.forEach(([key]) => transaction.put(key, updateValue));
      dangling.forEach((key) => transaction.delete(key));
    });

    return Result.ok(updateValue);
  }

  private async removeQuery(
    info: RequestInfo,
  ): Promise<Result<RemoveQueryResponse, StorageError>> {
    const { key } = info.body(isRemoveQueryRequest);

    const keys: string[] = [key];
    this.indexHandler.enhanceDeletePayload(key, keys);

    const deleted = await this.state.storage.delete(keys);
    if (deleted !== keys.length) {
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

  private async listQuery<T>(
    info: RequestInfo,
  ): Promise<Result<ListQueryResponse<T>, StorageError>> {
    const query = info.body(isListQueryRequest);
    const prefix = this.keyFromQuery(query.key, query.index);
    const items = await this.state.storage.list<QueryResponse<T>>({ prefix });

    const response = new Map<string, QueryResponse<T>>();
    items.forEach((value) => response.set(value.id, value));

    return Result.ok(Object.fromEntries(response));
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
}
