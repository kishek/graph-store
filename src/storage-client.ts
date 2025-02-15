import { Result } from '@badrap/result';

import {
  CreateIndexRequest,
  RemoveIndexRequest,
  ReadIndexRequest,
  UpdateIndexRequest,
  CreateIndexResponse,
  ReadIndexResponse,
  UpdateIndexResponse,
  RemoveIndexResponse,
  ListIndexResponse,
} from './index/index-request';
import {
  CreateQueryRequest,
  ReadQueryRequest,
  UpdateQueryRequest,
  RemoveQueryRequest,
  CreateQueryResponse,
  ReadQueryResponse,
  UpdateQueryResponse,
  RemoveQueryResponse,
  ListQueryResponse,
  ListQueryRequest,
  BatchReadQueryRequest,
  BatchReadQueryResponse,
  BatchCreateQueryRequest,
  BatchCreateQueryResponse,
  BatchUpdateQueryRequest,
  BatchUpdateQueryResponse,
  BatchUpsertQueryRequest,
  BatchUpsertQueryResponse,
  BatchRemoveQueryRequest,
  BatchRemoveQueryResponse,
} from './query/query-request';
import {
  CreateRelationshipRequest,
  CreateRelationshipResponse,
  ReadRelationshipRequest,
  ReadRelationshipResponse,
  ListRelationshipRequest,
  ListRelationshipResponse,
  RemoveRelationshipRequest,
  RemoveRelationshipResponse,
  CreateRelationshipBatchRequest,
  CreateRelationshipBatchResponse,
  RemoveRelationshipBatchRequest,
  RemoveRelationshipBatchResponse,
  BatchListRelationshipRequest,
  BatchListRelationshipResponse,
} from './relationship/relationship-request';
import { StorageRequest } from './storage-request';
import { retry } from './retry';
import { RestoreRequest } from './store/store-request';
import { Storage } from './storage';
import { StorageError } from './storage-errors';

export class HTTPError extends Error {
  private status: number;

  constructor(status: number, message: string) {
    super();
    this.message = message;
    this.name = 'HTTPError';
    this.status = status;
  }

  static from(status: number, reason: string) {
    return new HTTPError(status, `${status}: ${reason ?? 'no reason provided'}`);
  }

  get statusCode() {
    return this.status;
  }
}

type HTTPRequest = Parameters<DurableObjectStub['fetch']>[1];

function request(opts: StorageRequest<any>): HTTPRequest {
  return {
    method: 'POST',
    body: JSON.stringify(opts),
  };
}

export interface StorageClientMetadata {
  namespace: string;
  instrumented?: boolean;
  hint?: DurableObjectLocationHint;
  version?: number;
}

export type StorageMiddleware = (opts: StorageRequest<any>) => StorageRequest<any>;

export class StorageClient {
  private constructor(
    private api: DurableObjectStub<Storage>,
    private meta: StorageClientMetadata,
    private middlewares: StorageMiddleware[] = [],
  ) {}

  public static from(
    api: DurableObjectStub<Storage>,
    meta: StorageClientMetadata,
    middlewares: StorageMiddleware[] = [],
  ) {
    return new StorageClient(api, meta, middlewares);
  }

  public use(middleware: StorageMiddleware) {
    this.middlewares.push(middleware);
  }

  public createIndex(opts: CreateIndexRequest) {
    return this.execute<CreateIndexResponse>({
      type: 'index',
      operation: 'create',
      request: opts,
    });
  }

  public readIndex(opts: ReadIndexRequest) {
    return this.execute<ReadIndexResponse>({
      type: 'index',
      operation: 'read',
      request: opts,
    });
  }

  public updateIndex(opts: UpdateIndexRequest) {
    return this.execute<UpdateIndexResponse>({
      type: 'index',
      operation: 'update',
      request: opts,
    });
  }

  public removeIndex(opts: RemoveIndexRequest) {
    return this.execute<RemoveIndexResponse>({
      type: 'index',
      operation: 'remove',
      request: opts,
    });
  }

  public listIndexes() {
    return this.execute<ListIndexResponse>({
      type: 'index',
      operation: 'list',
      request: {},
    });
  }

  public createQuery<T>(opts: CreateQueryRequest<T>) {
    return this.execute<CreateQueryResponse<T>>({
      type: 'query',
      operation: 'create',
      request: opts,
    });
  }

  public batchCreateQuery<T>(opts: BatchCreateQueryRequest<T>) {
    return this.execute<BatchCreateQueryResponse<T>>({
      type: 'query',
      operation: 'batchCreate',
      request: opts,
    });
  }

  public readQuery<T>(opts: ReadQueryRequest) {
    return this.execute<ReadQueryResponse<T>>({
      type: 'query',
      operation: 'read',
      request: opts,
    });
  }

  public batchReadQuery<T>(opts: BatchReadQueryRequest) {
    return this.execute<BatchReadQueryResponse<T>>({
      type: 'query',
      operation: 'batchRead',
      request: opts,
    });
  }

  public updateQuery<T>(opts: UpdateQueryRequest<T>) {
    return this.execute<UpdateQueryResponse<T>>({
      type: 'query',
      operation: 'update',
      request: opts,
    });
  }

  public batchUpdateQuery<T>(opts: BatchUpdateQueryRequest<T>) {
    return this.execute<BatchUpdateQueryResponse<T>>({
      type: 'query',
      operation: 'batchUpdate',
      request: opts,
    });
  }

  public batchUpsertQuery<T>(opts: BatchUpsertQueryRequest<T>) {
    return this.execute<BatchUpsertQueryResponse<T>>({
      type: 'query',
      operation: 'batchUpsert',
      request: opts,
    });
  }

  public removeQuery(opts: RemoveQueryRequest) {
    return this.execute<RemoveQueryResponse>({
      type: 'query',
      operation: 'remove',
      request: opts,
    });
  }

  public batchRemoveQuery(opts: BatchRemoveQueryRequest) {
    return this.execute<BatchRemoveQueryResponse>({
      type: 'query',
      operation: 'batchRemove',
      request: opts,
    });
  }

  public listQuery<T>(opts: ListQueryRequest) {
    return this.execute<ListQueryResponse<T>>({
      type: 'query',
      operation: 'list',
      request: opts,
    });
  }

  public createRelationship(opts: CreateRelationshipRequest) {
    return this.execute<CreateRelationshipResponse>({
      type: 'relationship',
      operation: 'create',
      request: opts,
    });
  }

  public createRelationshipBatch(opts: CreateRelationshipBatchRequest) {
    return this.execute<CreateRelationshipBatchResponse>({
      type: 'relationship',
      operation: 'batchCreate',
      request: opts,
    });
  }

  public removeRelationship(opts: RemoveRelationshipRequest) {
    return this.execute<RemoveRelationshipResponse>({
      type: 'relationship',
      operation: 'remove',
      request: opts,
    });
  }

  public removeRelationshipBatch(opts: RemoveRelationshipBatchRequest) {
    return this.execute<RemoveRelationshipBatchResponse>({
      type: 'relationship',
      operation: 'batchRemove',
      request: opts,
    });
  }

  public hasRelationship(opts: ReadRelationshipRequest) {
    return this.execute<ReadRelationshipResponse>({
      type: 'relationship',
      operation: 'read',
      request: opts,
    });
  }

  public listRelationship(opts: ListRelationshipRequest) {
    return this.execute<ListRelationshipResponse>({
      type: 'relationship',
      operation: 'list',
      request: opts,
    });
  }

  public batchListRelationship(opts: BatchListRelationshipRequest) {
    return this.execute<BatchListRelationshipResponse>({
      type: 'relationship',
      operation: 'batchList',
      request: opts,
    });
  }

  /**
   * Deletes all relationships entirely from the store.
   * @returns The number of items purged
   */
  public purgeAllRelationships() {
    return this.execute<number>({
      type: 'relationship',
      operation: 'purge',
      request: {},
    });
  }

  /**
   * Deletes all purged items entirely from the store.
   * @returns The number of items purged
   */
  public purgeAllQuery() {
    return this.execute<boolean>({
      type: 'query',
      operation: 'purge',
      request: {},
    });
  }

  /**
   * Backs up the entire store to a durable object backup.
   * @returns The backup ID
   */
  public backup() {
    return this.execute<string>({
      type: 'store',
      operation: 'backup',
      request: {},
    });
  }

  public restore(opts: RestoreRequest) {
    return this.execute<RestoreRequest>({
      type: 'store',
      operation: 'restore',
      request: opts,
    });
  }

  public diagnostics() {
    return this.execute<boolean>({
      type: 'diagnostic',
      operation: 'log',
      request: {},
    });
  }

  public init() {
    return this.execute<boolean>({
      type: 'diagnostic',
      operation: 'echo',
      request: {},
    });
  }

  private async execute<T>(opts: StorageRequest<any>): Promise<Result<T, StorageError>> {
    if (this.meta.version === 1) {
      return this.executeV1<T>(opts);
    } else {
      return this.executeV2<T>(opts);
    }
  }

  private async executeV1<T>(
    opts: StorageRequest<any>,
  ): Promise<Result<T, StorageError>> {
    const request = this.getStorageRequest(opts);

    if (this.meta.instrumented) {
      const start = Date.now();
      const result = await this.doOperation<T>(this.getStorageRequest(request));
      const end = Date.now();

      const tag =
        'tag' in opts.request
          ? opts.request.tag
          : opts.tag ?? `${opts.operation}:${opts.type}`;

      console.log({
        tag,
        duration: end - start,
      });

      return result;
    }

    return await this.doOperation(request);
  }

  private async executeV2<T>(
    opts: StorageRequest<any>,
  ): Promise<Result<T, StorageError>> {
    const request = this.getStorageRequest(opts);

    if (this.meta.instrumented) {
      const start = Date.now();
      const result = await this.doOperationV2<T>(this.getStorageRequest(request));
      const end = Date.now();

      const tag =
        'tag' in opts.request
          ? opts.request.tag
          : opts.tag ?? `${opts.operation}:${opts.type}`;

      console.log({
        tag,
        duration: end - start,
      });

      return result;
    }

    return await this.doOperationV2(request);
  }

  private async doOperation<T>(opts: StorageRequest<any>): Promise<Result<T, HTTPError>> {
    const response = await retry('storage', () => this.doNetworkOperation<T>(opts), {
      maxRetries: 3,
      delay: 100,
      factor: 2,
      shouldRetryOnSuccess: (response) => {
        return response.status ? response.status >= 500 : false;
      },
    });

    if (!response.ok) {
      try {
        const json = await response.json();
        return Result.err(HTTPError.from(response.status, (json as any).reason));
      } catch (err) {
        const error = err instanceof Error ? err : new Error(String(err));
        return Result.err(HTTPError.from(response.status, error.message));
      }
    }

    const json = (await response.json()) as T;
    return Result.ok(json);
  }

  private async doOperationV2<T>(
    opts: StorageRequest<any>,
  ): Promise<Result<T, StorageError>> {
    const result: Result<T, StorageError> = await retry(
      'storage',
      () => this.doRpcOperation<T>(opts),
      {
        maxRetries: 3,
        delay: 100,
        factor: 2,
        shouldRetryOnSuccess: (result) => {
          if (result.isErr) {
            const error: any = result.error;
            return error && error.retryable;
          } else {
            return false;
          }
        },
      },
    );

    if (result.isErr) {
      return Result.err(result.error);
    }

    return Result.ok(result.value);
  }

  private async doNetworkOperation<T>(opts: StorageRequest<any>) {
    const response = await this.api.fetch('https://durable-object.com/', request(opts));
    return response;
  }

  private async doRpcOperation<T>(
    opts: StorageRequest<any>,
  ): Promise<Result<T, StorageError>> {
    const result = await this.api.handle(opts);
    if (result.type === 'error') {
      return Result.err(result.error);
    } else if (result.type === 'success') {
      return Result.ok((result as any).value);
    }

    return Result.err(new StorageError('unknown'));
  }

  private getStorageRequest<T>(opts: StorageRequest<T>): StorageRequest<T> {
    return this.middlewares.reduce((acc, middleware) => middleware(acc), opts);
  }
}
