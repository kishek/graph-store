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
} from './relationship/relationship-request';
import { StorageRequest } from './storage-request';

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

async function response<T>(promise: Promise<Response>): Promise<Result<T, HTTPError>> {
  const response = await promise;
  const json = (await response.json()) as T;

  if (!response.ok) {
    return Result.err(HTTPError.from(response.status, (json as any).reason));
  }
  return Result.ok(json);
}

async function instrument<T>(promise: Promise<T>, tag: string): Promise<T> {
  const start = Date.now();
  const result = await promise;
  const end = Date.now();

  console.log({
    tag,
    duration: end - start,
  })

  return result;
}

export interface StorageClientMetadata {
  namespace: string;
  instrumented?: boolean
}

export class StorageClient {
  private constructor(
    private api: DurableObjectStub,
    private meta: StorageClientMetadata,
  ) { }

  public static from(api: DurableObjectStub, meta: StorageClientMetadata) {
    return new StorageClient(api, meta);
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

  private async execute<T>(opts: StorageRequest<any>): Promise<Result<T, HTTPError>> {
    if (this.meta.instrumented) {
      const start = Date.now();
      const result = await this.doOperation<T>(opts);
      const end = Date.now();

      console.log({
        tag: opts.request.tag ?? `${opts.operation}:${opts.type}`,
        duration: end - start,
      })

      return result;
    }
    return await this.doOperation(opts);
  }

  private async doOperation<T>(opts: StorageRequest<any>): Promise<Result<T, HTTPError>> {
    const response = await this.api.fetch(
      'https://durable-object.com/',
      request(opts),
    );
    const json = (await response.json()) as T;

    if (!response.ok) {
      return Result.err(HTTPError.from(response.status, (json as any).reason));
    }
    return Result.ok(json);
  }
}
