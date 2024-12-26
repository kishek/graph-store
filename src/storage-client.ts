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

export interface StorageClientMetadata {
  namespace: string;
}

export class StorageClient {
  private constructor(
    private api: DurableObjectStub,
    private meta: StorageClientMetadata,
  ) {}

  public static from(api: DurableObjectStub, meta: StorageClientMetadata) {
    return new StorageClient(api, meta);
  }

  public createIndex(opts: CreateIndexRequest) {
    return response<CreateIndexResponse>(
      this.api.fetch(
        'https://durable-object.com/',
        request({
          type: 'index',
          operation: 'create',
          request: opts,
        }),
      ),
    );
  }

  public readIndex(opts: ReadIndexRequest) {
    return response<ReadIndexResponse>(
      this.api.fetch(
        'https://durable-object.com/',
        request({
          type: 'index',
          operation: 'read',
          request: opts,
        }),
      ),
    );
  }

  public updateIndex(opts: UpdateIndexRequest) {
    return response<UpdateIndexResponse>(
      this.api.fetch(
        'https://durable-object.com/',
        request({
          type: 'index',
          operation: 'update',
          request: opts,
        }),
      ),
    );
  }

  public removeIndex(opts: RemoveIndexRequest) {
    return response<RemoveIndexResponse>(
      this.api.fetch(
        'https://durable-object.com/',
        request({
          type: 'index',
          operation: 'remove',
          request: opts,
        }),
      ),
    );
  }

  public listIndexes() {
    return response<ListIndexResponse>(
      this.api.fetch(
        'https://durable-object.com/',
        request({
          type: 'index',
          operation: 'list',
          request: {},
        }),
      ),
    );
  }

  public createQuery<T>(opts: CreateQueryRequest<T>) {
    return response<CreateQueryResponse<T>>(
      this.api.fetch(
        'https://durable-object.com/',
        request({
          type: 'query',
          operation: 'create',
          request: opts,
        }),
      ),
    );
  }

  public batchCreateQuery<T>(opts: BatchCreateQueryRequest<T>) {
    return response<BatchCreateQueryResponse<T>>(
      this.api.fetch(
        'https://durable-object.com/',
        request({
          type: 'query',
          operation: 'batchCreate',
          request: opts,
        }),
      ),
    );
  }

  public readQuery<T>(opts: ReadQueryRequest) {
    return response<ReadQueryResponse<T>>(
      this.api.fetch(
        'https://durable-object.com/',
        request({
          type: 'query',
          operation: 'read',
          request: opts,
        }),
      ),
    );
  }

  public batchReadQuery<T>(opts: BatchReadQueryRequest) {
    return response<BatchReadQueryResponse<T>>(
      this.api.fetch(
        'https://durable-object.com/',
        request({
          type: 'query',
          operation: 'batchRead',
          request: opts,
        }),
      ),
    );
  }

  public updateQuery<T>(opts: UpdateQueryRequest<T>) {
    return response<UpdateQueryResponse<T>>(
      this.api.fetch(
        'https://durable-object.com/',
        request({
          type: 'query',
          operation: 'update',
          request: opts,
        }),
      ),
    );
  }

  public batchUpdateQuery<T>(opts: BatchUpdateQueryRequest<T>) {
    return response<BatchUpdateQueryResponse<T>>(
      this.api.fetch(
        'https://durable-object.com/',
        request({
          type: 'query',
          operation: 'batchUpdate',
          request: opts,
        }),
      ),
    );
  }

  public batchUpsertQuery<T>(opts: BatchUpsertQueryRequest<T>) {
    return response<BatchUpsertQueryResponse<T>>(
      this.api.fetch(
        'https://durable-object.com/',
        request({
          type: 'query',
          operation: 'batchUpsert',
          request: opts,
        }),
      ),
    );
  }

  public removeQuery(opts: RemoveQueryRequest) {
    return response<RemoveQueryResponse>(
      this.api.fetch(
        'https://durable-object.com/',
        request({
          type: 'query',
          operation: 'remove',
          request: opts,
        }),
      ),
    );
  }

  public listQuery<T>(opts: ListQueryRequest) {
    return response<ListQueryResponse<T>>(
      this.api.fetch(
        'https://durable-object.com/',
        request({
          type: 'query',
          operation: 'list',
          request: opts,
        }),
      ),
    );
  }

  public createRelationship(opts: CreateRelationshipRequest) {
    return response<CreateRelationshipResponse>(
      this.api.fetch(
        'https://durable-object.com/',
        request({
          type: 'relationship',
          operation: 'create',
          request: opts,
        }),
      ),
    );
  }

  public createRelationshipBatch(opts: CreateRelationshipBatchRequest) {
    return response<CreateRelationshipBatchResponse>(
      this.api.fetch(
        'https://durable-object.com/',
        request({
          type: 'relationship',
          operation: 'batchCreate',
          request: opts,
        }),
      ),
    );
  }

  public removeRelationship(opts: RemoveRelationshipRequest) {
    return response<RemoveRelationshipResponse>(
      this.api.fetch(
        'https://durable-object.com/',
        request({
          type: 'relationship',
          operation: 'remove',
          request: opts,
        }),
      ),
    );
  }

  public removeRelationshipBatch(opts: RemoveRelationshipBatchRequest) {
    return response<RemoveRelationshipBatchResponse>(
      this.api.fetch(
        'https://durable-object.com/',
        request({
          type: 'relationship',
          operation: 'batchRemove',
          request: opts,
        }),
      ),
    );
  }

  public hasRelationship(opts: ReadRelationshipRequest) {
    return response<ReadRelationshipResponse>(
      this.api.fetch(
        'https://durable-object.com/',
        request({
          type: 'relationship',
          operation: 'read',
          request: opts,
        }),
      ),
    );
  }

  public listRelationship(opts: ListRelationshipRequest) {
    return response<ListRelationshipResponse>(
      this.api.fetch(
        'https://durable-object.com/',
        request({
          type: 'relationship',
          operation: 'list',
          request: opts,
        }),
      ),
    );
  }
}
