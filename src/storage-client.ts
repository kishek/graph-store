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
  QueryResponse,
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
import DataLoader from 'dataloader';

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
  instrumented?: boolean
}

export class StorageClient {
  private readLoadersByIndex: Record<string, { index: string, loader: DataLoader<string, any> }> = {};

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

  public async readQuery<T>(opts: ReadQueryRequest) {
    const loader = this.getReadLoader(opts);

    try {
      const result = await loader.load(opts.key) as ReadQueryResponse<T>;
      return Result.ok(result);
    } catch (err) {
      if (err instanceof HTTPError) {
        return Result.err(err);
      }

      const error = err instanceof Error ? err : new Error(String(err));
      return Result.err(HTTPError.from(500, error.message));
    }
  }

  public async batchReadQuery<T>(opts: BatchReadQueryRequest) {
    const loader = this.getReadLoader(opts);

    try {
      const result = await loader.loadMany(opts.keys) as BatchReadQueryResponse<T>;
      return Result.ok(result);
    } catch (err) {
      if (err instanceof HTTPError) {
        return Result.err(err);
      }

      const error = err instanceof Error ? err : new Error(String(err));
      return Result.err(HTTPError.from(500, error.message));
    }
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

  private getReadLoader(opts: ReadQueryRequest | BatchReadQueryRequest) {
    const key = opts.index ?? 'default';

    const loader = this.readLoadersByIndex[key];
    if (!loader) {
      this.readLoadersByIndex[key] = {
        index: opts.index ?? 'no-index',
        loader: new DataLoader<string, QueryResponse>((keys) => this.doBatchedRead(keys, opts.index)),
      };
    }

    return this.readLoadersByIndex[key].loader;
  }

  private async doBatchedRead(ids: readonly string[], index?: string): Promise<BatchReadQueryResponse<any>> {
    const payload: BatchReadQueryRequest = {
      keys: ids as string[],
      index,
    };

    const result = await this.execute<BatchReadQueryResponse<any>>({
      type: 'query',
      operation: 'batchRead',
      request: payload,
    });

    if (result.isErr) {
      throw result.error;
    }

    return result.value;
  }
}
