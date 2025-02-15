import { Result } from '@badrap/result';
import { DurableObject } from 'cloudflare:workers';

import { IndexHandler } from './index/index-handler';
import { QueryHandler } from './query/query-handler';
import { RelationshipHandler } from './relationship/relationship-handler';
import {
  RelationshipName,
  RelationshipRequest,
} from './relationship/relationship-request';
import {
  StorageClient,
  StorageClientMetadata,
  StorageMiddleware,
} from './storage-client';
import { StorageError, StorageUnexpectedError } from './storage-errors';
import { parseRequest, StorageRequest } from './storage-request';
import { StorageEnvironment } from './storage-environment';
import { StoreHandler } from './store/store-handler';
import { DiagnosticsHandler } from './diagnostics/diagnostic-handler';
import { InMemoryReadCache } from './cache/read-cache';

export type StorageResult<T> =
  | { type: 'error'; error: StorageError }
  | { type: 'success'; value: T };

export class Storage extends DurableObject {
  private queryHandler: QueryHandler;
  private indexHandler: IndexHandler;
  private relationshipHandler: RelationshipHandler;
  private storeHandler: StoreHandler;
  private diagnosticHandler: DiagnosticsHandler;

  private cache: InMemoryReadCache = new InMemoryReadCache();

  constructor(ctx: DurableObjectState, env: StorageEnvironment) {
    super(ctx, env);
    this.relationshipHandler = new RelationshipHandler(ctx, this.cache);
    this.indexHandler = new IndexHandler(ctx);
    this.queryHandler = new QueryHandler(
      ctx,
      this.indexHandler,
      this.relationshipHandler,
      this.cache,
    );
    this.storeHandler = new StoreHandler(ctx, env, this.cache);
    this.diagnosticHandler = new DiagnosticsHandler(ctx);
  }

  async fetch(request: Request) {
    try {
      const info = await parseRequest(request);
      const result = await this.handle(info);

      if (result.type === 'success') {
        return this.toResponse(result.value);
      } else {
        return this.toResponseError(result.error);
      }
    } catch (err) {
      return this.toResponseError(err);
    }
  }

  async handle<T extends Record<any, any> = Record<any, any>>(
    request: StorageRequest,
  ): Promise<StorageResult<T>> {
    try {
      let result: Result<unknown | StorageError>;

      switch (request.type) {
        case 'query': {
          result = await this.queryHandler.handle(request);
          break;
        }
        case 'index': {
          result = await this.indexHandler.handle(request);
          break;
        }
        case 'relationship': {
          result = await this.relationshipHandler.handle(request);
          break;
        }
        case 'store': {
          result = await this.storeHandler.handle(request);
          break;
        }
        case 'diagnostic': {
          result = await this.diagnosticHandler.handle(request);
          break;
        }
      }

      if (result.isOk) {
        return { type: 'success', value: result.value as T };
      } else {
        return { type: 'error', error: result.error };
      }
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err));
      return { type: 'error', error: new StorageUnexpectedError(error.message) };
    }
  }

  toResponse(value: unknown) {
    return new Response(JSON.stringify(value), { status: 200 });
  }

  toResponseError(error: Error | unknown) {
    if (error instanceof Error) {
      const reason = JSON.stringify({ reason: error.message });

      switch (error.name) {
        case 'StorageBadRequestError': {
          return new Response(reason, { status: 400 });
        }
        case 'StorageUnknownOperationError': {
          return new Response(reason, { status: 400 });
        }
        case 'StorageDeleteFailedError': {
          return new Response(reason, { status: 400 });
        }
        case 'StorageNotFoundError': {
          return new Response(reason, { status: 404 });
        }
        case 'StorageUnexpectedError': {
          return new Response(reason, { status: 500 });
        }
      }
    }

    return new Response(JSON.stringify(error), { status: 500 });
  }
}

export const getDurableObjectSingleton = (context: {
  env: StorageEnvironment;
  meta: StorageClientMetadata;
}) => {
  const durableObjectNamespace = `storage-${context.meta.namespace}`;
  const durableObjectId = context.env.GRAPH_STORAGE.idFromName(durableObjectNamespace);
  const durableObject = context.env.GRAPH_STORAGE.get(durableObjectId, {
    locationHint: context.meta.hint,
  });

  return durableObject;
};

export const getStorageClient = (context: {
  env: StorageEnvironment;
  meta: StorageClientMetadata;
  middleware?: StorageMiddleware[];
}) => {
  return StorageClient.from(
    getDurableObjectSingleton(context),
    context.meta,
    context.middleware,
  );
};

export type { RelationshipName, RelationshipRequest };
export { StorageClient, type StorageMiddleware } from './storage-client';
export type { StorageEnvironment } from './storage-environment';
