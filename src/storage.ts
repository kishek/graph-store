import { Result } from '@badrap/result';

import { IndexHandler } from './index/index-handler';
import { QueryHandler } from './query/query-handler';
import { RelationshipHandler } from './relationship/relationship-handler';
import {
  RelationshipName,
  RelationshipRequest,
} from './relationship/relationship-request';
import { StorageClient, StorageClientMetadata } from './storage-client';
import { StorageError } from './storage-errors';
import { parseRequest } from './storage-request';

export interface StorageEnvironment {
  GRAPH_STORAGE: DurableObjectNamespace;
}

export class Storage {
  private queryHandler: QueryHandler;
  private indexHandler: IndexHandler;
  private relationshipHandler: RelationshipHandler;

  constructor(state: DurableObjectState) {
    this.relationshipHandler = new RelationshipHandler(state);
    this.indexHandler = new IndexHandler(state);
    this.queryHandler = new QueryHandler(
      state,
      this.indexHandler,
      this.relationshipHandler,
    );
  }

  async fetch(request: Request) {
    try {
      const info = await parseRequest(request);
      let result: Result<unknown | StorageError>;

      switch (info.type) {
        case 'query': {
          result = await this.queryHandler.handle(info);
          break;
        }
        case 'index': {
          result = await this.indexHandler.handle(info);
          break;
        }
        case 'relationship': {
          result = await this.relationshipHandler.handle(info);
          break;
        }
      }

      if (result.isOk) {
        return this.mapResponse(result.value);
      } else {
        return this.mapError(result.error);
      }
    } catch (err) {
      return this.mapError(err);
    }
  }

  mapResponse(value: unknown) {
    return new Response(JSON.stringify(value), { status: 200 });
  }

  mapError(error: Error | unknown) {
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
  const durableObject = context.env.GRAPH_STORAGE.get(durableObjectId);

  return durableObject;
};

export const getStorageClient = (context: {
  env: StorageEnvironment;
  meta: StorageClientMetadata;
}) => {
  return StorageClient.from(getDurableObjectSingleton(context), context.meta);
};

export type { RelationshipName, RelationshipRequest };
export { StorageClient } from './storage-client';
