import { StorageBadRequestError } from './storage-errors';

type ItemOperation =
  | 'create'
  | 'read'
  | 'update'
  | 'remove'
  | 'list'
  | 'batchRead'
  | 'batchUpdate'
  | 'batchUpsert'
  | 'batchCreate'
  | 'batchRemove'
  | 'purge';
type RelationshipOperation =
  | 'create'
  | 'read'
  | 'remove'
  | 'list'
  | 'batchCreate'
  | 'batchRemove'
  | 'batchList'
  | 'purge';
type StoreOperation = 'backup' | 'restore';
type DiagnosticsOperation = 'log' | 'echo';

type StorageRequestType = 'query' | 'relationship' | 'index' | 'store' | 'diagnostic';
type StorageOperation =
  | ItemOperation
  | RelationshipOperation
  | StoreOperation
  | DiagnosticsOperation;

export type ItemRequest<T = Record<any, any>> = {
  tag?: string;
  type: 'query' | 'index';
  operation: ItemOperation;
  request: T;
};
export type RelationshipRequest<T = Record<any, any>> = {
  tag?: string;
  type: 'relationship';
  operation: RelationshipOperation;
  request: T;
};
export type OperationalRequest<T> = {
  type: 'store';
  operation: StoreOperation;
  request: T;
};
export type DiagnosticRequest<T> = {
  type: 'diagnostic';
  operation: DiagnosticsOperation;
  request: T;
};

export type StorageRequest<T = Record<any, any>> =
  | ItemRequest<T>
  | RelationshipRequest<T>
  | OperationalRequest<T>
  | DiagnosticRequest<T>;

export interface RequestInfo {
  type: StorageRequestType;
  operation: StorageOperation;
  body: <T>(guard: (body: any) => body is T) => T;
}
export const parseRequest = async (request: Request): Promise<RequestInfo> => {
  const body = await request.json<StorageRequest>();

  return {
    type: body.type,
    operation: body.operation ?? 'unknown',
    body: <T>(guard: (body: any) => body is T) => {
      if (guard(body.request)) {
        return body.request;
      }
      throw new StorageBadRequestError(`incorrect parameters provided`);
    },
  };
};
