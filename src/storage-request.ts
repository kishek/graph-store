import {
  CreateIndexRequest,
  ListIndexRequest,
  ReadIndexRequest,
  RemoveIndexRequest,
  UpdateIndexRequest,
} from './index/index-request';
import {
  BatchCreateQueryRequest,
  BatchReadQueryRequest,
  BatchRemoveQueryRequest,
  BatchUpdateQueryRequest,
  BatchUpsertQueryRequest,
  CreateQueryRequest,
  ListQueryRequest,
  ReadQueryRequest,
  RemoveQueryRequest,
  UpdateQueryRequest,
} from './query/query-request';
import {
  BatchListRelationshipRequest,
  CreateRelationshipBatchRequest,
  CreateRelationshipRequest,
  ListRelationshipRequest,
  ReadRelationshipRequest,
  RemoveRelationshipBatchNodeRequest,
  RemoveRelationshipBatchRequest,
  RemoveRelationshipNodeRequest,
  RemoveRelationshipRequest,
} from './relationship/relationship-request';
import { StorageBadRequestError } from './storage-errors';
import { RestoreRequest } from './store/store-request';

type QueryBase<T, S> = {
  type: 'query';
  tag?: string;
  operation: T;
  request: S;
};

export type QueryStorageRequest<T = Record<any, any>> =
  | QueryBase<'create', CreateQueryRequest<T>>
  | QueryBase<'batchCreate', BatchCreateQueryRequest<T>>
  | QueryBase<'read', ReadQueryRequest>
  | QueryBase<'batchRead', BatchReadQueryRequest>
  | QueryBase<'update', UpdateQueryRequest<T>>
  | QueryBase<'batchUpdate', BatchUpdateQueryRequest<T>>
  | QueryBase<'batchUpsert', BatchUpsertQueryRequest<T>>
  | QueryBase<'remove', RemoveQueryRequest>
  | QueryBase<'batchRemove', BatchRemoveQueryRequest>
  | QueryBase<'list', ListQueryRequest>
  | QueryBase<'purge', {}>;

type IndexBase<T, S> = {
  type: 'index';
  tag?: string;
  operation: T;
  request: S;
};

export type IndexStorageRequest =
  | IndexBase<'create', CreateIndexRequest>
  | IndexBase<'read', ReadIndexRequest>
  | IndexBase<'update', UpdateIndexRequest>
  | IndexBase<'remove', RemoveIndexRequest>
  | IndexBase<'list', {}>;

type RelationshipBase<T, S> = {
  type: 'relationship';
  tag?: string;
  operation: T;
  request: S;
};

export type RelationshipStorageRequest =
  | RelationshipBase<'create', CreateRelationshipRequest>
  | RelationshipBase<'batchCreate', CreateRelationshipBatchRequest>
  | RelationshipBase<'read', ReadRelationshipRequest>
  | RelationshipBase<'remove', RemoveRelationshipRequest>
  | RelationshipBase<'batchRemove', RemoveRelationshipBatchRequest>
  | RelationshipBase<'removeNode', RemoveRelationshipNodeRequest>
  | RelationshipBase<'batchRemoveNode', RemoveRelationshipBatchNodeRequest>
  | RelationshipBase<'list', ListRelationshipRequest>
  | RelationshipBase<'batchList', BatchListRelationshipRequest>
  | RelationshipBase<'purge', {}>;

type OperationalBase<T, S> = {
  type: 'store';
  tag?: string;
  operation: T;
  request: S;
};

export type OperationalStorageRequest =
  | OperationalBase<'backup', {}>
  | OperationalBase<'restore', RestoreRequest>;

type DiagnosticBase<T, S> = {
  type: 'diagnostic';
  tag?: string;
  operation: T;
  request: S;
};

export type DiagnosticStorageRequest =
  | DiagnosticBase<'log', {}>
  | DiagnosticBase<'echo', {}>;

export type StorageRequest<T = Record<any, any>> =
  | QueryStorageRequest<T>
  | IndexStorageRequest
  | RelationshipStorageRequest
  | OperationalStorageRequest
  | DiagnosticStorageRequest;

export const parseRequest = async (request: Request): Promise<StorageRequest> => {
  const body = await request.json<StorageRequest>();
  return body;
};

export function enforce<T>(value: T, guard: (v: any) => v is T): asserts value is T {
  if (!guard(value)) {
    throw new StorageBadRequestError('incorrect parameters provided');
  }
}
