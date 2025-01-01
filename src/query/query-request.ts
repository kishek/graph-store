export interface QueryRequest<T = Record<string, string | number>> {
  tag?: string;
  key: string;
  value: T;
  index?: string;
}
export type QueryResponse<T = Record<string, string | number>> = { id: string } & T;

export type CreateQueryRequest<T> = QueryRequest<T>;
export type CreateQueryResponse<T> = QueryResponse<T>;

export type BatchCreateQueryRequest<T> = {
  tag?: string;
  entries: Record<string, T>;
  index?: string;
};
export type BatchCreateQueryResponse<T> = QueryResponse<T>[];

export type ReadQueryRequest = Pick<QueryRequest, 'key' | 'index' | 'tag'>;
export type ReadQueryResponse<T> = QueryResponse<T>;

export type BatchReadQueryRequest = {
  tag?: string;
  keys: string[];
  index?: string;
};
export type BatchReadQueryResponse<T> = QueryResponse<T>[];

export type UpdateQueryRequest<T> = {
  tag?: string;
  key: string;
  value: Partial<T>;
};
export type UpdateQueryResponse<T> = QueryResponse<T>;

export type BatchUpdateQueryRequest<T> = {
  tag?: string;
  entries: Record<string, Partial<T>>;
  index?: string;
};
export type BatchUpdateQueryResponse<T> = QueryResponse<T>[];

export type BatchUpsertQueryRequest<T> = {
  tag?: string;
  entries: Record<string, T>;
  index?: string;
};
export type BatchUpsertQueryResponse<T> = QueryResponse<T>[];

export type RemoveQueryRequest = Omit<Pick<QueryRequest, 'key' | 'index' | 'tag'>, 'index'>;
export type RemoveQueryResponse = { success: boolean };

export type BatchRemoveQueryRequest = {
  tag?: string;
  keys: string[];
};
export type BatchRemoveQueryResponse = { success: boolean };

export type ListQueryRequest = Partial<Pick<QueryRequest, 'key' | 'index' | 'tag'>>;
export type ListQueryResponse<T> = Record<string, QueryResponse<T>>;

export const isCreateQueryRequest = <T>(body: any): body is CreateQueryRequest<T> => {
  return body && typeof body.key === 'string' && typeof body.value === 'object';
};
export const isBatchCreateQueryRequest = <T>(
  body: any,
): body is BatchCreateQueryRequest<T> => {
  return body?.entries && Object.keys(body.entries).length > 0;
};
export const isUpdateQueryRequest = <T>(body: any): body is UpdateQueryRequest<T> => {
  return body && typeof body.key === 'string' && typeof body.value === 'object';
};
export const isBatchUpdateQueryRequest = <T>(
  body: any,
): body is BatchUpdateQueryRequest<T> => {
  return body?.entries && Object.keys(body.entries).length > 0;
};
export const isBatchUpsertQueryRequest = <T>(
  body: any,
): body is BatchUpsertQueryRequest<T> => {
  return body?.entries && Object.keys(body.entries).length > 0;
};
export const isReadQueryRequest = (body: any): body is ReadQueryRequest => {
  return body && typeof body.key === 'string';
};
export const isBatchReadQueryRequest = (body: any): body is BatchReadQueryRequest => {
  return body && Array.isArray(body.keys);
};
export const isRemoveQueryRequest = (body: any): body is RemoveQueryRequest => {
  return body && typeof body.key === 'string';
};
export const isBatchRemoveQueryRequest = (body: any): body is BatchRemoveQueryRequest => {
  return body && Array.isArray(body.keys);
};
export const isListQueryRequest = (body: any): body is ListQueryRequest => {
  return !!body;
};
