export interface Index {
  id: string;
  property: string;
}

export type CreateIndexRequest = Omit<Index, 'id'>;
export type CreateIndexResponse = Index;

export type ReadIndexRequest = Pick<Index, 'id'>;
export type ReadIndexResponse = Index;

export type UpdateIndexRequest = Index;
export type UpdateIndexResponse = Index;

export type RemoveIndexRequest = Pick<Index, 'id'>;
export type RemoveIndexResponse = { success: boolean };

export type ListIndexRequest = void;
export type ListIndexResponse = Record<string, Index>;

export const isCreateIndexRequest = (body: any): body is CreateIndexRequest => {
  return body && typeof body.property === 'string';
};
export const isUpdateIndexRequest = (body: any): body is UpdateIndexRequest => {
  return body && typeof body.property === 'string';
};
export const isReadIndexRequest = (body: any): body is ReadIndexRequest => {
  return body && typeof body.id === 'string';
};
export const isRemoveIndexRequest = (body: any): body is RemoveIndexRequest => {
  return body && typeof body.id === 'string';
};
