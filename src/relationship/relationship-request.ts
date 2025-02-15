export type RelationshipName = string & { _relationshipRole: symbol };

export type RelationshipRequest = {
  tag?: string;
  nodeA: string;
  nodeB: string;
  nodeAToBRelationshipName: RelationshipName;
  nodeBToARelationshipName: RelationshipName;
};
export type RelationshipTuple = [string, string];
export type RelationshipData = Set<string>;
export type RelationshipNameData = string;
export type RelationshipItem = {
  relationshipId: string;
  nodeA: string;
  nodeB: string;
};

export type CreateRelationshipRequest = RelationshipRequest;
export type CreateRelationshipResponse = { success: boolean };

export type CreateRelationshipBatchRequest = RelationshipRequest[];
export type CreateRelationshipBatchResponse = { success: boolean };

export type ReadRelationshipRequest = {
  tag?: string;
  nodeA: string;
  nodeB: string;
  name: RelationshipName;
};
export type ReadRelationshipResponse = { exists: boolean };

export type RemoveRelationshipRequest = {
  tag?: string;
  nodeA: string;
  nodeB: string;
  nodeAToBRelationshipName: RelationshipName;
  nodeBToARelationshipName: RelationshipName;
};
export type RemoveRelationshipResponse = { success: boolean };

export type RemoveRelationshipBatchRequest = RemoveRelationshipRequest[];
export type RemoveRelationshipBatchResponse = { success: boolean };

export type RemoveRelationshipNodeRequest = { node: string; tag?: string };
export type RemoveRelationshipNodeResponse = { success: boolean };

export type RemoveRelationshipBatchNodeRequest = { node: string; tag?: string }[];
export type RemoveRelationshipBatchNodeResponse = { success: boolean };

export type ListRelationshipRequest = {
  tag?: string;
  name: RelationshipName;
  node: string;
  first?: number;
  last?: number;
  before?: string;
  after?: string;
};

export type BatchListRelationshipRequest = {
  tag?: string;
  requests: Omit<ListRelationshipRequest, 'tag'>[];
};
export type ListRelationshipResponse = {
  relationships: string[];
  hasBefore: boolean;
  hasAfter: boolean;
};
export type BatchListRelationshipResponse = ListRelationshipResponse[];

export const isCreateRelationshipRequest = (
  body: any,
): body is CreateRelationshipRequest => {
  return (
    body &&
    typeof body.nodeA === 'string' &&
    typeof body.nodeB === 'string' &&
    typeof body.nodeAToBRelationshipName === 'string' &&
    typeof body.nodeBToARelationshipName === 'string'
  );
};

export const isCreateRelationshipBatchRequest = (
  body: any,
): body is CreateRelationshipBatchRequest => {
  return body && Array.isArray(body) && body.every(isCreateRelationshipRequest);
};

export const isReadRelationshipRequest = (body: any): body is ReadRelationshipRequest => {
  return (
    body &&
    typeof body.nodeA === 'string' &&
    typeof body.nodeB === 'string' &&
    typeof body.name === 'string'
  );
};

export const isRemoveRelationshipRequest = (
  body: any,
): body is RemoveRelationshipRequest => {
  return (
    body &&
    ((typeof body.nodeA === 'string' &&
      typeof body.nodeB === 'string' &&
      typeof body.nodeAToBRelationshipName === 'string' &&
      typeof body.nodeBToARelationshipName === 'string') ||
      typeof body.node === 'string')
  );
};

export const isRemoveRelationshipBatchRequest = (
  body: any,
): body is RemoveRelationshipBatchRequest => {
  return body && Array.isArray(body) && body.every(isRemoveRelationshipRequest);
};

export const isRemoveRelationshipNodeRequest = (
  body: any,
): body is RemoveRelationshipNodeRequest => {
  return body && typeof body.node === 'string';
};

export const isRemoveRelationshipNodeBatchRequest = (
  body: any,
): body is RemoveRelationshipBatchNodeRequest => {
  return body && Array.isArray(body) && body.every(isRemoveRelationshipNodeRequest);
};

export const isListRelationshipRequest = (body: any): body is ListRelationshipRequest => {
  return body && typeof body.name === 'string';
};

export const isBatchListRelationshipRequest = (
  body: any,
): body is BatchListRelationshipRequest => {
  return (
    body && Array.isArray(body.requests) && body.requests.every(isListRelationshipRequest)
  );
};
