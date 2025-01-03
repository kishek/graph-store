import { Result } from '@badrap/result';

import {
  CreateRelationshipBatchResponse,
  CreateRelationshipRequest,
  CreateRelationshipBatchRequest,
  CreateRelationshipResponse,
  isCreateRelationshipRequest,
  isListRelationshipRequest,
  isReadRelationshipRequest,
  isRemoveRelationshipRequest,
  ListRelationshipRequest,
  ListRelationshipResponse,
  ReadRelationshipRequest,
  ReadRelationshipResponse,
  RelationshipData,
  RelationshipName,
  RemoveRelationshipRequest,
  RemoveRelationshipResponse,
  isCreateRelationshipBatchRequest,
  RemoveRelationshipBatchResponse,
  isRemoveRelationshipBatchRequest,
  RelationshipRequest,
  RelationshipTuple,
} from './relationship-request';
import {
  StorageBadRequestError,
  StorageError,
  StorageNotFoundError,
  StorageUnknownOperationError,
} from '../storage-errors';
import { RequestInfo } from '../storage-request';

export const hierarchicalRole = {
  parent: 'parent' as RelationshipName,
  child: 'child' as RelationshipName,
};

const id = (relationshipName: RelationshipName, node: string) => {
  return `relationship$${node}${type(relationshipName)}`;
};

const prefix = (node: string) => {
  return `relationship$${node}`;
};

const type = (relationshipName: RelationshipName) => {
  return `$${relationshipName}`;
};

const mappingId = (relationshipName: RelationshipName) => {
  return `relationship-name$${relationshipName}`;
};

export class RelationshipHandler {
  constructor(private state: DurableObjectState) {}

  async handle(info: RequestInfo) {
    switch (info.operation) {
      case 'create': {
        return await this.createRelationship(info);
      }
      case 'batchCreate': {
        return await this.createRelationshipBatch(info);
      }
      case 'read': {
        return await this.hasRelationship(info);
      }
      case 'remove': {
        return await this.removeRelationship(info);
      }
      case 'batchRemove': {
        return await this.removeRelationshipBatch(info);
      }
      case 'list': {
        return await this.listRelationship(info);
      }
      default: {
        return Result.err(new StorageUnknownOperationError());
      }
    }
  }

  private async createRelationship(
    info: RequestInfo,
  ): Promise<Result<CreateRelationshipResponse, StorageError>> {
    const input = info.body<CreateRelationshipRequest>(isCreateRelationshipRequest);
    const nodeAId = id(input.nodeAToBRelationshipName, input.nodeA);
    const nodeBId = id(input.nodeBToARelationshipName, input.nodeB);

    await this.state.storage.transaction(async (transaction) => {
      await this.addRelationship(transaction, nodeAId, input.nodeB);
      await this.addRelationship(transaction, nodeBId, input.nodeA);
      await this.addRelationshipNameMapping(
        transaction,
        input.nodeAToBRelationshipName,
        input.nodeBToARelationshipName,
      );
    });

    return Result.ok({ success: true });
  }

  private async createRelationshipBatch(
    info: RequestInfo,
  ): Promise<Result<CreateRelationshipBatchResponse, StorageError>> {
    const input = info.body<CreateRelationshipBatchRequest>(
      isCreateRelationshipBatchRequest,
    );

    const { relationsRight, relationsLeft } = this.parseBatchRequest(input);

    await this.state.storage.transaction(async (transaction) => {
      await this.addRelationshipBatch(transaction, relationsRight);
      await this.addRelationshipBatch(transaction, relationsLeft);
    });

    return Result.ok({ success: true });
  }

  private async addRelationship(
    transaction: DurableObjectTransaction,
    id: string,
    relatedToId: string,
  ) {
    const relations = (await transaction.get<RelationshipData>(id)) ?? new Set();
    relations.add(relatedToId);

    await transaction.put<RelationshipData>(id, relations);
  }

  private async addRelationshipBatch(
    transaction: DurableObjectTransaction,
    relations: RelationshipTuple[],
  ) {
    const existing = await transaction.get<RelationshipData>(
      relations.map(([relationRoot]) => relationRoot),
    );

    const update: Record<string, RelationshipData> = {};
    for (const [source, target] of relations) {
      const allRelations = update[source] ?? existing.get(source) ?? new Set();
      if (!allRelations.has(target)) {
        allRelations.add(target);
      }
      update[source] = allRelations;
    }

    await transaction.put(update);
  }

  private async addRelationshipNameMapping(
    transaction: DurableObjectTransaction,
    nodeAToBRelationshipName: RelationshipName,
    nodeBToARelationshipName: RelationshipName,
  ) {
    await transaction.put({
      [mappingId(nodeAToBRelationshipName)]: nodeBToARelationshipName,
      [mappingId(nodeBToARelationshipName)]: nodeAToBRelationshipName,
    });
  }

  private async deleteRelationship(
    transaction: DurableObjectTransaction,
    id: string,
    relatedToId: string,
  ) {
    const relations = (await transaction.get<RelationshipData>(id)) ?? new Set();
    relations.delete(relatedToId);

    await transaction.put<RelationshipData>(id, relations);
  }

  private async deleteRelationshipBatch(
    transaction: DurableObjectTransaction,
    relations: RelationshipTuple[],
  ) {
    const existing = await transaction.get<RelationshipData>(
      relations.map(([relationRoot]) => relationRoot),
    );

    const update: Record<string, RelationshipData> = {};
    for (const [source, target] of relations) {
      const allRelations = update[source] ?? existing.get(source) ?? new Set();
      if (allRelations.has(target)) {
        allRelations.delete(target);
      }
      update[source] = allRelations;
    }

    await transaction.put(update);
  }

  private async hasRelationship(
    info: RequestInfo,
  ): Promise<Result<ReadRelationshipResponse, StorageError>> {
    const input = info.body<ReadRelationshipRequest>(isReadRelationshipRequest);

    const relationshipId = id(input.name, input.nodeA);
    const relationships = await this.state.storage.get<RelationshipData>(relationshipId, {
      allowConcurrency: true,
    });

    if (!relationships) {
      return Result.err(new StorageNotFoundError());
    }
    return Result.ok({ exists: relationships.has(input.nodeB) });
  }

  private async removeRelationship(
    info: RequestInfo,
  ): Promise<Result<RemoveRelationshipResponse, StorageError>> {
    const input = info.body<RemoveRelationshipRequest>(isRemoveRelationshipRequest);

    if ('node' in input) {
      await this.clearAllRelationshipsForNode(input.node);
      return Result.ok({ success: true });
    }

    const nodeAId = id(input.nodeAToBRelationshipName, input.nodeA);
    const nodeBId = id(input.nodeBToARelationshipName, input.nodeB);

    try {
      await this.state.storage.transaction(async (transaction) => {
        await this.deleteRelationship(transaction, nodeAId, input.nodeB);
        await this.deleteRelationship(transaction, nodeBId, input.nodeA);
      });

      return Result.ok({ success: true });
    } catch (err) {
      return Result.ok({ success: false });
    }
  }

  private async clearAllRelationshipsForNode(node: string) {
    await this.state.storage.transaction(async (transaction) => {
      const relationsRight = await transaction.list<RelationshipData>({
        prefix: prefix(node),
      });

      const relationsRightIds = Array.from(relationsRight.keys());
      const relationsLeftIds: RelationshipTuple[] = [];

      const relationsNamesIds = relationsRightIds.map((id) =>
        mappingId(id.split('$')[2] as RelationshipName),
      );
      const relationsNamesMapping = await transaction.get<RelationshipName>(
        relationsNamesIds,
      );

      for (const [source, targets] of Array.from(relationsRight.entries())) {
        for (const target of targets) {
          const [, , relationshipName] = source.split('$');

          const relationName = relationshipName as RelationshipName;
          const relationsReverseName = relationsNamesMapping.get(mappingId(relationName));

          if (!relationsReverseName) {
            continue;
          }

          relationsLeftIds.push([id(relationsReverseName, target), node]);
        }
      }

      // `a` is being deleted below:
      // - Delete all relations which go from a -> b
      // - Update all relations which go from a <- b
      await Promise.all([
        transaction.delete(relationsRightIds),
        this.deleteRelationshipBatch(transaction, relationsLeftIds),
      ]);
    });
  }

  private async removeRelationshipBatch(
    info: RequestInfo,
  ): Promise<Result<RemoveRelationshipBatchResponse, StorageError>> {
    const input = info.body(isRemoveRelationshipBatchRequest);

    const { relationsRight, relationsLeft } = this.parseBatchRequest(input);

    await this.state.storage.transaction(async (transaction) => {
      await this.deleteRelationshipBatch(transaction, relationsRight);
    });

    await this.state.storage.transaction(async (transaction) => {
      await this.deleteRelationshipBatch(transaction, relationsLeft);
    });

    return Result.ok({ success: true });
  }

  private parseBatchRequest(input: RelationshipRequest[]) {
    const relationsRight: RelationshipTuple[] = [];
    const relationsLeft: RelationshipTuple[] = [];

    for (const relation of input) {
      const nodeAId = id(relation.nodeAToBRelationshipName, relation.nodeA);
      const nodeBId = id(relation.nodeBToARelationshipName, relation.nodeB);

      relationsRight.push([nodeAId, relation.nodeB]);
      relationsLeft.push([nodeBId, relation.nodeA]);
    }
    return { relationsRight, relationsLeft };
  }

  private async listRelationship(
    info: RequestInfo,
  ): Promise<Result<ListRelationshipResponse, StorageError>> {
    const { name, node, first, last, after, before } = info.body<ListRelationshipRequest>(
      isListRelationshipRequest,
    );

    const relationshipId = id(name, node);
    const relationships =
      (await this.state.storage.get<RelationshipData>(relationshipId, {
        allowConcurrency: true,
      })) ?? new Set();

    let ids = Array.from(relationships.values());
    const length = ids.length;

    let hasAfter = false;
    let hasBefore = false;

    if (first && before) {
      throw new StorageBadRequestError('cannot supply `first` and `before` together');
    }
    if (last && after) {
      throw new StorageBadRequestError('cannot supply `last` and `after` together');
    }
    if (first && last) {
      throw new StorageBadRequestError('cannot supply `first` and `last` together');
    }

    let startAtIdx = after ? this.findOrThrow(ids, after, 1) : 0;
    let endAtIdx = before ? this.findOrThrow(ids, before, -1) : ids.length - 1;

    if (first) {
      endAtIdx = startAtIdx + first - 1;
      ids = ids.slice(startAtIdx, endAtIdx + 1);
    }
    if (last) {
      startAtIdx = endAtIdx - last + 1;
      ids = ids.slice(startAtIdx, endAtIdx + 1);
    }

    if (startAtIdx > 0) {
      hasBefore = true;
    }
    if (endAtIdx < length - 1) {
      hasAfter = true;
    }

    return Result.ok({ relationships: ids, hasBefore, hasAfter });
  }

  private findOrThrow(ids: string[], targetId: string, increment: number) {
    const idx = ids.findIndex((r) => r === targetId);
    if (idx === -1) {
      throw new StorageNotFoundError();
    }

    return idx + increment;
  }
}
