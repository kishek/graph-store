import { expect, test } from 'vitest';
import { env } from 'cloudflare:test';

import { RelationshipName, StorageEnvironment } from './storage';
import { StorageClient } from './storage-client';

import '../example';
import { isCreateQueryRequest, isUpdateQueryRequest } from './query/query-request';

declare module 'cloudflare:test' {
  interface ProvidedEnv extends StorageEnvironment {}
}

function getStorageClient() {
  const storageId = env.GRAPH_STORAGE.newUniqueId();
  const storage = env.GRAPH_STORAGE.get(storageId);

  const client = StorageClient.from(storage, { namespace: 'test-storage' });
  return client;
}

async function createManyRelationships(
  client: StorageClient,
  relations = ['b', 'c', 'd', 'e'],
  parent = 'a',
) {
  for await (const r of relations) {
    await client.createRelationship({
      nodeA: parent,
      nodeB: r,
      nodeAToBRelationshipName: 'parent' as RelationshipName,
      nodeBToARelationshipName: 'child' as RelationshipName,
    });
  }
}

test('creates index', async () => {
  const client = getStorageClient();
  const index = await client.createIndex({
    property: 'indexed-property',
  });

  const indexes = (await client.listIndexes()).unwrap();
  expect(indexes).toMatchObject({
    [index.unwrap().id]: {
      property: 'indexed-property',
    },
  });
});

test('reads index', async () => {
  const client = getStorageClient();
  const response = await client.createIndex({
    property: 'indexed-property',
  });
  const index = response.unwrap();

  const indexRead = await client.readIndex({
    id: index.id,
  });

  expect(indexRead.unwrap()).toEqual({
    id: index.id,
    property: 'indexed-property',
  });
});

test('updates index', async () => {
  const client = getStorageClient();
  const result = await client.createIndex({
    property: 'indexed-property',
  });
  const index = result.unwrap();

  const indexUpdated = await client.updateIndex({
    id: index.id,
    property: 'indexed-property-updated',
  });

  const indexes = await client.listIndexes();
  expect(indexes.unwrap()).toEqual({
    [index.id]: {
      id: indexUpdated.unwrap().id,
      property: 'indexed-property-updated',
    },
  });
});

test('removes index', async () => {
  const client = getStorageClient();
  const result = await client.createIndex({
    property: 'indexed-property',
  });
  const index = result.unwrap();

  const indexRemoved = await client.removeIndex({
    id: index.id,
  });
  expect(indexRemoved.unwrap()).toEqual({ success: true });

  const indexes = await client.listIndexes();
  expect(indexes.unwrap()).toEqual({});
});

interface TestEntity {
  a: number;
  b: number;
  c: number;
}

interface TestEntityWithId {
  id: string;
  a: number;
  b: number;
  c: number;
}

test('creates entity in database', async () => {
  const client = getStorageClient();
  await client.createQuery<TestEntity>({
    key: 'entity-a',
    value: {
      a: 1,
      b: 2,
      c: 3,
    },
  });

  const items = await client.listQuery<TestEntity>({ key: 'entity' });
  expect(items.unwrap()).toMatchObject({
    'entity-a': {
      a: 1,
      b: 2,
      c: 3,
    },
  });
});

test('batch creates entities in database', async () => {
  const client = getStorageClient();
  await client.batchCreateQuery<TestEntity>({
    entries: {
      'entity-a': {
        a: 1,
        b: 2,
        c: 3,
      },
      'entity-b': {
        a: 9,
        b: 10,
        c: 11,
      },
    },
  });

  const items = await client.listQuery<TestEntity>({ key: 'entity' });
  expect(items.unwrap()).toMatchObject({
    'entity-a': {
      a: 1,
      b: 2,
      c: 3,
    },
    'entity-b': {
      a: 9,
      b: 10,
      c: 11,
    },
  });
});

test('reads entity in database', async () => {
  const client = getStorageClient();
  const result = await client.createQuery<TestEntity>({
    key: 'entity-a',
    value: {
      a: 1,
      b: 2,
      c: 3,
    },
  });
  const entity = result.unwrap();

  const items = await client.readQuery<TestEntity>({ key: entity.id });
  expect(items.unwrap()).toMatchObject({
    a: 1,
    b: 2,
    c: 3,
  });
});

test('batch reads entities from database', async () => {
  const client = getStorageClient();
  await client.createQuery<TestEntity>({
    key: 'entity-a',
    value: {
      a: 1,
      b: 2,
      c: 3,
    },
  });
  await client.createQuery<TestEntity>({
    key: 'entity-b',
    value: {
      a: 4,
      b: 5,
      c: 6,
    },
  });

  const items = await client.batchReadQuery<TestEntity>({
    keys: ['entity-a', 'entity-b'],
  });
  expect(items.unwrap()).toEqual(
    expect.arrayContaining([
      {
        id: 'entity-a',
        a: 1,
        b: 2,
        c: 3,
      },
      {
        id: 'entity-b',
        a: 4,
        b: 5,
        c: 6,
      },
    ]),
  );
});

test('reading entity which does not exist yields 404', async () => {
  const client = getStorageClient();
  const response = await client.readQuery<TestEntity>({ key: 'a' });

  expect(() => response.unwrap()).toThrowErrorMatchingInlineSnapshot(
    `[HTTPError: 404: row not found]`,
  );
});

test('updates entity in database', async () => {
  const client = getStorageClient();
  const result = await client.createQuery<TestEntity>({
    key: 'entity-a',
    value: {
      a: 1,
      b: 2,
      c: 3,
    },
  });
  const entity = result.unwrap();

  await client.updateQuery<TestEntity>({
    key: entity.id,
    value: {
      a: 2,
      b: 2,
      c: 2,
    },
  });

  const items = await client.listQuery<TestEntity>({ key: 'entity' });
  expect(items.unwrap()).toMatchObject({
    'entity-a': {
      a: 2,
      b: 2,
      c: 2,
    },
  });
});

test('batch updates entities in database', async () => {
  const client = getStorageClient();
  await client.batchCreateQuery<TestEntity>({
    entries: {
      'entity-a': {
        a: 1,
        b: 2,
        c: 3,
      },
      'entity-b': {
        a: 4,
        b: 5,
        c: 6,
      },
    },
  });

  await client.batchUpdateQuery<TestEntity>({
    entries: {
      'entity-a': {
        a: 101,
      },
      'entity-b': {
        a: 104,
      },
    },
  });

  const items = await client.listQuery<TestEntity>({ key: 'entity' });
  expect(items.unwrap()).toMatchObject({
    'entity-a': {
      a: 101,
    },
    'entity-b': {
      a: 104,
    },
  });
});

test('batch updates entities in database including indexes', async () => {
  const client = getStorageClient();

  await client.createIndex({ property: 'a' });
  await client.batchCreateQuery<TestEntity>({
    entries: {
      'entity-a': {
        a: 1,
        b: 2,
        c: 3,
      },
      'entity-b': {
        a: 4,
        b: 5,
        c: 6,
      },
    },
  });

  await client.batchUpdateQuery<TestEntity>({
    entries: {
      'entity-a': {
        a: 101,
      },
      'entity-b': {
        a: 104,
      },
    },
  });

  const items = await client.listQuery<TestEntity>({ index: 'a' });
  const results = items.unwrap();

  expect(results).toMatchObject({
    'entity-a': {
      a: 101,
    },
    'entity-b': {
      a: 104,
    },
  });
  expect(Object.keys(results)).toHaveLength(2);
});

test('batch upserts entities in database', async () => {
  const client = getStorageClient();
  await client.batchCreateQuery<TestEntity>({
    entries: {
      'entity-a': {
        a: 1,
        b: 2,
        c: 3,
      },
    },
  });

  await client.batchUpsertQuery<TestEntity>({
    entries: {
      'entity-a': {
        a: 101,
        b: 2,
        c: 3,
      },
      'entity-b': {
        a: 4,
        b: 5,
        c: 6,
      },
    },
  });

  const items = await client.listQuery<TestEntity>({ key: 'entity' });
  expect(items.unwrap()).toMatchObject({
    'entity-a': {
      a: 101,
    },
    'entity-b': {
      a: 4,
    },
  });
});

test('batch upserts entities in database including indexes', async () => {
  const client = getStorageClient();

  await client.createIndex({ property: 'a' });
  await client.batchCreateQuery<TestEntity>({
    entries: {
      'entity-a': {
        a: 1,
        b: 2,
        c: 3,
      },
    },
  });

  await client.batchUpsertQuery<TestEntity>({
    entries: {
      'entity-a': {
        a: 101,
        b: 2,
        c: 3,
      },
      'entity-b': {
        a: 104,
        b: 5,
        c: 6,
      },
    },
  });

  const items = await client.listQuery<TestEntity>({ index: 'a' });
  const results = items.unwrap();

  expect(results).toMatchObject({
    'entity-a': {
      a: 101,
    },
    'entity-b': {
      a: 104,
    },
  });
  expect(Object.keys(results)).toHaveLength(2);
});

test('removes entity in database', async () => {
  const client = getStorageClient();
  const result = await client.createQuery<TestEntity>({
    key: 'entity-a',
    value: {
      a: 1,
      b: 2,
      c: 3,
    },
  });
  const entity = result.unwrap();

  await client.removeQuery({ key: entity.id });

  const items = await client.listQuery<TestEntity>({ key: 'entity' });
  expect(items.unwrap()).toMatchObject({});
});

test('removes batch of entities in database', async () => {
  const client = getStorageClient();
  const result = await client.batchCreateQuery<TestEntity>({
    entries: {
      'entity-a': {
        a: 1,
        b: 2,
        c: 3,
      },
      'entity-b': {
        a: 4,
        b: 5,
        c: 6,
      },
    },
  });
  const entities = result.unwrap();

  await client.batchRemoveQuery({ keys: entities.map((e) => e.id) });

  const items = await client.listQuery<TestEntity>({ key: 'entity' });
  expect(Object.keys(items.unwrap())).toHaveLength(0);
});

test('creating entity in database creates entry in storage index', async () => {
  const client = getStorageClient();

  await client.createIndex({ property: 'a' });
  await client.createQuery<TestEntity>({
    key: 'entity-a',
    value: {
      a: 1,
      b: 2,
      c: 3,
    },
  });

  const items = await client.listQuery<TestEntity>({ index: 'a' });
  expect(items.unwrap()).toMatchObject({
    'entity-a': {
      a: 1,
      b: 2,
      c: 3,
    },
  });
});

test('creating entity in database does not yield indexed entry', async () => {
  const client = getStorageClient();

  await client.createIndex({ property: 'a' });
  const result = await client.createQuery<TestEntity>({
    key: 'entity-a',
    value: {
      a: 1,
      b: 2,
      c: 3,
    },
  });

  expect(result.unwrap()).toMatchObject({
    a: 1,
    b: 2,
    c: 3,
  });
});

test('creating multiple entities in database does not yield indexed entries', async () => {
  const client = getStorageClient();

  await client.createIndex({ property: 'a' });
  const result = await client.batchCreateQuery<TestEntity>({
    entries: {
      'entity-a': {
        a: 1,
        b: 2,
        c: 3,
      },
      'entity-b': {
        a: 4,
        b: 5,
        c: 6,
      },
    },
  });

  expect(result.unwrap()).toHaveLength(2);
});

test('reads entity in database using indexed key', async () => {
  const client = getStorageClient();

  await client.createIndex({ property: 'a' });
  await client.createQuery<TestEntity>({
    key: 'entity-a',
    value: {
      a: 1,
      b: 2,
      c: 3,
    },
  });

  const items = await client.readQuery<TestEntity>({ key: '1', index: 'a' });
  expect(items.unwrap()).toMatchObject({
    a: 1,
  });
});

test('reads entity in database using indexed key with unchanged id', async () => {
  const client = getStorageClient();

  await client.createIndex({ property: 'a' });
  await client.createQuery<TestEntityWithId>({
    key: 'entity-a',
    value: {
      id: 'entity-a-id',
      a: 1,
      b: 2,
      c: 3,
    },
  });

  const items = await client.readQuery<TestEntityWithId>({ key: '1', index: 'a' });
  expect(items.unwrap()).toMatchObject({
    id: 'entity-a-id',
  });
});

test('reads multiple entities in database using indexed key with unchanged ids', async () => {
  const client = getStorageClient();

  await client.createIndex({ property: 'a' });
  await client.createQuery<TestEntityWithId>({
    key: 'entity-a',
    value: {
      id: 'entity-a-id',
      a: 1,
      b: 2,
      c: 3,
    },
  });
  await client.createQuery<TestEntityWithId>({
    key: 'entity-b',
    value: {
      id: 'entity-b-id',
      a: 4,
      b: 5,
      c: 6,
    },
  });

  const items = await client.batchReadQuery<TestEntityWithId>({
    keys: ['entity-a', 'entity-b'],
  });
  expect(items.unwrap()).toEqual(
    expect.arrayContaining([
      expect.objectContaining({
        id: 'entity-a-id',
      }),
      expect.objectContaining({
        id: 'entity-b-id',
      }),
    ]),
  );
});

test('updating an entity in database also updates value in index', async () => {
  const client = getStorageClient();

  await client.createIndex({ property: 'a' });
  await client.createQuery<TestEntity>({
    key: 'entity-a',
    value: {
      a: 1,
      b: 2,
      c: 3,
    },
  });
  await client.updateQuery<TestEntity>({
    key: 'entity-a',
    value: {
      a: 4,
    },
  });

  const items = await client.listQuery<TestEntity>({ key: '4', index: 'a' });
  expect(items.unwrap()).toMatchObject({
    'entity-a': {
      a: 4,
    },
  });
});

test('removing entity from database also removes it from index', async () => {
  const client = getStorageClient();

  await client.createIndex({ property: 'a' });
  await client.createQuery<TestEntity>({
    key: 'entity-a',
    value: {
      a: 1,
      b: 2,
      c: 3,
    },
  });
  await client.removeQuery({ key: 'entity-a' });

  const items = await client.listQuery<TestEntity>({ key: '1', index: 'a' });
  expect(items.unwrap()).toMatchObject({});
});

test('creating entity in database creates entry in all storage indexes', async () => {
  const client = getStorageClient();
  const value = {
    a: 1,
    b: 2,
    c: 3,
  };

  await client.createIndex({ property: 'a' });
  await client.createIndex({ property: 'b' });
  await client.createIndex({ property: 'c' });

  await client.createQuery<TestEntity>({
    key: 'entity-a',
    value,
  });

  expect(
    (await client.listQuery<TestEntity>({ key: '1', index: 'a' })).unwrap(),
  ).toMatchObject({
    'entity-a': value,
  });
  expect(
    (await client.listQuery<TestEntity>({ key: '2', index: 'b' })).unwrap(),
  ).toMatchObject({
    'entity-a': value,
  });
  expect(
    (await client.listQuery<TestEntity>({ key: '3', index: 'c' })).unwrap(),
  ).toMatchObject({
    'entity-a': value,
  });
});

test('updating an entity in database also updates values in all storage indexes', async () => {
  const client = getStorageClient();

  await client.createIndex({ property: 'a' });
  await client.createIndex({ property: 'b' });
  await client.createIndex({ property: 'c' });

  await client.createQuery<TestEntity>({
    key: 'entity-a',
    value: {
      a: 1,
      b: 2,
      c: 3,
    },
  });
  await client.updateQuery<TestEntity>({
    key: 'entity-a',
    value: {
      a: 4,
    },
  });

  expect(
    (await client.listQuery<TestEntity>({ key: '4', index: 'a' })).unwrap(),
  ).toMatchObject({
    'entity-a': {
      a: 4,
    },
  });
  expect(
    (await client.listQuery<TestEntity>({ key: '2', index: 'b' })).unwrap(),
  ).toMatchObject({
    'entity-a': {
      a: 4,
    },
  });
  expect(
    (await client.listQuery<TestEntity>({ key: '3', index: 'c' })).unwrap(),
  ).toMatchObject({
    'entity-a': {
      a: 4,
    },
  });
});

test('removing entity from database also removes it from all indexes', async () => {
  const client = getStorageClient();

  await client.createIndex({ property: 'a' });
  await client.createIndex({ property: 'b' });
  await client.createIndex({ property: 'c' });

  await client.createQuery<TestEntity>({
    key: 'entity-a',
    value: {
      a: 1,
      b: 2,
      c: 3,
    },
  });
  await client.removeQuery({ key: 'entity-a' });

  expect(
    (await client.listQuery<TestEntity>({ key: 'entity', index: 'a' })).unwrap(),
  ).toMatchObject({});
  expect(
    (await client.listQuery<TestEntity>({ key: 'entity', index: 'b' })).unwrap(),
  ).toMatchObject({});
  expect(
    (await client.listQuery<TestEntity>({ key: 'entity', index: 'c' })).unwrap(),
  ).toMatchObject({});
});

test('removing entity from database also removes it from all relationships', async () => {
  const client = getStorageClient();

  await client.createQuery<TestEntity>({
    key: 'a',
    value: {
      a: 1,
      b: 2,
      c: 3,
    },
  });
  await client.createRelationship({
    nodeA: 'a',
    nodeB: 'b',
    nodeAToBRelationshipName: 'children' as RelationshipName,
    nodeBToARelationshipName: 'parents' as RelationshipName,
  });
  await client.removeQuery({
    key: 'a',
  });

  const [children, parents] = await Promise.all([
    client.listRelationship({
      name: 'children' as RelationshipName,
      node: 'a',
    }),
    client.listRelationship({
      name: 'parents' as RelationshipName,
      node: 'b',
    }),
  ]);

  expect(parents.unwrap().relationships).toHaveLength(0);
  expect(children.unwrap().relationships).toHaveLength(0);
});

test('creates parent relationships', async () => {
  const client = getStorageClient();

  await client.createRelationship({
    nodeA: 'a',
    nodeB: 'b',
    nodeAToBRelationshipName: 'parent' as RelationshipName,
    nodeBToARelationshipName: 'child' as RelationshipName,
  });
  await client.createRelationship({
    nodeA: 'a',
    nodeB: 'c',
    nodeAToBRelationshipName: 'parent' as RelationshipName,
    nodeBToARelationshipName: 'child' as RelationshipName,
  });

  const result = await client.listRelationship({
    name: 'parent' as RelationshipName,
    node: 'a',
  });

  expect(result.unwrap().relationships).toHaveLength(2);
  expect(result.unwrap().relationships[0]).toMatchInlineSnapshot(`"b"`);
  expect(result.unwrap().relationships[1]).toMatchInlineSnapshot(`"c"`);
});

test('creates child relationship', async () => {
  const client = getStorageClient();

  await client.createRelationship({
    nodeA: 'a',
    nodeB: 'b',
    nodeAToBRelationshipName: 'parent' as RelationshipName,
    nodeBToARelationshipName: 'child' as RelationshipName,
  });
  await client.createRelationship({
    nodeA: 'aa',
    nodeB: 'b',
    nodeAToBRelationshipName: 'parent' as RelationshipName,
    nodeBToARelationshipName: 'child' as RelationshipName,
  });

  const result = await client.listRelationship({
    name: 'child' as RelationshipName,
    node: 'b',
  });
  expect(result.unwrap().relationships).toHaveLength(2);
  expect(result.unwrap().relationships[0]).toMatchInlineSnapshot(`"a"`);
  expect(result.unwrap().relationships[1]).toMatchInlineSnapshot(`"aa"`);
});

test('batch creates child relationships', async () => {
  const client = getStorageClient();

  await client.createRelationshipBatch([
    {
      nodeA: 'a',
      nodeB: 'b',
      nodeAToBRelationshipName: 'children' as RelationshipName,
      nodeBToARelationshipName: 'parents' as RelationshipName,
    },
    {
      nodeA: 'aa',
      nodeB: 'b',
      nodeAToBRelationshipName: 'children' as RelationshipName,
      nodeBToARelationshipName: 'parents' as RelationshipName,
    },
  ]);

  const result = await client.listRelationship({
    name: 'parents' as RelationshipName,
    node: 'b',
  });
  expect(result.unwrap().relationships).toHaveLength(2);
  expect(result.unwrap().relationships[0]).toMatchInlineSnapshot(`"a"`);
  expect(result.unwrap().relationships[1]).toMatchInlineSnapshot(`"aa"`);
});

test('removes relationship in both directions', async () => {
  const client = getStorageClient();

  await client.createRelationship({
    nodeA: 'a',
    nodeB: 'b',
    nodeAToBRelationshipName: 'children' as RelationshipName,
    nodeBToARelationshipName: 'parents' as RelationshipName,
  });
  const result = await client.removeRelationship({
    nodeA: 'a',
    nodeB: 'b',
    nodeAToBRelationshipName: 'children' as RelationshipName,
    nodeBToARelationshipName: 'parents' as RelationshipName,
  });

  const [children, parents] = await Promise.all([
    client.listRelationship({
      name: 'children' as RelationshipName,
      node: 'a',
    }),
    client.listRelationship({
      name: 'parents' as RelationshipName,
      node: 'b',
    }),
  ]);

  expect(result.unwrap().success).toBe(true);
  expect(parents.unwrap().relationships).toHaveLength(0);
  expect(children.unwrap().relationships).toHaveLength(0);
});

test('batch removes relationship in both directions', async () => {
  const client = getStorageClient();

  await client.createRelationshipBatch([
    {
      nodeA: 'a',
      nodeB: 'b',
      nodeAToBRelationshipName: 'children' as RelationshipName,
      nodeBToARelationshipName: 'parents' as RelationshipName,
    },
    {
      nodeA: 'a',
      nodeB: 'bb',
      nodeAToBRelationshipName: 'children' as RelationshipName,
      nodeBToARelationshipName: 'parents' as RelationshipName,
    },
  ]);
  const result = await client.removeRelationshipBatch([
    {
      nodeA: 'a',
      nodeB: 'b',
      nodeAToBRelationshipName: 'children' as RelationshipName,
      nodeBToARelationshipName: 'parents' as RelationshipName,
    },
    {
      nodeA: 'a',
      nodeB: 'bb',
      nodeAToBRelationshipName: 'children' as RelationshipName,
      nodeBToARelationshipName: 'parents' as RelationshipName,
    },
  ]);

  const [children, ...parents] = await Promise.all([
    client.listRelationship({
      name: 'children' as RelationshipName,
      node: 'a',
    }),
    client.listRelationship({
      name: 'parents' as RelationshipName,
      node: 'b',
    }),
    client.listRelationship({
      name: 'parents' as RelationshipName,
      node: 'bb',
    }),
  ]);

  expect(result.unwrap().success).toBe(true);
  expect(children.unwrap().relationships).toHaveLength(0);
  expect(parents[0].unwrap().relationships).toHaveLength(0);
  expect(parents[1].unwrap().relationships).toHaveLength(0);
});

test('removes relationship without affecting others', async () => {
  const client = getStorageClient();

  await client.createRelationship({
    nodeA: 'a',
    nodeB: 'b',
    nodeAToBRelationshipName: 'parent' as RelationshipName,
    nodeBToARelationshipName: 'child' as RelationshipName,
  });
  await client.createRelationship({
    nodeA: 'a',
    nodeB: 'c',
    nodeAToBRelationshipName: 'parent' as RelationshipName,
    nodeBToARelationshipName: 'child' as RelationshipName,
  });

  await client.removeRelationship({
    nodeA: 'a',
    nodeB: 'c',
    nodeAToBRelationshipName: 'parent' as RelationshipName,
    nodeBToARelationshipName: 'child' as RelationshipName,
  });
  const [children, parents] = await Promise.all([
    client.listRelationship({
      name: 'parent' as RelationshipName,
      node: 'a',
    }),
    client.listRelationship({
      name: 'child' as RelationshipName,
      node: 'b',
    }),
  ]);

  expect(children.unwrap().relationships).toEqual(expect.arrayContaining(['b']));
  expect(parents.unwrap().relationships).toEqual(expect.arrayContaining(['a']));
});

test('is able to find parent relationship after creation', async () => {
  const client = getStorageClient();

  await client.createRelationship({
    nodeA: 'a',
    nodeB: 'b',
    nodeAToBRelationshipName: 'parent' as RelationshipName,
    nodeBToARelationshipName: 'child' as RelationshipName,
  });

  const result = await client.hasRelationship({
    nodeA: 'a',
    nodeB: 'b',
    name: 'parent' as RelationshipName,
  });

  expect(result.unwrap().exists).toBe(true);
});

test('is able to find child relationship after creation', async () => {
  const client = getStorageClient();

  await client.createRelationship({
    nodeA: 'a',
    nodeB: 'b',
    nodeAToBRelationshipName: 'parent' as RelationshipName,
    nodeBToARelationshipName: 'child' as RelationshipName,
  });

  const result = await client.hasRelationship({
    nodeA: 'b',
    nodeB: 'a',
    name: 'child' as RelationshipName,
  });

  expect(result.unwrap().exists).toBe(true);
});

test('is able to list relationships all under node id', async () => {
  const client = getStorageClient();

  await createManyRelationships(client);

  const result = await client.listRelationship({
    node: 'a',
    name: 'parent' as RelationshipName,
  });

  expect(result.unwrap()).toEqual({
    relationships: ['b', 'c', 'd', 'e'],
    hasBefore: false,
    hasAfter: false,
  });
});

test('is able to list relationships under multiple node ids', async () => {
  const client = getStorageClient();

  await createManyRelationships(client, ['b', 'c', 'd', 'e'], 'aa');
  await createManyRelationships(client, ['b', 'c', 'd', 'e'], 'ab');

  const result = await client.batchListRelationship({
    requests: [
      {
        name: 'parent' as RelationshipName,
        node: 'aa',
        first: 2,
      },
      {
        name: 'parent' as RelationshipName,
        node: 'ab',
        first: 3,
      },
    ],
  });

  expect(result.unwrap()).toEqual([
    {
      relationships: ['b', 'c'],
      hasBefore: false,
      hasAfter: true,
    },
    {
      relationships: ['b', 'c', 'd'],
      hasBefore: false,
      hasAfter: true,
    },
  ]);
});

test('is able to list first N relationships under node id', async () => {
  const client = getStorageClient();

  await createManyRelationships(client);

  const result = await client.listRelationship({
    node: 'a',
    name: 'parent' as RelationshipName,
    first: 2,
  });

  expect(result.unwrap()).toEqual({
    relationships: ['b', 'c'],
    hasBefore: false,
    hasAfter: true,
  });
});

test('is able to list last N relationships under node id', async () => {
  const client = getStorageClient();

  await createManyRelationships(client);

  const result = await client.listRelationship({
    node: 'a',
    name: 'parent' as RelationshipName,
    last: 2,
  });

  expect(result.unwrap()).toEqual({
    relationships: ['d', 'e'],
    hasBefore: true,
    hasAfter: false,
  });
});

test('is able to list first N relationships after a node', async () => {
  const client = getStorageClient();

  await createManyRelationships(client);

  const result = await client.listRelationship({
    node: 'a',
    name: 'parent' as RelationshipName,
    first: 2,
    after: 'b',
  });

  expect(result.unwrap()).toEqual({
    relationships: ['c', 'd'],
    hasBefore: true,
    hasAfter: true,
  });
});

test('is able to list last N relationships before a node', async () => {
  const client = getStorageClient();

  await createManyRelationships(client);

  const result = await client.listRelationship({
    node: 'a',
    name: 'parent' as RelationshipName,
    last: 2,
    before: 'd',
  });

  expect(result.unwrap()).toEqual({
    relationships: ['b', 'c'],
    hasBefore: false,
    hasAfter: true,
  });
});

test('is able to purge all data', async () => {
  const client = getStorageClient();

  await client.createQuery<TestEntity>({
    key: 'entity-a',
    value: {
      a: 1,
      b: 2,
      c: 3,
    },
  });

  await client.purgeAllQuery();

  const items = await client.listQuery<TestEntity>({ key: 'entity' });
  expect(items.unwrap()).toMatchObject({});
});

test('is able to backup all data', async () => {
  const client = getStorageClient();

  await client.createQuery<TestEntity>({
    key: 'entity-a',
    value: {
      a: 1,
      b: 2,
      c: 3,
    },
  });

  const result = await client.backup();
  if (!result.isOk) {
    throw result.error;
  }

  const backup = await env.GRAPH_STORAGE_BACKUP.list();

  expect(backup.objects).toHaveLength(1);
  expect(backup.objects[0].size).toBeGreaterThan(32);
});

test('is able to restore all data', async () => {
  const client = getStorageClient();

  await client.batchCreateQuery<TestEntity>({
    entries: {
      'entity-a': {
        a: 1,
        b: 2,
        c: 3,
      },
      'entity-b': {
        a: 4,
        b: 5,
        c: 6,
      },
    },
  });

  const result = await client.backup();
  if (!result.isOk) {
    throw result.error;
  }

  const backupId = result.unwrap();

  await client.purgeAllQuery();
  await client.restore({ backupId });

  const items = await client.listQuery<TestEntity>({ key: 'entity' });
  expect(items.unwrap()).toMatchObject({
    'entity-a': {
      a: 1,
      b: 2,
      c: 3,
    },
    'entity-b': {
      a: 4,
      b: 5,
      c: 6,
    },
  });
});

test('is able to apply middleware', async () => {
  const client = getStorageClient();

  client.use((ctx) => {
    if (ctx.type === 'query') {
      if (isCreateQueryRequest<any>(ctx.request)) {
        ctx.request.value = {
          ...ctx.request.value,
          createdBy: 'test-user',
        };
      }
      if (isUpdateQueryRequest<any>(ctx.request)) {
        ctx.request.value = {
          ...ctx.request.value,
          updatedBy: 'test-user-updated',
        };
      }
    }

    return ctx;
  });

  await client.createQuery<TestEntity>({
    key: 'a',
    value: {
      a: 1,
      b: 2,
      c: 3,
    },
  });

  const entityUpdated = await client.updateQuery<TestEntity>({
    key: 'a',
    value: {
      a: 3,
    },
  });

  expect(entityUpdated.unwrap()).toMatchObject({
    a: 3,
    b: 2,
    c: 3,
    createdBy: 'test-user',
    updatedBy: 'test-user-updated',
  });
});

test('able to list first N entities in list operation', async () => {
  const client = getStorageClient();

  await client.batchCreateQuery<TestEntity>({
    entries: {
      'entity-a': {
        a: 1,
        b: 2,
        c: 3,
      },
      'entity-b': {
        a: 4,
        b: 5,
        c: 6,
      },
    },
  });

  const items = await client.listQuery<TestEntity>({ key: 'entity', first: 1 });
  const result = items.unwrap();

  expect(Object.keys(result)).toHaveLength(1);
  expect(result).toMatchObject({
    'entity-a': {
      a: 1,
      b: 2,
      c: 3,
    },
  });
});

test('able to list last N entities in list operation', async () => {
  const client = getStorageClient();

  await client.batchCreateQuery<TestEntity>({
    entries: {
      'entity-a': {
        a: 1,
        b: 2,
        c: 3,
      },
      'entity-b': {
        a: 4,
        b: 5,
        c: 6,
      },
    },
  });

  const items = await client.listQuery<TestEntity>({ key: 'entity', last: 1 });
  const result = items.unwrap();

  expect(Object.keys(result)).toHaveLength(1);
  expect(result).toMatchObject({
    'entity-b': {
      a: 4,
      b: 5,
      c: 6,
    },
  });
});

test('able to list last N entities in list operation', async () => {
  const client = getStorageClient();

  await client.batchCreateQuery<TestEntity>({
    entries: {
      'entity-a': {
        a: 1,
        b: 2,
        c: 3,
      },
      'entity-b': {
        a: 4,
        b: 5,
        c: 6,
      },
    },
  });

  const items = await client.listQuery<TestEntity>({ key: 'entity', last: 1 });
  const result = items.unwrap();

  expect(Object.keys(result)).toHaveLength(1);
  expect(result).toMatchObject({
    'entity-b': {
      a: 4,
      b: 5,
      c: 6,
    },
  });
});

test('able to list entities after a specific entity', async () => {
  const client = getStorageClient();

  const created = await client.batchCreateQuery<TestEntity>({
    entries: {
      'entity-a': {
        a: 1,
        b: 2,
        c: 3,
      },
      'entity-b': {
        a: 4,
        b: 5,
        c: 6,
      },
    },
  });
  const cursor = created.unwrap()[0].id;

  const items = await client.listQuery<TestEntity>({ key: 'entity', after: cursor });
  const result = items.unwrap();

  expect(Object.keys(result)).toHaveLength(1);
  expect(result).toMatchObject({
    'entity-b': {
      a: 4,
      b: 5,
      c: 6,
    },
  });
});

test('able to list entities before a specific entity', async () => {
  const client = getStorageClient();

  const created = await client.batchCreateQuery<TestEntity>({
    entries: {
      'entity-a': {
        a: 1,
        b: 2,
        c: 3,
      },
      'entity-b': {
        a: 4,
        b: 5,
        c: 6,
      },
    },
  });
  const cursor = created.unwrap()[1].id;

  const items = await client.listQuery<TestEntity>({ key: 'entity', before: cursor });
  const result = items.unwrap();

  expect(Object.keys(result)).toHaveLength(1);
  expect(result).toMatchObject({
    'entity-a': {
      a: 1,
      b: 2,
      c: 3,
    },
  });
});

test('able to list entities after a specific entity', async () => {
  const client = getStorageClient();

  const created = await client.batchCreateQuery<TestEntity>({
    entries: {
      'entity-a': {
        a: 1,
        b: 2,
        c: 3,
      },
      'entity-b': {
        a: 4,
        b: 5,
        c: 6,
      },
      'entity-c': {
        a: 7,
        b: 8,
        c: 9,
      },
    },
  });
  const cursor = created.unwrap()[0].id;

  const items = await client.listQuery<TestEntity>({
    key: 'entity',
    after: cursor,
    first: 1,
  });
  const result = items.unwrap();

  expect(Object.keys(result)).toHaveLength(1);
  expect(result).toMatchObject({
    'entity-b': {
      a: 4,
      b: 5,
      c: 6,
    },
  });
});

test('able to list entities before a specific entity', async () => {
  const client = getStorageClient();

  const created = await client.batchCreateQuery<TestEntity>({
    entries: {
      'entity-a': {
        a: 1,
        b: 2,
        c: 3,
      },
      'entity-b': {
        a: 4,
        b: 5,
        c: 6,
      },
      'entity-c': {
        a: 7,
        b: 8,
        c: 9,
      },
    },
  });
  const cursor = created.unwrap()[2].id;

  const items = await client.listQuery<TestEntity>({
    key: 'entity',
    before: cursor,
    last: 1,
  });
  const result = items.unwrap();

  expect(Object.keys(result)).toHaveLength(1);
  expect(result).toMatchObject({
    'entity-b': {
      a: 4,
      b: 5,
      c: 6,
    },
  });
});

test('able to perform a ranged list query', async () => {
  const client = getStorageClient();

  const created = await client.batchCreateQuery<TestEntity>({
    entries: {
      'entity-a': {
        a: 1,
        b: 2,
        c: 3,
      },
      'entity-b': {
        a: 4,
        b: 5,
        c: 6,
      },
      'entity-c': {
        a: 7,
        b: 8,
        c: 9,
      },
    },
  });

  const items = await client.listQuery<TestEntity>({
    key: 'entity',
    query: { type: 'range', min: 5, max: 8, property: 'b' },
  });
  const result = items.unwrap();

  expect(Object.keys(result)).toEqual(['entity-b', 'entity-c']);
});
