import { Storage } from './storage';

export interface StorageEnvironment {
  GRAPH_STORAGE: DurableObjectNamespace<Storage>;
  GRAPH_STORAGE_BACKUP: R2Bucket;
}
