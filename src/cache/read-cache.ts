export class InMemoryReadCache {
  private cache: Map<string, any>;

  constructor() {
    this.cache = new Map();
  }

  get<T>(key: string) {
    const result = this.cache.get(key) as T | undefined;
    return result;
  }

  set(key: string, value: any) {
    this.cache.set(key, value);
    console.log({ cache: 'set', key });
  }

  deleteAll() {
    this.cache.clear();
    console.log({ cache: 'deleteAll' });
  }
}
