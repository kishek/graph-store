export class InMemoryReadCache {
  private cache: Map<string, any>;

  constructor() {
    this.cache = new Map();
  }

  get<T>(key: string) {
    return this.cache.get(key) as T;
  }

  set(key: string, value: any) {
    this.cache.set(key, value);
  }

  deleteAll() {
    this.cache.clear();
  }
}
