export class StorageError extends Error {}

/**
 * Propagated when a query cannot find an entry for a row.
 */
export class StorageNotFoundError extends Error {
  constructor() {
    super();
    this.name = 'StorageNotFoundError';
    this.message = `row not found`;
  }
}

/**
 * Propagated when a query cannot find an entry for a row.
 */
export class StorageDeleteFailedError extends Error {
  constructor() {
    super();
    this.name = 'StorageDeleteFailedError';
    this.message = `deletion of row did not succeed`;
  }
}

/**
 * Propagated when an unexpected error occurs.
 */
export class StorageUnexpectedError extends Error {
  constructor(reason?: string) {
    super();
    this.name = 'StorageUnexpectedError';
    this.message = `unexpected error: ${reason}`;
  }
}

/**
 * Propagated when an unknown storage operation is requested.
 */
export class StorageUnknownOperationError extends Error {
  constructor(operation?: string) {
    super();
    this.name = 'StorageUnknownOperationError';
    this.message = `unknown operation: ${operation}`;
  }
}

/**
 * Propagated when a storage operation is triggered with incorrect parameters.
 */
export class StorageBadRequestError extends Error {
  constructor(reason?: string) {
    super();
    this.name = 'StorageBadRequestError';
    this.message = `bad request: ${reason}`;
  }
}
