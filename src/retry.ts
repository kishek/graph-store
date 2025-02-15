export type RetryOpts<T> = {
  maxRetries?: number;
  delay?: number;
  factor?: number;
  shouldRetryOnSuccess?: (response: T) => boolean;
  shoudlRetryOnFailure?: (error: unknown | Error) => boolean;
};

export class RetryError extends Error {
  constructor(message?: string) {
    super(message);
    this.name = 'RetryError';
  }
}

export class RetryExhaustedError extends RetryError {
  constructor(message?: string) {
    super(message);
    this.name = 'RetryExhaustedError';
  }
}

// by default, we return all successful responses
const continueOnSuccess = () => false;
// by default, we retry on all failures
const retryOnFailure = () => true;

export const retry = <T>(tag: string, cb: () => Promise<T>, opts: RetryOpts<T> = {}) => {
  const {
    maxRetries = 5,
    delay = 100,
    factor = 2,
    shouldRetryOnSuccess = continueOnSuccess,
    shoudlRetryOnFailure = retryOnFailure,
  } = opts;
  let retryCount = 0;

  const callback = async (): Promise<T> => {
    try {
      const response = await cb();
      if (shouldRetryOnSuccess(response)) {
        throw new RetryError(`${tag}: retry condition met`);
      }

      return response;
    } catch (err) {
      if (retryCount >= maxRetries && shoudlRetryOnFailure(err)) {
        console.error(`${tag}: retries exhausted`, { err });
        throw new RetryExhaustedError(`${tag}: retries exhausted`);
      }

      retryCount++;
      await new Promise((resolve) => setTimeout(resolve, delay * factor ** retryCount));

      console.log(`${tag}: retrying operation`, { retryCount, err });
      return callback();
    }
  };

  return callback();
};
