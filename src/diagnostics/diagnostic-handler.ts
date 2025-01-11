import { Result } from '@badrap/result';
import { StorageUnknownOperationError } from '../storage-errors';
import { RequestInfo } from '../storage-request';

export class DiagnosticsHandler {
  constructor(private state: DurableObjectState) {}

  async handle(info: RequestInfo) {
    switch (info.operation) {
      case 'log': {
        return await this.log();
      }
      default: {
        return Result.err(new StorageUnknownOperationError());
      }
    }
  }

  private async log() {
    const trace = await fetch('https://cloudflare.com/cdn-cgi/trace', {
      method: 'GET',
    });

    return {
      id: this.state.id,
      time: Date.now(),
      traceInfo: await trace.text(),
    };
  }
}
