import { Result } from '@badrap/result';
import { StorageUnknownOperationError } from '../storage-errors';
import { DiagnosticStorageRequest } from '../storage-request';

export class DiagnosticsHandler {
  constructor(private state: DurableObjectState) {}

  async handle(opts: DiagnosticStorageRequest) {
    switch (opts.operation) {
      case 'log': {
        return await this.log();
      }
      case 'echo': {
        return await this.echo();
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

    return Result.ok({
      id: this.state.id,
      time: Date.now(),
      traceInfo: await trace.text(),
    });
  }

  private async echo() {
    return this.log();
  }
}
