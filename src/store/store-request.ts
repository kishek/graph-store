export type RestoreRequest = {
  backupId: string;
};

export const isRestoreRequest = (body: any): body is RestoreRequest => {
  return body && typeof body.backupId === 'string';
};
