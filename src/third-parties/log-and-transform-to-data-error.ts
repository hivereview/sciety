import axios, { AxiosResponse } from 'axios';
import { Logger } from '../shared-ports';
import * as DE from '../types/data-error';
import { LevelName } from '../infrastructure/logger';

const notFoundResponseStatuses = [404, 410];

const isANotFoundResponse = (
  response: AxiosResponse | undefined,
) => response?.status !== undefined && notFoundResponseStatuses.includes(response?.status);

export const logAndTransformToDataError = (logger: Logger, url: string, notFoundLogLevel: LevelName = 'warn') => (error: unknown): DE.DataError => {
  if (axios.isAxiosError(error)) {
    const logPayload = { error, responseBody: error.response?.data };
    if (isANotFoundResponse(error.response)) {
      logger(notFoundLogLevel, 'Third party data not found', logPayload);
      return DE.notFound;
    }
    logger('error', 'Request to third party failed', logPayload);
    return DE.unavailable;
  }
  logger('error', 'Unknown failure while attempting a third party request', { error, url });
  return DE.unavailable;
};
