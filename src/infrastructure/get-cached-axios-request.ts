import axios from 'axios';
import { RedisStore, setupCache } from 'axios-cache-adapter';
import redis from 'redis';
import { Logger } from './logger';

type GetCachedAxiosRequest = (logger: Logger, maxAge?: number)
=> <U>(url: string, headers?: Record<string, string>)
=> Promise<U>;

export const getCachedAxiosRequest: GetCachedAxiosRequest = (
  logger: Logger,
  maxAge = 24 * 60 * 60 * 1000,
) => {
  let store;
  if (process.env.APP_CACHE === 'redis') {
    const client = redis.createClient({
      host: 'sciety_cache',
    });
    store = new RedisStore(client);
  }
  const cache = setupCache({
    maxAge,
    store,
  });
  const api = axios.create({
    adapter: cache.adapter,
  });
  return async <U>(url: string, headers: Record<string, string> = {}): Promise<U> => {
    const startTime = new Date();
    const response = await api.get<U>(url, { headers });
    if (response.request.fromCache) {
      logger('debug', 'Axios cache hit', {
        url,
      });
    } else {
      logger('debug', 'Axios cache miss', {
        url,
      });
      const durationInMs = new Date().getTime() - startTime.getTime();
      logger('debug', 'Response time', { url, durationInMs, responseStatus: response.status });
    }
    return response.data;
  };
};
