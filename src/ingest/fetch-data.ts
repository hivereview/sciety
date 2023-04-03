import { performance } from 'perf_hooks';
import axios from 'axios';
import axiosRetry from 'axios-retry';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';

axiosRetry(axios, {
  retries: 3,
  onRetry: (retryCount: number, error, requestConfig) => {
    // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
    process.stdout.write(`Retrying retryCount: ${retryCount}, error: ${error}, requestConfig: ${requestConfig}\n`);
  },
});

export const axiosGet = async <D>(url: string, additionalHeaders: Record<string, string>) => {
  const startTime = performance.now();
  const headers = {
    'User-Agent': 'Sciety (http://sciety.org; mailto:team@sciety.org)',
    ...additionalHeaders,
  };
  return axios.get<D>(url, { headers }).finally(() => {
    if (process.env.INGEST_DEBUG && process.env.INGEST_DEBUG.length > 0) {
      const endTime = performance.now();
      process.stdout.write(`Fetched ${url} (${Math.round(endTime - startTime)}ms)\n`);
    }
  });
};

export type FetchData = <D>(url: string, headers?: Record<string, string>) => TE.TaskEither<string, D>;

export const fetchData: FetchData = <D>(url: string, headers = {}): TE.TaskEither<string, D> => pipe(
  TE.tryCatch(
    async () => axiosGet<D>(url, headers),
    String,
  ),
  TE.map((response) => response.data),
);
