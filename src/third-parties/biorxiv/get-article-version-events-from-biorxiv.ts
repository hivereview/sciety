import { URL } from 'url';
import * as E from 'fp-ts/Either';
import { Json } from 'fp-ts/Json';
import * as O from 'fp-ts/Option';
import * as RNEA from 'fp-ts/ReadonlyNonEmptyArray';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { flow, pipe } from 'fp-ts/function';
import * as PR from 'io-ts/PathReporter';
import { biorxivArticleDetails, BiorxivArticleDetails } from './BiorxivArticleDetails';
import { Logger } from '../../infrastructure/logger';
import { ArticleServer } from '../../types/article-server';
import { Doi } from '../../types/doi';

type GetJson = (url: string, headers: Record<string, string>) => Promise<Json>;

type Dependencies = {
  getJson: GetJson,
  logger: Logger,
};

type ArticleVersion = {
  source: URL,
  publishedAt: Date,
  version: number,
};

export type GetArticleVersionEventsFromBiorxiv = (
  doi: Doi,
  server: ArticleServer,
) => T.Task<O.Option<RNEA.ReadonlyNonEmptyArray<ArticleVersion>>>;

const headers = {
  'User-Agent': 'Sciety (http://sciety.org; mailto:team@sciety.org)',
};

type MakeRequest = (doi: Doi, server: ArticleServer)
=> ({ getJson, logger }: Dependencies)
=> TE.TaskEither<Error, BiorxivArticleDetails>;

export const makeRequest: MakeRequest = (doi, server) => ({ getJson, logger }) => pipe(
  TE.tryCatch(
    async () => getJson(
      `https://api.biorxiv.org/details/${server}/${doi.value}`,
      headers,
    ),
    E.toError,
  ),
  TE.chainEitherK(flow(
    biorxivArticleDetails.decode,
    E.mapLeft((errors) => new Error(PR.failure(errors).join('\n'))),
  )),
  TE.mapLeft(
    (error) => {
      logger('warn', 'Failed to retrieve article versions', { doi, error });
      return error;
    },
  ),
);

const mapResponse = (doi: Doi, server: ArticleServer) => flow(
  (response: BiorxivArticleDetails) => response.collection,
  RNEA.map(({ version, date }) => ({
    source: new URL(`https://www.${server}.org/content/${doi.value}v${version}`),
    publishedAt: date,
    version,
  })),
);

export const getArticleVersionEventsFromBiorxiv = (
  deps: Dependencies,
): GetArticleVersionEventsFromBiorxiv => (doi, server) => pipe(
  makeRequest(doi, server)(deps),
  TE.map(mapResponse(doi, server)),
  T.map(O.fromEither),
);
