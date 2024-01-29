import { URL } from 'url';
import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import * as TE from 'fp-ts/TaskEither';
import { flow, pipe } from 'fp-ts/function';
import * as DE from '../../types/data-error';
import * as EDOI from '../../types/expression-doi';
import { ColdSpringHarborServer } from '../../types/cold-spring-harbor-server';
import { ResponseWithVersions } from './biorxiv-details-api-response';
import { fetchArticleDetails } from './fetch-article-details';
import { ArticleId } from '../../types/article-id';
import { PaperExpression } from '../../types/paper-expression';
import { Logger } from '../../shared-ports';
import { QueryExternalService } from '../query-external-service';

type Dependencies = {
  queryExternalService: QueryExternalService,
  logger: Logger,
};

type GetArticleVersionEventsFromBiorxiv = (
  doi: EDOI.ExpressionDoi,
  server: ColdSpringHarborServer,
) => TE.TaskEither<DE.DataError, ReadonlyArray<PaperExpression>>;

const mapResponse = (expressionsDoi: EDOI.ExpressionDoi, expressionsServer: ColdSpringHarborServer) => flow(
  (response: ResponseWithVersions) => response.collection,
  RA.map(({ version, date }) => ({
    expressionType: 'preprint' as const,
    expressionDoi: expressionsDoi,
    publisherHtmlUrl: new URL(`https://www.${expressionsServer}.org/content/${expressionsDoi}v${version}`),
    publishedAt: date,
    server: O.some(expressionsServer),
  })),
);

export const getArticleVersionEventsFromBiorxiv = (
  deps: Dependencies,
): GetArticleVersionEventsFromBiorxiv => (doi, server) => pipe(
  fetchArticleDetails(deps, new ArticleId(doi), server),
  TE.map(mapResponse(doi, server)),
);
