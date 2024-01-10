import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { getArticleVersionEventsFromBiorxiv } from './biorxiv';
import { fetchAllPaperExpressions as fetchAllPaperExpressionsFromCrossref } from './crossref';
import { QueryExternalService } from './query-external-service';
import { Logger } from '../shared-ports';
import { ArticleId } from '../types/article-id';
import { ExternalQueries } from './external-queries';

export const findAllExpressionsOfPaper = (
  queryCrossrefService: QueryExternalService,
  queryExternalService: QueryExternalService,
  crossrefApiBearerToken: O.Option<string>,
  logger: Logger,
): ExternalQueries['findAllExpressionsOfPaper'] => (expressionDoi, server) => {
  const headers: Record<string, string> = { };
  if (O.isSome(crossrefApiBearerToken)) {
    headers['Crossref-Plus-API-Token'] = `Bearer ${crossrefApiBearerToken.value}`;
  }
  return pipe(
    fetchAllPaperExpressionsFromCrossref(
      queryCrossrefService(undefined, headers),
      logger,
      expressionDoi,
    ),
    TE.chain((expressionsFromCrossref) => pipe(
      (server === 'biorxiv' || server === 'medrxiv')
        ? pipe(
          getArticleVersionEventsFromBiorxiv({ queryExternalService, logger })(
            new ArticleId(expressionDoi),
            server,
          ),
          TE.map((expressionsFromBiorxiv) => [
            expressionsFromBiorxiv,
            pipe(
              expressionsFromCrossref,
              RA.filter((paperExpression) => paperExpression.expressionDoi !== expressionDoi),
            ),
          ]),
          TE.map(RA.flatten),
        )
        : TE.right(expressionsFromCrossref),
    )),
  );
};
