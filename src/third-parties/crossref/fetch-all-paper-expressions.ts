import { URL } from 'url';
import { formatValidationErrors } from 'io-ts-reporters';
import * as t from 'io-ts';
import * as RA from 'fp-ts/ReadonlyArray';
import * as S from 'fp-ts/string';
import * as RNEA from 'fp-ts/ReadonlyNonEmptyArray';
import * as TE from 'fp-ts/TaskEither';
import * as E from 'fp-ts/Either';
import * as TO from 'fp-ts/TaskOption';
import { pipe } from 'fp-ts/function';
import * as EDOI from '../../types/expression-doi';
import { PaperExpression } from '../../types/paper-expression';
import { QueryExternalService } from '../query-external-service';
import * as DE from '../../types/data-error';
import { Logger } from '../../shared-ports';

const crossrefWorkCodec = t.strict({
  DOI: t.string,
  posted: t.strict({
    'date-parts': t.readonlyArray(t.tuple([t.number, t.number, t.number])),
  }),
  resource: t.strict({
    primary: t.strict({
      URL: t.string,
    }),
  }),
  relation: t.partial({
    'has-version': t.array(t.strict({
      'id-type': t.literal('doi'),
      id: t.string,
    })),
    'is-version-of': t.array(t.strict({
      'id-type': t.literal('doi'),
      id: t.string,
    })),
  }),
});

export type CrossrefWork = t.TypeOf<typeof crossrefWorkCodec>;

const crossrefIndividualWorkResponseCodec = t.strict({
  message: crossrefWorkCodec,
});

type QueryCrossrefService = ReturnType<QueryExternalService>;

const fetchIndividualWork = (
  queryCrossrefService: QueryCrossrefService,
  logger: Logger,
) => (
  doi: string,
): TE.TaskEither<DE.DataError | t.Errors, CrossrefWork> => pipe(
  `https://api.crossref.org/works/${doi}`,
  queryCrossrefService,
  TE.chainEitherKW((response) => pipe(
    response,
    crossrefIndividualWorkResponseCodec.decode,
    E.mapLeft((errors) => {
      logger('error', 'fetchIndividualWork crossref codec failed', {
        doi,
        errors: formatValidationErrors(errors),
      });
      return errors;
    }),
    E.map((decodedResponse) => decodedResponse.message),
  )),
);

const toArticleVersion = (crossrefExpression: CrossrefWork): PaperExpression => ({
  expressionDoi: EDOI.fromValidatedString(crossrefExpression.DOI),
  version: parseInt(crossrefExpression.DOI.substring(crossrefExpression.DOI.length - 1), 10),
  publishedAt: new Date(
    crossrefExpression.posted['date-parts'][0][0],
    crossrefExpression.posted['date-parts'][0][1] - 1,
    crossrefExpression.posted['date-parts'][0][2],
  ),
  publisherHtmlUrl: new URL(crossrefExpression.resource.primary.URL),
});

const extractDoisOfRelatedExpressions = (work: CrossrefWork) => [
  ...pipe(
    work.relation['is-version-of'] ?? [],
    RA.map((relation) => relation.id.toLowerCase()),
  ),
  ...pipe(
    work.relation['has-version'] ?? [],
    RA.map((relation) => relation.id.toLowerCase()),
  ),
];

type State = {
  queue: ReadonlyArray<string>,
  collectedWorks: Map<string, CrossrefWork>,
};

const fetchAllQueuedWorksAndAddToCollector = (
  queryCrossrefService: QueryCrossrefService,
  logger: Logger,
) => (state: State) => pipe(
  state.queue,
  TE.traverseArray(fetchIndividualWork(queryCrossrefService, logger)),
  TE.map((newlyFetchedWorks) => pipe(
    newlyFetchedWorks,
    RA.reduce(
      state.collectedWorks,
      (collectedWorks, newlyFetchedWork) => {
        collectedWorks.set(newlyFetchedWork.DOI.toLowerCase(), newlyFetchedWork);
        return collectedWorks;
      },
    ),
    (collectedWorks) => ({
      queue: [],
      collectedWorks,
    }),
  )),
);

const hasNotBeenCollected = (state: State) => (doi: string) => !state.collectedWorks.has(doi);

export const enqueueAllRelatedDoisNotYetCollected = (state: State): State => pipe(
  Array.from(state.collectedWorks.values()),
  RA.chain(extractDoisOfRelatedExpressions),
  RA.uniq(S.Eq),
  RA.filter(hasNotBeenCollected(state)),
  (dois) => ({
    queue: dois,
    collectedWorks: state.collectedWorks,
  }),
);

const walkRelationGraph = (
  queryCrossrefService: QueryCrossrefService,
  logger: Logger,
  doi: string,
) => (
  state: State,
): TE.TaskEither<unknown, ReadonlyArray<CrossrefWork>> => pipe(
  state,
  fetchAllQueuedWorksAndAddToCollector(queryCrossrefService, logger),
  TE.map(enqueueAllRelatedDoisNotYetCollected),
  TE.chain((s) => {
    if (s.queue.length === 0) {
      return TE.right(Array.from(s.collectedWorks.values()));
    }
    if (s.collectedWorks.size > 20) {
      logger('warn', 'Exiting recursion early due to danger of an infinite loop', {
        collectedWorksSize: s.collectedWorks.size,
        startingDoi: doi,
      });

      return TE.left(DE.unavailable);
    }
    return walkRelationGraph(queryCrossrefService, logger, doi)(s);
  }),
);

type FetchAllPaperExpressions = (queryCrossrefService: QueryCrossrefService, logger: Logger, doi: string)
=> TO.TaskOption<RNEA.ReadonlyNonEmptyArray<PaperExpression>>;

export const fetchAllPaperExpressions: FetchAllPaperExpressions = (
  queryCrossrefService,
  logger,
  doi,
) => pipe(
  {
    queue: [doi],
    collectedWorks: new Map(),
  },
  walkRelationGraph(queryCrossrefService, logger, doi),
  TO.fromTaskEither,
  TO.map(RA.map(toArticleVersion)),
  TO.chainOptionK(RNEA.fromReadonlyArray),
);
