import { Json } from 'fp-ts/Json';
import * as O from 'fp-ts/Option';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { identity, pipe } from 'fp-ts/function';
import { Pool } from 'pg';
import * as RA from 'fp-ts/ReadonlyArray';
import * as TO from 'fp-ts/TaskOption';
import { ArticleServer } from '../types/article-server';
import { CollectedPorts } from './collected-ports';
import { commitEvents } from './commit-events';
import { dispatcher } from '../shared-read-models';
import { fetchNcrcReview } from '../third-parties/ncrc/fetch-ncrc-review';
import { fetchRapidReview } from '../third-parties/rapid-reviews/fetch-rapid-review';
import { fetchReview } from '../third-parties/fetch-review';
import { fetchHypothesisAnnotation } from './fetch-hypothesis-annotation';
import { fetchStaticFile } from './fetch-static-file';
import { fetchZenodoRecord } from '../third-parties/zenodo/fetch-zenodo-record';
import { fetchData } from './fetchers';
import { getCachedAxiosRequest } from './get-cached-axios-request';
import { getEventsFromDatabase } from './get-events-from-database';
import { getHtml } from './get-html';
import {
  createLogger, Logger, Config as LoggerConfig,
} from './logger';
import { stubAdapters } from './stub-adapters';
import { addArticleToListCommandHandler } from '../write-side/add-article-to-list';
import {
  DomainEvent, sort as sortEvents,
} from '../domain-events';
import { editListDetailsCommandHandler, createListCommandHandler, removeArticleFromListCommandHandler } from '../write-side/command-handlers';
import { executePolicies } from '../policies/execute-policies';
import { recordSubjectAreaCommandHandler } from '../write-side/record-subject-area';
import { getArticleVersionEventsFromBiorxiv } from '../third-parties/biorxiv';
import { getBiorxivOrMedrxivCategory } from '../third-parties/biorxiv/get-biorxiv-or-medrxiv-category';
import { fetchCrossrefArticle } from '../third-parties/crossref';
import { searchEuropePmc } from '../third-parties/europe-pmc';
import { fetchPrelightsHighlight } from '../third-parties/prelights';
import { fetchRecommendedPapers } from '../third-parties/semantic-scholar/fetch-recommended-papers';
import { Doi } from '../types/doi';
import { queryExternalService } from '../third-parties/query-external-service';

type Dependencies = LoggerConfig & {
  crossrefApiBearerToken: O.Option<string>,
};

type DatabaseConnectionPoolAndLogger = { pool: Pool, logger: Logger };

const createEventsTable = ({ pool }: DatabaseConnectionPoolAndLogger) => TE.tryCatch(
  async () => pool.query(`
      CREATE TABLE IF NOT EXISTS events (
        id uuid,
        type varchar,
        date timestamp,
        payload jsonb,
        PRIMARY KEY (id)
      );
    `),
  identity,
);

const createGetJson = (logger: Logger) => async (uri: string) => {
  const response = await fetchData(logger)<Json>(uri);
  return response.data;
};

const findVersionsForArticleDoiFromSupportedServers = (logger: Logger) => (doi: Doi, server: ArticleServer) => {
  if (server === 'biorxiv' || server === 'medrxiv') {
    return getArticleVersionEventsFromBiorxiv({
      queryExternalService: queryExternalService(logger, 24 * 60 * 60),
      logger,
    })(doi, server);
  }
  return TO.none;
};

export const createInfrastructure = (dependencies: Dependencies): TE.TaskEither<unknown, CollectedPorts> => pipe(
  {
    pool: new Pool(),
    logger: createLogger(dependencies),
  },
  TE.right,
  TE.chainFirst(createEventsTable),
  TE.chainW(({ pool, logger }) => pipe(
    getEventsFromDatabase(pool, logger),
    TE.map(RA.toArray),
    TE.map(sortEvents),
    TE.map((events) => (
      {
        events,
        pool,
        logger,
        getJson: createGetJson(logger),
      }
    )),
  )),
  TE.map((lowLevelAdapters) => ({
    ...lowLevelAdapters,
    getArticleSubjectArea: getBiorxivOrMedrxivCategory({
      queryExternalService: queryExternalService(lowLevelAdapters.logger, 24 * 60 * 60),
      logger: lowLevelAdapters.logger,
    }),
  })),
  TE.chain((partialAdapters) => TE.tryCatch(
    async () => {
      const {
        events, logger, pool, getJson,
      } = partialAdapters;

      const getAllEvents = T.of(events);
      const fetchers = {
        doi: fetchZenodoRecord(getJson, logger),
        hypothesis: fetchHypothesisAnnotation(queryExternalService(logger), logger),
        ncrc: fetchNcrcReview(logger),
        prelights: fetchPrelightsHighlight(logger, getHtml),
        rapidreviews: fetchRapidReview(logger, getHtml),
      };

      const {
        dispatchToAllReadModels,
        queries,
      } = dispatcher();

      dispatchToAllReadModels(events);

      const commitEventsWithoutListeners = commitEvents({
        inMemoryEvents: events,
        dispatchToAllReadModels,
        pool,
        logger,
      });

      const commandHandlerAdapters = {
        getAllEvents,
        commitEvents: commitEventsWithoutListeners,
      };

      const collectedAdapters = {
        ...queries,
        fetchArticle: fetchCrossrefArticle(
          getCachedAxiosRequest(logger),
          logger,
          dependencies.crossrefApiBearerToken,
        ),
        fetchRelatedArticles: fetchRecommendedPapers({ queryExternalService: queryExternalService(logger), logger }),
        fetchReview: fetchReview(fetchers),
        fetchStaticFile: fetchStaticFile(logger),
        searchForArticles: searchEuropePmc({
          queryExternalService: queryExternalService(logger, 5 * 60, 'error'),
          logger,
        }),
        getAllEvents,
        findVersionsForArticleDoi: findVersionsForArticleDoiFromSupportedServers(logger),
        recordSubjectArea: recordSubjectAreaCommandHandler(commandHandlerAdapters),
        editListDetails: editListDetailsCommandHandler(commandHandlerAdapters),
        createList: createListCommandHandler(commandHandlerAdapters),
        addArticleToList: addArticleToListCommandHandler(commandHandlerAdapters),
        removeArticleFromList: removeArticleFromListCommandHandler(commandHandlerAdapters),
        ...partialAdapters,
      };

      const policiesAdapters = {
        ...queries,
        commitEvents: commitEventsWithoutListeners,
        getAllEvents: collectedAdapters.getAllEvents,
        logger: collectedAdapters.logger,
        getArticleSubjectArea: collectedAdapters.getArticleSubjectArea,
        addArticleToList: collectedAdapters.addArticleToList,
        removeArticleFromList: collectedAdapters.removeArticleFromList,
        createList: collectedAdapters.createList,
      };

      const allAdapters = {
        ...collectedAdapters,
        commitEvents: (eventsToCommit: ReadonlyArray<DomainEvent>) => pipe(
          eventsToCommit,
          commitEventsWithoutListeners,
          T.chainFirst(() => pipe(
            eventsToCommit,
            T.traverseArray(executePolicies(policiesAdapters)),
          )),
        ),
      };

      if (process.env.USE_STUB_ADAPTERS === 'true') {
        return {
          ...allAdapters,
          ...stubAdapters,
        };
      }
      return allAdapters;
    },
    identity,
  )),
);
