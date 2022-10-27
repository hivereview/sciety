import { Json } from 'fp-ts/Json';
import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { identity, pipe } from 'fp-ts/function';
import { Pool } from 'pg';
import { CollectedPorts } from './collected-ports';
import { commitEvents, writeEventToDatabase } from './commit-events';
import { fetchHypothesisAnnotation } from './fetch-hypothesis-annotation';
import { fetchNcrcReview } from './fetch-ncrc-review';
import { fetchRapidReview } from './fetch-rapid-review';
import { fetchReview } from './fetch-review';
import { fetchStaticFile } from './fetch-static-file';
import { fetchZenodoRecord } from './fetch-zenodo-record';
import { fetchData } from './fetchers';
import { getCachedAxiosRequest } from './get-cached-axios-request';
import { getEventsFromDatabase } from './get-events-from-database';
import { getHtml } from './get-html';
import { getListsOwnedByFromListsReadModelService } from './get-lists-owned-by-from-lists-read-model-service';
import {
  jsonSerializer, Logger, loggerIO, rTracerLogger, streamLogger,
} from './logger';
import { needsToBeAdded } from './needs-to-be-added';
import { stubAdapters } from './stub-adapters';
import { addArticleToListCommandHandler } from '../add-article-to-list';
import { bootstrapGroups as groupJoinedEvents } from '../data/bootstrap-groups';
import { hardcodedListCreationEvents } from '../data/hardcoded-list-creation-events';
import {
  DomainEvent,
  isListCreatedEvent, sort as sortEvents,
} from '../domain-events';
import { createListCommandHandler } from '../lists';
import { executePolicies } from '../policies/execute-policies';
import { removeArticleFromListCommandHandler } from '../remove-article-from-list';
import { RemoveArticleFromList } from '../shared-ports';
import { getArticleVersionEventsFromBiorxiv } from '../third-parties/biorxiv';
import { getBiorxivOrMedrxivSubjectArea } from '../third-parties/biorxiv/get-biorxiv-or-medrxiv-subject-area';
import { fetchCrossrefArticle } from '../third-parties/crossref';
import { searchEuropePmc } from '../third-parties/europe-pmc';
import { fetchPrelightsHighlight } from '../third-parties/prelights';
import {
  getTwitterResponse, getTwitterUserDetails, getTwitterUserDetailsBatch, getTwitterUserId,
} from '../third-parties/twitter';
import { Doi } from '../types/doi';
import { ListId } from '../types/list-id';

type Dependencies = {
  prettyLog: boolean,
  logLevel: string, // TODO: Make this a level name
  crossrefApiBearerToken: O.Option<string>,
  twitterApiBearerToken: string,
};

const createLogger = (dependencies: Dependencies) => pipe(
  dependencies.prettyLog,
  jsonSerializer,
  (serializer) => streamLogger(process.stdout, serializer, dependencies.logLevel),
  rTracerLogger,
);

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

const createGetJsonWithTimeout = (logger: Logger, timeout: number) => async (uri: string) => {
  const response = await fetchData(logger, timeout)<Json>(uri);
  return response.data;
};

const addSpecifiedEventsFromCodeIntoDatabaseAndAppend = (
  pool: Pool,
) => (
  events: ReadonlyArray<DomainEvent>,
) => pipe(
  [],
  TE.right,
  TE.map(RA.filter(isListCreatedEvent)),
  TE.map(RA.filter(needsToBeAdded(events))),
  TE.chainFirstTaskK(T.traverseArray(writeEventToDatabase(pool))),
  TE.map((eventsToAdd) => [
    ...events,
    ...eventsToAdd,
  ]),
);

export const createInfrastructure = (dependencies: Dependencies): TE.TaskEither<unknown, CollectedPorts> => pipe(
  {
    pool: new Pool(),
    logger: createLogger(dependencies),
  },
  TE.right,
  TE.chainFirst(createEventsTable),
  TE.chainW(({ pool, logger }) => pipe(
    getEventsFromDatabase(pool, logger),
    TE.chainW(addSpecifiedEventsFromCodeIntoDatabaseAndAppend(pool)),
    TE.map((eventsFromDatabase) => pipe(
      [
        ...eventsFromDatabase,
        ...groupJoinedEvents,
        ...hardcodedListCreationEvents(),
      ],
      sortEvents,
    )),
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
    getBiorxivOrMedrxivSubjectArea: getBiorxivOrMedrxivSubjectArea({
      getJson: createGetJsonWithTimeout(lowLevelAdapters.logger, 10000),
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
        hypothesis: fetchHypothesisAnnotation(getCachedAxiosRequest(logger, 5 * 60 * 1000), logger),
        ncrc: fetchNcrcReview(logger),
        prelights: fetchPrelightsHighlight(getHtml(logger)),
        rapidreviews: fetchRapidReview(logger, getHtml(logger)),
      };

      type AddArticleToListCommandPayload = {
        articleId: Doi, listId: ListId,
      };

      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      const dispatchToAllReadModels = (es: ReadonlyArray<DomainEvent>) => undefined;

      const commitEventsWithoutListeners = commitEvents({
        inMemoryEvents: events,
        dispatchToAllReadModels,
        pool,
        logger,
      });

      type ExecuteAddArticleToListCommandInProcess = (
        payload: AddArticleToListCommandPayload
      ) => TE.TaskEither<string, void>;

      const executeRemoveArticleFromListCommandInProcess: RemoveArticleFromList = (payload) => pipe(
        {
          articleId: payload.articleId.value,
          listId: payload.listId.toString(),
        },
        removeArticleFromListCommandHandler({
          getAllEvents,
          commitEvents: commitEventsWithoutListeners,
          ...partialAdapters,
        }),
        TE.map(() => undefined),
      );

      const executeAddArticleToListCommandInProcess: ExecuteAddArticleToListCommandInProcess = (
        payload,
      ) => pipe(
        payload,
        addArticleToListCommandHandler({
          getAllEvents,
          commitEvents: commitEventsWithoutListeners,
          ...partialAdapters,
        }),
        TE.map(() => undefined),
      );

      const collectedAdapters = {
        fetchArticle: fetchCrossrefArticle(
          getCachedAxiosRequest(logger),
          logger,
          dependencies.crossrefApiBearerToken,
        ),
        fetchReview: fetchReview(fetchers),
        fetchStaticFile: fetchStaticFile(loggerIO(logger)),
        searchEuropePmc: searchEuropePmc({ getJson, logger }),
        getAllEvents,
        getListsOwnedBy: getListsOwnedByFromListsReadModelService(logger, `http://${process.env.LISTS_READ_MODEL_HOST ?? 'lists'}`),
        getUserDetails: getTwitterUserDetails(
          getTwitterResponse(dependencies.twitterApiBearerToken, logger),
          logger,
        ),
        getUserDetailsBatch: getTwitterUserDetailsBatch(
          getTwitterResponse(dependencies.twitterApiBearerToken, logger),
          logger,
        ),
        getUserId: getTwitterUserId(
          getTwitterResponse(dependencies.twitterApiBearerToken, logger),
          logger,
        ),
        findVersionsForArticleDoi: getArticleVersionEventsFromBiorxiv({
          getJson: getCachedAxiosRequest(logger),
          logger,
        }),
        addArticleToList: executeAddArticleToListCommandInProcess,
        removeArticleFromList: executeRemoveArticleFromListCommandInProcess,
        createList: createListCommandHandler({ commitEvents: commitEventsWithoutListeners }),
        ...partialAdapters,
      };

      const policiesAdapters = {
        commitEvents: commitEventsWithoutListeners,
        getAllEvents: collectedAdapters.getAllEvents,
        logger: collectedAdapters.logger,
        getBiorxivOrMedrxivSubjectArea: collectedAdapters.getBiorxivOrMedrxivSubjectArea,
        getListsOwnedBy: collectedAdapters.getListsOwnedBy,
        getUserDetails: collectedAdapters.getUserDetails,
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
