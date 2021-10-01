import axios from 'axios';
import { setupCache } from 'axios-cache-adapter';
import { sequenceS } from 'fp-ts/Apply';
import * as A from 'fp-ts/Array';
import { Json } from 'fp-ts/Json';
import * as O from 'fp-ts/Option';
import * as RNEA from 'fp-ts/ReadonlyNonEmptyArray';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { identity, pipe } from 'fp-ts/function';
import { Pool } from 'pg';
import { Adapters } from './adapters'
import { commitEvents } from './commit-events';
import { createEventSourceFollowListRepository } from './event-sourced-follow-list-repository';
import { fetchCrossrefArticle } from './fetch-crossref-article';
import { fetchDataciteReview } from './fetch-datacite-review';
import { fetchDataset } from './fetch-dataset';
import { fetchHypothesisAnnotation } from './fetch-hypothesis-annotation';
import { fetchNcrcReview } from './fetch-ncrc-review';
import { fetchPrelightsHighlight } from './fetch-prelights-highlight';
import { fetchRapidReview } from './fetch-rapid-review';
import { fetchReview } from './fetch-review';
import { fetchStaticFile } from './fetch-static-file';
import { fetchData } from './fetchers';
import { findGroups } from './find-groups';
import { findReviewsForArticleDoi } from './find-reviews-for-article-doi';
import { follows } from './follows';
import { getArticleVersionEventsFromBiorxiv } from './get-article-version-events-from-biorxiv';
import { getEventsFromDataFiles } from './get-events-from-data-files';
import { getEventsFromDatabase } from './get-events-from-database';
import { getHtml } from './get-html';
import { getTwitterResponse } from './get-twitter-response';
import { getTwitterUserDetails } from './get-twitter-user-details';
import { getTwitterUserDetailsBatch } from './get-twitter-user-details-batch';
import { getTwitterUserId } from './get-twitter-user-id';
import { getXmlFromCrossrefRestApi } from './get-xml-from-crossref-rest-api';
import { inMemoryGroupRepository } from './in-memory-groups';
import {
  jsonSerializer, loggerIO, rTracerLogger, streamLogger,
} from './logger';
import { responseCache } from './response-cache';
import { searchEuropePmc } from './search-europe-pmc';
import { bootstrapGroups } from '../data/bootstrap-groups';
import * as DomainEvent from '../domain-events';

type Dependencies = {
  prettyLog: boolean,
  logLevel: string, // TODO: Make this a level name
  crossrefApiBearerToken: O.Option<string>,
  twitterApiBearerToken: string,
};

export const createInfrastructure = (dependencies: Dependencies): TE.TaskEither<unknown, Adapters> => pipe(
  {
    logger: pipe(
      dependencies.prettyLog,
      jsonSerializer,
      (serializer) => streamLogger(process.stdout, serializer, dependencies.logLevel),
      rTracerLogger,
    ),
    pool: new Pool(),
  },
  TE.right,
  TE.chainFirst(({ pool }) => TE.tryCatch(
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
  )),
  TE.chain(({ pool, logger }) => pipe(
    {
      eventsFromDatabase: getEventsFromDatabase(pool, loggerIO(logger)),
      eventsFromDataFiles: pipe(
        bootstrapGroups,
        RNEA.map(({ id }) => id),
        getEventsFromDataFiles,
      ),
    },
    sequenceS(TE.ApplyPar),
    TE.map(({ eventsFromDataFiles, eventsFromDatabase }) => (
      {
        events: pipe(
          eventsFromDataFiles.concat(eventsFromDatabase),
          A.sort(DomainEvent.byDate),
        ),
        pool,
        logger,
      }
    )),
  )),
  TE.chain((adapters) => TE.tryCatch(
    async () => {
      const { events, logger, pool } = adapters;

      const getJson = async (uri: string) => {
        const response = await fetchData(logger)<Json>(uri);
        return response.data;
      };

      const cache = setupCache({
        maxAge: 24 * 60 * 60 * 1000,
      });
      const api = axios.create({
        adapter: cache.adapter,
      });

      const getCachedJson = async (url: string) => {
        const headers = {
          'User-Agent': 'Sciety (http://sciety.org; mailto:team@sciety.org)',
        };
        const response = await api.get<Json>(url, { headers });
        if (response.request.fromCache) {
          logger('debug', 'Axios cache hit', {
            url,
          });
        } else {
          logger('debug', 'Axios cache miss', {
            url,
          });
        }
        return response.data;
      };

      const groups = inMemoryGroupRepository(bootstrapGroups);
      const getAllEvents = T.of(events);
      const getFollowList = createEventSourceFollowListRepository(getAllEvents);
      const fetchFile = fetchStaticFile(loggerIO(logger));
      const fetchers = {
        doi: fetchDataciteReview(fetchDataset(logger), logger),
        hypothesis: fetchHypothesisAnnotation(getJson, logger),
        ncrc: fetchNcrcReview(logger),
        prelights: fetchPrelightsHighlight(getHtml(logger)),
        rapidreviews: fetchRapidReview(logger, getHtml(logger)),
      };

      return {
        fetchArticle: fetchCrossrefArticle(responseCache(getXmlFromCrossrefRestApi(
          logger,
          dependencies.crossrefApiBearerToken,
        ), logger), logger),
        fetchReview: fetchReview(fetchers),
        fetchStaticFile: fetchFile,
        findGroups: findGroups(fetchFile, bootstrapGroups),
        searchEuropePmc: searchEuropePmc({ getJson, logger }),
        getGroup: groups.lookup,
        getGroupBySlug: groups.lookupBySlug,
        getAllGroups: groups.all,
        findReviewsForArticleDoi: findReviewsForArticleDoi(getAllEvents),
        getAllEvents,
        commitEvents: commitEvents({ inMemoryEvents: events, pool, logger: loggerIO(logger) }),
        getFollowList,
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
        follows: follows(getAllEvents),
        findVersionsForArticleDoi: getArticleVersionEventsFromBiorxiv({
          getJson: getCachedJson,
          logger: loggerIO(logger),
        }),
        ...adapters,
      };
    },
    identity,
  )),
);
