import axios from 'axios';
import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import * as R from 'fp-ts/Record';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { batchTaskTraverse } from './batch-traverse';
import * as Es from './evaluations';
import { fetchData, FetchData } from './fetch-data';
import { fetchGoogleSheet, FetchGoogleSheet } from './fetch-google-sheet';

type Adapters = {
  fetchData: FetchData,
  fetchGoogleSheet: FetchGoogleSheet,
};

export type SkippedItem = {
  item: string,
  reason: string,
};

export type FeedData = {
  evaluations: Es.Evaluations,
  skippedItems: ReadonlyArray<SkippedItem>,
};

export type FetchEvaluations = (adapters: Adapters) => TE.TaskEither<string, FeedData>;

export type GroupIngestionConfiguration = {
  id: string,
  name: string,
  fetchFeed: FetchEvaluations,
};

type LevelName = 'error' | 'warn' | 'info' | 'debug';

const report = (level: LevelName, message: string) => (payload: Record<string, unknown>) => {
  const thingToLog = {
    timestamp: new Date(),
    level,
    message,
    payload,
  };
  process.stderr.write(`${JSON.stringify(thingToLog)}\n`);
};

const reportSkippedItems = (group: GroupIngestionConfiguration) => (feedData: FeedData) => {
  if (process.env.INGEST_DEBUG && process.env.INGEST_DEBUG.length > 0) {
    pipe(
      feedData.skippedItems,
      RA.map((skippedItem) => ({ item: skippedItem.item, reason: skippedItem.reason, groupName: group.name })),
      RA.map(report('debug', 'Ingestion item skipped')),
    );
  }
  return feedData;
};

type EvaluationCommand = {
  groupId: string,
  articleId: string,
  evaluationLocator: string,
  publishedAt: Date,
  authors: ReadonlyArray<string>,
};

const send = (evaluationCommand: EvaluationCommand) => TE.tryCatch(
  async () => axios.post(`${process.env.INGESTION_TARGET_APP ?? 'http://app'}/record-evaluation`, JSON.stringify(evaluationCommand), {
    headers: {
      Authorization: `Bearer ${process.env.SCIETY_TEAM_API_BEARER_TOKEN ?? 'secret'}`,
      'Content-Type': 'application/json',
    },
    timeout: 10000,
  }),
  (error) => {
    if (axios.isAxiosError(error)) {
      return `Failed to post evaluation command: ${String(error)}. Response data is: "${String(error.response?.data)}"`;
    }
    return `Failed to post evaluation command: ${String(error)}`;
  },
);

const countUniques = (accumulator: Record<string, number>, errorMessage: string) => pipe(
  accumulator,
  R.lookup(errorMessage),
  O.match(
    () => 1,
    (count) => count + 1,
  ),
  (count) => R.upsertAt(errorMessage, count)(accumulator),
);

const ingestionCommandsBatchSize = 1;

const sendRecordEvaluationCommands = (group: GroupIngestionConfiguration) => (feedData: FeedData) => pipe(
  feedData.evaluations,
  RA.map((evaluation) => ({
    groupId: group.id,
    articleId: evaluation.articleDoi,
    evaluationLocator: evaluation.evaluationLocator,
    publishedAt: evaluation.date,
    authors: evaluation.authors,
  })),
  batchTaskTraverse(send, ingestionCommandsBatchSize),
  T.map((array) => {
    const leftsCount = RA.lefts(array).length;
    const lefts = pipe(
      array,
      RA.lefts,
      RA.reduce({}, countUniques),
    );
    const rightsCount = RA.rights(array).length;
    const summaryOfRequests = {
      groupName: group.name,
      lefts,
      leftsTotal: leftsCount,
      rightsTotal: rightsCount,
    };
    if (leftsCount > 0) {
      return E.left(summaryOfRequests);
    }
    return E.right(summaryOfRequests);
  }),
);

const updateGroup = (group: GroupIngestionConfiguration): TE.TaskEither<unknown, void> => pipe(
  group.fetchFeed({
    fetchData,
    fetchGoogleSheet,
  }),
  TE.bimap(
    (error) => ({
      groupName: group.name,
      cause: 'Could not fetch feed',
      error,
    }),
    reportSkippedItems(group),
  ),
  TE.chainW(sendRecordEvaluationCommands(group)),
  TE.bimap(
    report('error', 'Ingestion failed'),
    report('info', 'Ingestion successful'),
  ),
);

export const updateAll = (
  groups: ReadonlyArray<GroupIngestionConfiguration>,
): TE.TaskEither<unknown, ReadonlyArray<void>> => pipe(
  groups,
  TE.traverseArray(updateGroup),
);
