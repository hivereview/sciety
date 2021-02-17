import * as RA from 'fp-ts/ReadonlyArray';
import * as T from 'fp-ts/Task';
import { constVoid, pipe } from 'fp-ts/function';
import { Pool } from 'pg';
import { Logger } from './logger';
import { Doi } from '../types/doi';
import { DomainEvent, RuntimeGeneratedEvent } from '../types/domain-events';
import { EditorialCommunityId } from '../types/editorial-community-id';
import { HypothesisAnnotationId } from '../types/hypothesis-annotation-id';
import * as NcrcId from '../types/ncrc-id';
import * as ReviewId from '../types/review-id';

// TODO: should return a TaskEither
export type CommitEvents = (event: ReadonlyArray<RuntimeGeneratedEvent>) => T.Task<void>;

const replacer = (key: string, value: unknown): unknown => {
  if (['date', 'id', 'type'].includes(key)) {
    return undefined;
  }

  if (value instanceof EditorialCommunityId) {
    return value.value;
  }

  if (value instanceof HypothesisAnnotationId || value instanceof Doi || NcrcId.isNrcId(value)) {
    return ReviewId.toString(value);
  }

  return value;
};

// TODO: should be all RuntimeGeneratedEvents
const persistedEventsWhiteList: ReadonlyArray<RuntimeGeneratedEvent['type']> = [
  'UserFollowedEditorialCommunity',
  'UserUnfollowedEditorialCommunity',
  'UserFoundReviewHelpful',
  'UserFoundReviewNotHelpful',
  'UserRevokedFindingReviewHelpful',
  'UserRevokedFindingReviewNotHelpful',
  'UserSavedArticle',
];

export const createCommitEvents = (
  inMemoryEvents: Array<DomainEvent>,
  pool: Pool,
  logger: Logger,
): CommitEvents => (
  (events) => pipe(
    events,
    RA.map((event) => async () => {
      if (persistedEventsWhiteList.includes(event.type)) {
        await pool.query(
          'INSERT INTO events (id, type, date, payload) VALUES ($1, $2, $3, $4);',
          [event.id, event.type, event.date, JSON.stringify(event, replacer)],
        );
      }

      inMemoryEvents.push(event);

      logger('info', 'Event committed', { event });
    }),
    T.sequenceArray,
    T.map(constVoid),
  )
);
