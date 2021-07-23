import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import { pipe } from 'fp-ts/function';
import { DomainEvent, isUserSavedArticleEvent } from '../../types/domain-events';
import { UserId } from '../../types/user-id';

type UserListDetails = {
  articleCount: number,
  lastUpdated: O.Option<Date>,
};

export const getUserListDetails = (userId: UserId) => (events: ReadonlyArray<DomainEvent>): UserListDetails => pipe(
  events,
  RA.filter(isUserSavedArticleEvent),
  RA.filter((event) => event.userId === userId),
  (relevantEvents) => ({
    articleCount: relevantEvents.length,
    lastUpdated: pipe(
      relevantEvents,
      RA.last,
      O.map((latestEvent) => latestEvent.date),
    ),
  }),
);
