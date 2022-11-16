import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import { pipe } from 'fp-ts/function';
import {
  DomainEvent,
  isUserSavedArticleEvent,
  isUserUnsavedArticleEvent,
  UserSavedArticleEvent,
  UserUnsavedArticleEvent,
} from '../../domain-events';
import * as LID from '../../types/list-id';
import { UserId } from '../../types/user-id';

type UserListDetails = {
  listId: LID.ListId,
  articleCount: number,
  lastUpdated: O.Option<Date>,
};

const isRelevantEvent = (event: DomainEvent): event is UserSavedArticleEvent | UserUnsavedArticleEvent => (
  isUserSavedArticleEvent(event) || isUserUnsavedArticleEvent(event)
);

export const getUserListDetails = (
  userId: UserId,
  listId: LID.ListId,
) => (events: ReadonlyArray<DomainEvent>): UserListDetails => pipe(
  events,
  RA.filter(isRelevantEvent),
  RA.filter((event) => event.userId === userId),
  (relevantEvents) => ({
    listId,
    articleCount: pipe(
      relevantEvents,
      RA.reduce(0, (state, event) => (isUserSavedArticleEvent(event) ? state + 1 : state - 1)),
    ),
    lastUpdated: pipe(
      relevantEvents,
      RA.last,
      O.map((latestEvent) => latestEvent.date),
    ),
  }),
);
