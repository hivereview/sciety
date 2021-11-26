import * as M from 'fp-ts/Map';
import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import { pipe } from 'fp-ts/function';
import { createListFromEvaluationEvents } from './create-list-from-evaluation-events';
import { List } from './list';
import { DomainEvent, GroupEvaluatedArticleEvent } from '../../domain-events';
import { ListCreatedEvent } from '../../domain-events/list-created-event';
import { GroupId } from '../../types/group-id';

type ReadModel = Map<GroupId, List>;

export const constructListsReadModel = (
  events: ReadonlyArray<DomainEvent>,
): ReadModel => pipe(
  events,
  RA.filter((event): event is ListCreatedEvent => event.type === 'ListCreated'),
  RA.map((event): List => ({
    ...event,
    articleCount: 0,
    lastUpdated: O.some(event.date),
  })),
  () => events,
  RA.filter((event): event is GroupEvaluatedArticleEvent => event.type === 'GroupEvaluatedArticle'),
  RA.reduce(
    new Map<GroupId, Array<GroupEvaluatedArticleEvent>>(),
    (accumulator, event) => {
      if (accumulator.has(event.groupId)) {
        accumulator.get(event.groupId)?.push(event);
      } else {
        accumulator.set(event.groupId, [event]);
      }
      return accumulator;
    },
  ),
  M.mapWithIndex(createListFromEvaluationEvents),
);
