import * as O from 'fp-ts/Option';
import * as RM from 'fp-ts/ReadonlyMap';
import { pipe } from 'fp-ts/function';
import * as S from 'fp-ts/string';
import { constructListsReadModel } from './construct-lists-read-model';
import { createListFromEvaluationEvents } from './create-list-from-evaluation-events';
import { DomainEvent } from '../../domain-events';
import { GroupId } from '../../types/group-id';

type List = {
  name: string,
  description: string,
  articleCount: number,
  lastUpdated: O.Option<Date>,
  ownerId: GroupId,
};

export const selectAllListsOwnedBy = (groupId: GroupId) => (events: ReadonlyArray<DomainEvent>): List => pipe(
  events,
  constructListsReadModel,
  RM.lookup(S.Eq)(groupId),
  O.getOrElse(
    () => createListFromEvaluationEvents(groupId, []),
  ),
);
