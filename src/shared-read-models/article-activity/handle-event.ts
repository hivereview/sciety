/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable no-param-reassign */
import * as O from 'fp-ts/Option';
import { pipe } from 'fp-ts/function';
import { ArticleActivity } from '../../types/article-activity';
import { GroupId } from '../../types/group-id';
import { ListId } from '../../types/list-id';
import { DomainEvent } from '../../domain-events';

type ArticleState = ArticleActivity & {
  evaluatingGroups: Set<GroupId>,
  lists: Set<ListId>,
};

const mostRecentDate = (a: Date) => (b: Date) => (a.getTime() > b.getTime() ? a : b);

const deleteFromSet = (set: Set<ListId>, element: ListId) => {
  set.delete(element);
  return set;
};

export type ReadModel = Map<string, ArticleState>;

// ts-unused-exports:disable-next-line
export const initialState = (): ReadModel => new Map();

// ts-unused-exports:disable-next-line
export const handleEvent = (readmodel: ReadModel, event: DomainEvent): ReadModel => {
  switch (event.type) {
    case 'ArticleAddedToList':
      return pipe(
        readmodel.get(event.articleId.value),
        O.fromNullable,
        O.fold(
          () => readmodel.set(event.articleId.value, {
            articleId: event.articleId,
            latestActivityDate: O.none,
            evaluationCount: 0,
            evaluatingGroups: new Set(),
            lists: new Set([event.listId]),
            listMembershipCount: 1,
          }),
          (entry) => readmodel.set(event.articleId.value, {
            ...entry,
            lists: entry.lists.add(event.listId),
            listMembershipCount: entry.lists.add(event.listId).size,
          }),
        ),
      );

    case 'EvaluationRecorded':
      return pipe(
        readmodel.get(event.articleId.value),
        O.fromNullable,
        O.fold(
          () => readmodel.set(event.articleId.value, {
            articleId: event.articleId,
            latestActivityDate: O.some(event.publishedAt),
            evaluationCount: 1,
            evaluatingGroups: new Set([event.groupId]),
            lists: new Set(),
            listMembershipCount: 0,
          }),
          (entry) => readmodel.set(event.articleId.value, {
            ...entry,
            latestActivityDate: pipe(
              entry.latestActivityDate,
              O.map(mostRecentDate(event.publishedAt)),
            ),
            evaluationCount: entry.evaluationCount + 1,
            evaluatingGroups: entry.evaluatingGroups.add(event.groupId),
          }),
        ),
      );

    case 'ArticleRemovedFromList':
      return pipe(
        readmodel.get(event.articleId.value),
        O.fromNullable,
        O.fold(
          () => readmodel,
          (entry) => readmodel.set(event.articleId.value, {
            ...entry,
            lists: deleteFromSet(entry.lists, event.listId),
            listMembershipCount: deleteFromSet(entry.lists, event.listId).size,
          }),
        ),
      );

    default:
      return readmodel;
  }
};
