import { pipe } from 'fp-ts/function';
import { DomainEvent, GroupEvaluatedArticleEvent } from '../domain-events';
import { Doi } from '../types/doi';
import { GroupId } from '../types/group-id';

type CollapsedGroupEvaluatedArticle = {
  type: 'CollapsedGroupEvaluatedArticle',
  groupId: GroupId,
  articleId: Doi,
  evaluationCount: number,
  date: Date,
};

type CollapsedGroupEvaluatedMultipleArticles = {
  type: 'CollapsedGroupEvaluatedMultipleArticles',
  groupId: GroupId,
  articleCount: number,
  date: Date,
};

type StateEntry = DomainEvent | CollapsedGroupEvaluatedArticle | CollapsedGroupEvaluatedMultipleArticles;

const isCollapsedGroupEvaluatedArticle = (
  entry: StateEntry,
): entry is CollapsedGroupEvaluatedArticle => entry.type === 'CollapsedGroupEvaluatedArticle';

const isCollapsedGroupEvaluatedMultipleArticles = (
  entry: StateEntry,
): entry is CollapsedGroupEvaluatedMultipleArticles => entry.type === 'CollapsedGroupEvaluatedMultipleArticles';

const isEditorialCommunityReviewedArticleEvent = (event: StateEntry):
  event is GroupEvaluatedArticleEvent => (
  event.type === 'GroupEvaluatedArticle'
);

const collapsesIntoPreviousEvent = (
  state: ReadonlyArray<StateEntry>, event: GroupEvaluatedArticleEvent,
) => state.length && pipe(
  state[state.length - 1],
  (entry) => {
    if (isEditorialCommunityReviewedArticleEvent(entry)) {
      return entry.groupId === event.groupId;
    }
    if (isCollapsedGroupEvaluatedArticle(entry)) {
      return entry.groupId === event.groupId;
    }
    if (isCollapsedGroupEvaluatedMultipleArticles(entry)) {
      return entry.groupId === event.groupId;
    }
    return false;
  },
);

const replaceWithCollapseEvent = (
  state: ReadonlyArray<StateEntry>,
  event: GroupEvaluatedArticleEvent,
) => {
  const last = state[state.length - 1];
  const head = state.slice(0, -1);
  if (isEditorialCommunityReviewedArticleEvent(last)) {
    if (event.articleId.value === last.articleId.value) {
      return [...head, {
        type: 'CollapsedGroupEvaluatedArticle' as const,
        articleId: last.articleId,
        groupId: last.groupId,
        evaluationCount: 2,
        date: last.date,
      }];
    }
    return [...head, {
      type: 'CollapsedGroupEvaluatedMultipleArticles' as const,
      groupId: last.groupId,
      articleCount: 2,
      date: last.date,
    }];
  }

  if (isCollapsedGroupEvaluatedArticle(last)) {
    if (event.articleId.value === last.articleId.value) {
      return [...head, {
        ...last,
        evaluationCount: last.evaluationCount + 1,
      }];
    }
    return [...head, {
      type: 'CollapsedGroupEvaluatedMultipleArticles' as const,
      groupId: last.groupId,
      articleCount: 2,
      date: last.date,
    }];
  }

  if (isCollapsedGroupEvaluatedMultipleArticles(last)) {
    return [...head, {
      ...last,
      articleCount: last.articleCount + 1,
    }];
  }

  return state;
};

const processEvent = (
  state: ReadonlyArray<StateEntry>, event: DomainEvent,
) => (isEditorialCommunityReviewedArticleEvent(event)
    && collapsesIntoPreviousEvent(state, event)
  ? replaceWithCollapseEvent(state, event)
  : [...state, event]);

export const collapseCloseEvents = (
  events: ReadonlyArray<DomainEvent>,
): ReadonlyArray<StateEntry> => events.reduce(processEvent, []);
