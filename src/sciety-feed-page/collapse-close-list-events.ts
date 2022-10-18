import { pipe } from 'fp-ts/function';
import { FeedItem, isCollapsedArticlesAddedToList } from './feed-item';
import {
  ArticleAddedToListEvent, DomainEvent, isArticleAddedToListEvent,
} from '../domain-events';

const collapsesIntoPreviousEvent = (
  state: ReadonlyArray<FeedItem>, event: ArticleAddedToListEvent,
) => state.length && pipe(
  state[state.length - 1],
  (entry) => {
    if (
      isArticleAddedToListEvent(entry)
      || isCollapsedArticlesAddedToList(entry)
    ) {
      return entry.listId === event.listId;
    }
    return false;
  },
);

const replaceWithCollapseEvent = (
  entriesSoFar: Array<FeedItem>,
  event: ArticleAddedToListEvent,
) => {
  const mostRecentEntry = entriesSoFar.pop();
  if (!mostRecentEntry) { return; }
  if (isArticleAddedToListEvent(mostRecentEntry)) {
    entriesSoFar.push({
      type: 'CollapsedArticlesAddedToList',
      listId: event.listId,
      date: event.date,
      articleCount: 2,
    });
  } else if (isCollapsedArticlesAddedToList(mostRecentEntry)) {
    entriesSoFar.push({
      type: 'CollapsedArticlesAddedToList',
      listId: event.listId,
      date: event.date,
      articleCount: mostRecentEntry.articleCount + 1,
    });
  }
};

const processEvent = (
  state: Array<FeedItem>, event: DomainEvent,
) => {
  if (isArticleAddedToListEvent(event)) {
    if (collapsesIntoPreviousEvent(state, event)) {
      replaceWithCollapseEvent(state, event);
    } else {
      state.push(event);
    }
  } else {
    state.push(event);
  }
  return state;
};

export const collapseCloseListEvents = (
  events: ReadonlyArray<DomainEvent>,
): ReadonlyArray<FeedItem> => events.reduce(processEvent, []);
