import { DomainEvent } from '../domain-events';
import { ListId } from '../types/list-id';

export type CollapsedArticlesAddedToList = {
  type: 'CollapsedArticlesAddedToList',
  listId: ListId,
  date: Date,
  articleCount: number,
};

export const isCollapsedArticlesAddedToList = (
  entry: StateEntry,
): entry is CollapsedArticlesAddedToList => entry.type === 'CollapsedArticlesAddedToList';

export type StateEntry = DomainEvent | CollapsedArticlesAddedToList;
