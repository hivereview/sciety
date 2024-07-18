import * as A from 'fp-ts/Array';
import * as E from 'fp-ts/Either';
import * as RA from 'fp-ts/ReadonlyArray';
import * as B from 'fp-ts/boolean';
import { flow, pipe } from 'fp-ts/function';
import { doesListExist } from './does-list-exist';
import {
  constructEvent,
  isEventOfType,
  DomainEvent,
  filterByName,
} from '../../../domain-events';
import { ArticleId, eqArticleId } from '../../../types/article-id';
import { ErrorMessage, toErrorMessage } from '../../../types/error-message';
import { ListId } from '../../../types/list-id';
import { RemoveArticleFromListCommand } from '../../commands';
import { ResourceAction } from '../resource-action';

type ListWriteModel = {
  articles: Array<{ articleId: ArticleId }>,
};

type RelevantEvent = ReturnType<typeof filterToEventsRelevantToWriteModel>[number];

const filterToEventsRelevantToWriteModel = filterByName(['ListCreated', 'ArticleAddedToList', 'ArticleRemovedFromList']);

const isAnEventOfThisList = (listId: ListId) => (event: RelevantEvent) => event.listId === listId;

const updateListWriteModel = (resource: E.Either<ErrorMessage, ListWriteModel>, event: DomainEvent) => {
  if (isEventOfType('ListCreated')(event)) {
    return E.right({ articles: [] } satisfies ListWriteModel);
  }
  if (isEventOfType('ArticleAddedToList')(event)) {
    pipe(
      resource,
      E.map((listResource) => {
        listResource.articles.push({ articleId: event.articleId } satisfies ListWriteModel['articles'][number]);
        return undefined;
      }),
    );
  }
  if (isEventOfType('ArticleRemovedFromList')(event)) {
    return pipe(
      resource,
      E.map((listResource) => pipe(
        listResource.articles,
        A.filter((article) => !eqArticleId.equals(article.articleId, event.articleId)),
        (ids) => ({ ...listResource, articles: ids } satisfies ListWriteModel),
      )),
    );
  }
  return resource;
};

const createAppropriateEvents = (command: RemoveArticleFromListCommand) => (listResource: ListWriteModel) => pipe(
  listResource.articles,
  RA.some((article) => article.articleId.value === command.articleId),
  B.fold(
    () => [],
    () => [constructEvent('ArticleRemovedFromList')({
      articleId: new ArticleId(command.articleId),
      listId: command.listId,
    })],
  ),
);

export const removeArticle: ResourceAction<RemoveArticleFromListCommand> = (command) => (events) => pipe(
  events,
  E.right,
  E.filterOrElse(
    doesListExist(command.listId),
    () => toErrorMessage('list-not-found'),
  ),
  E.chain(flow(
    filterToEventsRelevantToWriteModel,
    RA.filter(isAnEventOfThisList(command.listId)),
    RA.reduce(E.left(toErrorMessage('list-not-found')), updateListWriteModel),
  )),
  E.map(createAppropriateEvents(command)),
);
