import * as RA from 'fp-ts/ReadonlyArray';
import { pipe } from 'fp-ts/function';
import * as E from 'fp-ts/Either';
import { toHtmlFragment } from '../../../types/html-fragment';
import { constructEvent } from '../../../domain-events';
import { AnnotateArticleInListCommand } from '../../commands';
import { ResourceAction } from '../resource-action';
import { replayListResource } from './replay-list-resource';
import { ListResource } from './list-resource';
import { ArticleId } from '../../../types/article-id';
import { toErrorMessage } from '../../../types/error-message';

const createAppropriateEvents = (command: AnnotateArticleInListCommand) => (article: ListResource['articles'][number]) => (
  article.annotated
    ? []
    : [constructEvent('ArticleInListAnnotated')({ articleId: command.articleId, listId: command.listId, content: toHtmlFragment(command.content) })]
);

const findRelevantArticle = (articleId: ArticleId) => (listResource: ListResource) => pipe(
  listResource.articles,
  RA.findFirst((article) => article.articleId.value === articleId.value),
  E.fromOption(() => toErrorMessage('Article not in list')),
);

export const annotate: ResourceAction<AnnotateArticleInListCommand> = (command) => (events) => pipe(
  events,
  replayListResource(command.listId),
  E.chain(findRelevantArticle(command.articleId)),
  E.map(createAppropriateEvents(command)),
);
