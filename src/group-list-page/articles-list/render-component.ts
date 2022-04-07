import * as E from 'fp-ts/Either';
import * as RA from 'fp-ts/ReadonlyArray';
import { flow } from 'fp-ts/function';
import { ArticleErrorCardViewModel, renderArticleErrorCard } from './render-article-error-card';
import { ArticleViewModel, renderArticleCard } from '../../shared-components/article-card';
import { HtmlFragment, toHtmlFragment } from '../../types/html-fragment';

type RenderArticlesList = (
  articleViewModels: ReadonlyArray<E.Either<ArticleErrorCardViewModel, ArticleViewModel>>,
) => HtmlFragment;

export const renderComponent: RenderArticlesList = flow(
  RA.map(E.fold(
    renderArticleErrorCard,
    renderArticleCard,
  )),
  RA.map((activity) => `<li class="articles-list__item">${activity}</li>`),
  (renderedActivities) => `
      <ul class="articles-list" role="list">${renderedActivities.join('')}</ul>
  `,
  toHtmlFragment,
);
