import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import { flow } from 'fp-ts/function';
import { ArticleViewModel, renderArticleCard } from '../../shared-components/article-card';
import { HtmlFragment, toHtmlFragment } from '../../types/html-fragment';

type RenderEvaluatedArticlesList = (
  articleViewModels: ReadonlyArray<E.Either<unknown, ArticleViewModel>>,
) => HtmlFragment;

export const renderEvaluatedArticlesList: RenderEvaluatedArticlesList = flow(
  RA.map(E.fold(
    () => toHtmlFragment('<div class="error-card">Can\'t currently display this article.</div>'),
    renderArticleCard(O.none),
  )),
  RA.map((activity) => `<li class="evaluated-articles-list__item">${activity}</li>`),
  (renderedActivities) => `
      <ul class="evaluated-articles-list" role="list">${renderedActivities.join('')}</ul>
  `,
  toHtmlFragment,
);
