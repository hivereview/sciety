import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import { flow } from 'fp-ts/function';
import { ArticleViewModel, renderArticleCard } from '../../shared-components/article-card';
import { paginationControls } from '../../shared-components/pagination-controls';
import { HtmlFragment, toHtmlFragment } from '../../types/html-fragment';

type RenderRecentGroupActivity = (
  nextPageHref: O.Option<string>,
) => (
  articleViewModels: ReadonlyArray<ArticleViewModel>,
) => HtmlFragment;

const renderNextLink = O.fold(
  () => '',
  paginationControls,
);

export const renderRecentGroupActivity: RenderRecentGroupActivity = (nextPageHref) => flow(
  RA.map(renderArticleCard),
  RA.map((activity) => `<li class="group-activity-list__item">${activity}</li>`),
  (renderedActivities) => `
    <div>
      <ul class="group-activity-list" role="list">${renderedActivities.join('')}</ul>
      ${renderNextLink(nextPageHref)}
    </div>
  `,
  toHtmlFragment,
);
