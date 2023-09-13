import { pipe } from 'fp-ts/function';
import * as RA from 'fp-ts/ReadonlyArray';
import { renderCountWithDescriptor } from '../render-count-with-descriptor';
import { renderLangAttribute } from '../lang-attribute';
import { ArticleCardViewModel } from './view-model';
import { HtmlFragment, toHtmlFragment } from '../../types/html-fragment';

export const renderCurationStatements = (curationStatementsTeasers: ArticleCardViewModel['curationStatementsTeasers']): HtmlFragment => {
  if (curationStatementsTeasers.length === 0) {
    return toHtmlFragment('');
  }
  return pipe(
    curationStatementsTeasers,
    RA.map((curationStatementTeaser) => `
      <li role="listitem" class="article-card-teasers__teaser">
        <article>
          <h4 class="article-card-teasers__teaser_heading">Curated by <a href="${curationStatementTeaser.groupPageHref}"><strong>${curationStatementTeaser.groupName}</strong></a></h4>
          <div ${renderLangAttribute(curationStatementTeaser.quoteLanguageCode)}class="article-card-teasers__teaser_quote">
            ${curationStatementTeaser.quote}
          </div>
        </article>
      </li>
    `),
    (listItems) => listItems.join(''),
    (listContent) => `
    <div class="visually-hidden">This article has been curated by ${renderCountWithDescriptor(curationStatementsTeasers.length, 'group', 'groups')}:</div>
    <ul class="article-card-teasers" role="list">
      ${listContent}
    </ul>
    `,
    toHtmlFragment,
  );
};
