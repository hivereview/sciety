import * as O from 'fp-ts/Option';
import { constant, flow, pipe } from 'fp-ts/function';
import * as RA from 'fp-ts/ReadonlyArray';
import { renderCountWithDescriptor } from '../render-count-with-descriptor';
import { HtmlFragment, toHtmlFragment } from '../../types/html-fragment';
import { templateDate } from '../date';
import { renderAuthors } from '../render-card-authors';
import { renderLangAttribute } from '../lang-attribute';
import { ArticleCardViewModel } from './view-model';

const wrapInSpan = (text: string) => toHtmlFragment(`<span>${text}</span>`);

const renderEvaluationCount = (evaluationCount: ArticleCardViewModel['evaluationCount']) => pipe(
  evaluationCount,
  O.fold(
    () => '<span class="visually-hidden">This article has no evaluations</span>',
    (count) => pipe(
      renderCountWithDescriptor(count, 'evaluation', 'evaluations'),
      wrapInSpan,
      (content) => `<span class="visually-hidden">This article has </span>${content}`,
    ),
  ),
);

const renderListMembershipCount = (listMembershipCount: ArticleCardViewModel['listMembershipCount']) => pipe(
  listMembershipCount,
  O.fold(
    constant(''),
    (count) => pipe(
      `Appears in ${renderCountWithDescriptor(count, 'list', 'lists')}`,
      wrapInSpan,
    ),
  ),
);

const renderArticleVersionDate = O.fold(
  constant(''),
  flow(
    templateDate,
    (date) => `Latest version ${date}`,
    wrapInSpan,
  ),
);

const renderArticleLatestActivityDate = O.fold(
  constant(''),
  flow(
    templateDate,
    (date) => `Latest activity ${date}`,
    wrapInSpan,
  ),
);

const renderCurationStatements = (curationStatementsTeasers: ArticleCardViewModel['curationStatementsTeasers']) => {
  if (curationStatementsTeasers.length === 0) {
    return '';
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
  );
};

const renderReviewingGroupsWithLink = (reviewingGroups: ArticleCardViewModel['reviewingGroups']) => {
  if (reviewingGroups.length === 0) {
    return '';
  }
  return pipe(
    reviewingGroups,
    RA.map((group) => `<a href="${group.groupPageHref}">${group.groupName}</a>`),
    (links) => links.join(', '),
    (groupNamesWithLinks) => `<p class="article-card__reviewing_groups">Reviewed by ${groupNamesWithLinks}</p>`,
  );
};

export const renderArticleCardContents = (model: ArticleCardViewModel): HtmlFragment => toHtmlFragment(`
  <h3 class="article-card__title"><a href="${model.articleLink}">${model.title}</a></h3>
  ${renderAuthors(model.authors)}
  ${renderCurationStatements(model.curationStatementsTeasers)}
  ${renderReviewingGroupsWithLink(model.reviewingGroups)}
  <footer class="article-card__footer">
    <div class="article-card__meta">
      ${renderEvaluationCount(model.evaluationCount)}${renderListMembershipCount(model.listMembershipCount)}${renderArticleVersionDate(model.latestVersionDate)}${renderArticleLatestActivityDate(model.latestActivityAt)}
    </div>
  </footer>
`);

export const renderArticleCard = (model: ArticleCardViewModel): HtmlFragment => toHtmlFragment(`
  <section class="article-card">
    ${renderArticleCardContents(model)}
  </section>
`);
