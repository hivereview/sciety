import { pipe } from 'fp-ts/function';
import { renderAuthors } from './render-authors';
import { tabs } from '../../shared-components/tabs';
import { ArticleAuthors } from '../../types/article-authors';
import { Doi } from '../../types/doi';
import { HtmlFragment, toHtmlFragment } from '../../types/html-fragment';
import { tabList } from '../tab-list';

// TODO: title should be HtmlFragment and sanitized outside of here
type ArticleDetails = {
  title: string,
  abstract: HtmlFragment,
  authors: ArticleAuthors,
};

export const renderMetaPage = (components: {
  articleDetails: ArticleDetails,
  doi: Doi,
  badge: HtmlFragment,
  saveArticle: HtmlFragment,
  tweetThis: HtmlFragment,
  header: HtmlFragment,
}): HtmlFragment => pipe(
  `
      <section class="article-meta">
        ${renderAuthors(components.articleDetails.authors)}
        <ul aria-label="Publication details" class="article-meta-data-list" role="list">
          <li>
            <a href="https://doi.org/${components.doi.value}" target="_blank">https://doi.org/${components.doi.value}</a>
          </li>
        </ul>
      </section>

      <section class="article-abstract" role="doc-abstract">
        ${components.articleDetails.abstract}
      </section>

      <a href="https://doi.org/${components.doi.value}" class="full-article-button" target="_blank">
        Read the full article
      </a>
  `,
  toHtmlFragment,
  tabs({
    tabList: tabList(components.doi),
    activeTabIndex: 0,
  }),
  (mainContent) => toHtmlFragment(`
    ${components.header}
    <div class="main-content main-content--meta">
      ${mainContent}
    </div>
  `),
);
