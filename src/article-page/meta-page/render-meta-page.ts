import { pipe } from 'fp-ts/function';
import { tabs } from '../../shared-components/tabs';
import { ArticleServer } from '../../types/article-server';
import { Doi } from '../../types/doi';
import { HtmlFragment, toHtmlFragment } from '../../types/html-fragment';
import { tabList } from '../tab-list';

// TODO: title should be HtmlFragment and sanitized outside of here
type ArticleDetails = {
  title: string,
  abstract: HtmlFragment,
  authors: ReadonlyArray<string>,
  server: ArticleServer,
};

export const renderMetaPage = (components: {
  articleDetails: ArticleDetails,
  doi: Doi,
  saveArticle: HtmlFragment,
  tweetThis: HtmlFragment,
}): HtmlFragment => pipe(
  `
      <section class="article-meta">
        <ol aria-label="Authors of this article" class="article-author-list" role="list">
          ${components.articleDetails.authors.map((author) => `<li>${author}</li>`).join('')}
        </ol>
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
    <header class="page-header page-header--article">
      <h1 class="page-header__title">${components.articleDetails.title}</h1>
      <div class="article-actions">
        ${components.tweetThis}
        ${components.saveArticle}
      </div>
    </header>

    <div class="main-content main-content--meta">
      ${mainContent}
    </div>
  `),
);
