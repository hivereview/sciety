import { Doi } from '../types/doi';
import { HtmlFragment, toHtmlFragment } from '../types/html-fragment';

type ArticleDetails = {
  title: string,
};

export const renderActivityPage = (components: {
  articleDetails: ArticleDetails,
  doi: Doi,
  feed: string,
  saveArticle: string,
  tweetThis: string,
}): HtmlFragment => toHtmlFragment(`
  <article class="sciety-grid sciety-grid--activity">
    <header class="page-header page-header--article">
      <h1>${components.articleDetails.title}</h1>
      <div class="article-actions">
        ${components.tweetThis}
        ${components.saveArticle}
      </div>
    </header>
    <div class="article-tabs">
      <a class="article-tabs__tab article-tabs__link" href="/articles/meta/${components.doi.value}" aria-label="Discover article information and abstract">Article</a>
      <h2 class="article-tabs__tab article-tabs__heading">Activity</h2>
    </div>
    <div class="main-content main-content--article">
      ${components.feed}
    </div>
  </article>
`);
