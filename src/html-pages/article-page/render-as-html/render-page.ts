import { renderFeed } from './render-feed';
import { renderSaveArticle } from './render-save-article';
import { HtmlFragment, toHtmlFragment } from '../../../types/html-fragment';
import { ViewModel } from '../view-model';
import { renderListedIn } from './render-listed-in';
import { renderRelatedArticles } from './render-related-articles';
import { renderLangAttribute } from '../../../shared-components/lang-attribute';
import { renderHeader } from './render-header';
import { renderRelatedArticlesLink } from './render-related-articles-link';

export const renderPage = (viewmodel: ViewModel): HtmlFragment => toHtmlFragment(`
  <div class="article-page-wrapper">
    ${renderHeader(viewmodel)}
    <div class="sciety-grid-two-columns">
      <section class="article-actions">
        <a href="${viewmodel.fullArticleUrl}" class="full-article-button">Read the full article</a>
        ${renderRelatedArticlesLink(viewmodel.relatedArticles)}
        <div class="list-management">
          ${renderListedIn(viewmodel.listedIn)}
          ${renderSaveArticle(viewmodel)}
        </div>
      </section>
      <section>
        <section role="doc-abstract" class="article-abstract">
          <h2>Abstract</h2>
          <div${renderLangAttribute(viewmodel.abstractLanguageCode)}>${viewmodel.abstract}</div>
        </section>
        ${renderFeed(viewmodel.feedItemsByDateDescending)}
        ${renderRelatedArticles(viewmodel)}
      </section>
    </div>
  </div>
`);
