import { renderVersionErrorFeedItem } from './render-article-version-error-feed-item';
import { templateDate } from '../../shared-components/date';
import { HtmlFragment, toHtmlFragment } from '../../types/html-fragment';
import { ArticleVersionFeedItem } from '../view-model';

type RenderArticleVersionFeedItem = (feedItem: ArticleVersionFeedItem) => HtmlFragment;

const renderVersionFeedItem = (feedItem: ArticleVersionFeedItem, name: string, avatarUrl: string) => toHtmlFragment(`
  <div class="activity-feed__item__contents">
    <header class="activity-feed__item__header">
      <img class="activity-feed__item__avatar" src="${avatarUrl}" alt="">
      <div class="activity-feed__item__meta">
        <div class="activity-feed__item__title">
          <a href="${feedItem.source.toString()}">
            Version ${feedItem.version} published on ${name}
          </a>
        </div>
        ${templateDate(feedItem.publishedAt, 'activity-feed__item__date')}
      </div>
    </header>
  </div>
`);

export const renderArticleVersionFeedItem: RenderArticleVersionFeedItem = (feedItem) => {
  switch (feedItem.server) {
    case 'medrxiv':
      return renderVersionFeedItem(feedItem, 'medRxiv', '/static/images/medrxiv.jpg');
    case 'biorxiv':
      return renderVersionFeedItem(feedItem, 'bioRxiv', '/static/images/biorxiv.jpg');
    default:
      return renderVersionErrorFeedItem(feedItem.server);
  }
};
