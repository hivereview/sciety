import templateDate from '../templates/date';
import Doi from '../types/doi';
import { DomainEvent, isEditorialCommunityEndorsedArticleEvent, isEditorialCommunityReviewedArticleEvent } from '../types/domain-events';
import EditorialCommunityId from '../types/editorial-community-id';

export type RenderFeedItem = (event: DomainEvent) => Promise<string>;

type Actor = {
  url: string;
  name: string;
  imageUrl: string;
};

type Article = {
  title: string;
};

type RenderFeedItemSummary = (event: DomainEvent, actor: Actor) => Promise<string>;

const createRenderFeedItemSummary = (getArticle: GetArticle): RenderFeedItemSummary => (
  async (event, actor) => {
    if (isEditorialCommunityEndorsedArticleEvent(event)) {
      const article = await getArticle(event.articleId);

      return `
        <a href="${actor.url}">${actor.name}</a>
        endorsed
        <a href="/articles/${event.articleId.value}">${article.title}</a>
      `;
    }
    if (isEditorialCommunityReviewedArticleEvent(event)) {
      const article = await getArticle(event.articleId);

      return `
        <a href="${actor.url}">${actor.name}</a>
        reviewed
        <a href="/articles/${event.articleId.value}">${article.title}</a>
      `;
    }
    return `
      <a href="${actor.url}">${actor.name}</a>
      joined The Hive
    `;
  }
);

export type GetActor = (id: EditorialCommunityId) => Promise<Actor>;

export type GetArticle = (id: Doi) => Promise<Article>;

export default (
  getActor: GetActor,
  getArticle: GetArticle,
): RenderFeedItem => {
  const renderFeedItemSummary = createRenderFeedItemSummary(getArticle);

  return async (event) => {
    let actor: Actor;
    if (isEditorialCommunityEndorsedArticleEvent(event)) {
      actor = await getActor(event.editorialCommunityId);
    } else {
      actor = await getActor(event.actorId);
    }
    return `
      <div class="label">
        <img src="${actor.imageUrl}" alt="">
      </div>
      <div class="content">
        <div class="date">
          ${templateDate(event.date)}
        </div>
        <div class="summary">
          ${await renderFeedItemSummary(event, actor)}
        </div>
      </div>
    `;
  };
};
