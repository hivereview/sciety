import { URL } from 'url';
import * as O from 'fp-ts/Option';
import * as T from 'fp-ts/Task';
import { constant, flow, pipe } from 'fp-ts/function';
import clip from 'text-clipper';
import { RenderReviewResponses } from './render-review-responses';
import { templateDate } from '../shared-components';
import { EditorialCommunityId } from '../types/editorial-community-id';
import { HtmlFragment, toHtmlFragment } from '../types/html-fragment';
import { ReviewId, toString } from '../types/review-id';
import { SanitisedHtmlFragment } from '../types/sanitised-html-fragment';
import { UserId } from '../types/user-id';

export type RenderReviewFeedItem = (review: ReviewFeedItem, userId: O.Option<UserId>) => T.Task<HtmlFragment>;

export type ReviewFeedItem = {
  type: 'review',
  id: ReviewId,
  source: O.Option<URL>,
  occurredAt: Date,
  editorialCommunityId: EditorialCommunityId,
  editorialCommunityName: string,
  editorialCommunityAvatar: string,
  fullText: O.Option<SanitisedHtmlFragment>,
};

const avatar = (review: ReviewFeedItem): HtmlFragment => toHtmlFragment(`
  <img class="article-feed__item__avatar" src="${review.editorialCommunityAvatar}" alt="">
`);

const eventMetadata = (review: ReviewFeedItem): HtmlFragment => toHtmlFragment(`
  ${templateDate(review.occurredAt, 'article-feed__item__date')}
  <div class="article-feed__item__title">
    ${(review.editorialCommunityId.value === 'f97bd177-5cb6-4296-8573-078318755bf2') ? 'Highlighted by' : 'Reviewed by'}
    <a href="/groups/${review.editorialCommunityId.value}">
      ${review.editorialCommunityName}
    </a>
  </div>
`);

const sourceLink = (review: ReviewFeedItem): O.Option<HtmlFragment> => pipe(
  review.source,
  O.map(
    (source) => `<a href="${source.toString()}" class="article-feed__item__read_more article-call-to-action-link">
    Read the original source
  </a>`,
  ),
  O.map(toHtmlFragment),
);

type RenderWithText = (
  teaserChars: number,
  review: ReviewFeedItem,
  fullText: string,
) => (responses: HtmlFragment) => string;

const renderWithText: RenderWithText = (teaserChars, review, fullText) => (responses) => {
  const teaserText = clip(fullText, teaserChars);
  if (teaserText === fullText) {
    return `
      <div class="article-feed__item_contents" id="${toString(review.id)}">
        ${avatar(review)}
        <div class="article-feed__item_body">
          ${eventMetadata(review)}
          <div>
            ${fullText}
            ${pipe(review, sourceLink, O.getOrElse(constant('')))}
          </div>
        </div>
      </div>
      ${responses}
    `;
  }
  // TODO: a review.id containing dodgy chars could break this
  return `
    <div class="article-feed__item_contents" id="${toString(review.id)}">
      ${avatar(review)}
      <div class="article-feed__item_body" data-behaviour="collapse_to_teaser">
        ${eventMetadata(review)}
        <div class="hidden" data-teaser>
          ${teaserText}
        </div>
        <div data-full-text>
          ${fullText}
          ${pipe(review, sourceLink, O.getOrElse(constant('')))}
        </div>
      </div>
    </div>
    ${responses}
  `;
};

const render = (teaserChars: number, review: ReviewFeedItem) => (responses: HtmlFragment): string => pipe(
  review.fullText,
  O.fold(
    () => `
      <div class="article-feed__item_contents" id="${toString(review.id)}">
        ${avatar(review)}
        <div class="article-feed__item_body">
          ${eventMetadata(review)}
          <div>
            ${pipe(review, sourceLink, O.getOrElse(constant('')))}
          </div>
        </div>
      </div>
      ${responses}
    `,
    (fullText) => renderWithText(teaserChars, review, fullText)(responses),
  ),
);

export const createRenderReviewFeedItem = (
  teaserChars: number,
  renderReviewResponses: RenderReviewResponses,
): RenderReviewFeedItem => (review, userId) => pipe(
  renderReviewResponses(review.id, userId),
  T.map(flow(
    // TODO: remove the currying
    render(teaserChars, review),
    toHtmlFragment,
  )),
);
