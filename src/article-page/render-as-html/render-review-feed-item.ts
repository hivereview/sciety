import { htmlEscape } from 'escape-goat';
import * as O from 'fp-ts/Option';
import { constant, flow, pipe } from 'fp-ts/function';
import clip from 'text-clipper';
import { renderReviewResponses } from './render-review-responses';
import { missingFullTextAndSourceLink } from './static-messages';
import { templateDate } from '../../shared-components/date';
import { langAttributeFor } from '../../shared-components/lang-attribute-for';
import { HtmlFragment, toHtmlFragment } from '../../types/html-fragment';
import * as RI from '../../types/review-id';
import { ReviewFeedItem } from '../view-model';

type RenderReviewFeedItem = (review: ReviewFeedItem) => HtmlFragment;

const avatar = (review: ReviewFeedItem) => toHtmlFragment(`
  <img class="activity-feed__item__avatar" src="${review.groupAvatar}" alt="">
`);

const eventMetadata = (review: ReviewFeedItem) => toHtmlFragment(`
  <div class="activity-feed__item__meta">
    <div class="activity-feed__item__title">
      <a href="${review.groupHref}">
        ${htmlEscape(review.groupName)}
      </a>
    </div>
    ${templateDate(review.publishedAt, 'activity-feed__item__date')}
  </div>
`);

const appendSourceLink = flow(
  (review: ReviewFeedItem) => review.source,
  O.map(flow(
    (source) => `
      <div data-read-original-source>
        <a href="${source.toString()}" class="activity-feed__item__read_original_source">
          Read the original source
        </a>
      </div>
    `,
    toHtmlFragment,
  )),
);

const renderWithText = (teaserChars: number, review: ReviewFeedItem, fullText: string) => (responses: HtmlFragment) => {
  const teaserText = clip(fullText, teaserChars, { html: true });
  const fulltextAndSourceLink = `
    <div${langAttributeFor(fullText)}>${fullText}</div>
    ${pipe(review, appendSourceLink, O.getOrElse(constant('')))}
  `;
  let feedItemBody = `
    <div class="activity-feed__item__body" data-behaviour="collapse_to_teaser">
      <div class="hidden" data-teaser${langAttributeFor(fullText)}>
        ${teaserText}
      </div>
      <div data-full-text>
        ${fulltextAndSourceLink}
      </div>
    </div>
  `;

  if (teaserText === fullText) {
    feedItemBody = `
      <div class="activity-feed__item__body">
        <div>
          ${fulltextAndSourceLink}
        </div>
      </div>
    `;
  }
  return `
    <article class="activity-feed__item__contents" id="${RI.reviewIdCodec.encode(review.id)}">
      <header class="activity-feed__item__header">
        ${avatar(review)}
        ${eventMetadata(review)}
      </header>
      ${feedItemBody}
    </article>
    ${responses}
  `;
};

const renderSourceLinkWhenFulltextMissing = (review: ReviewFeedItem) => pipe(
  review,
  appendSourceLink,
  O.getOrElse(constant(missingFullTextAndSourceLink)),
);

const render = (teaserChars: number, review: ReviewFeedItem, responses: HtmlFragment) => pipe(
  review.fullText,
  O.fold(
    () => `
      <article class="activity-feed__item__contents" id="${RI.reviewIdCodec.encode(review.id)}">
        <header class="activity-feed__item__header">
          ${avatar(review)}
          ${eventMetadata(review)}
        </header>
        <div class="activity-feed__item__body">
          <div>
            ${renderSourceLinkWhenFulltextMissing(review)}
          </div>
        </div>
      </article>
      ${responses}
    `,
    (fullText) => renderWithText(teaserChars, review, fullText)(responses),
  ),
);

export const renderReviewFeedItem = (
  teaserChars: number,
): RenderReviewFeedItem => flow(
  (review) => render(teaserChars, review, renderReviewResponses({ ...review, evaluationLocator: review.id })),
  toHtmlFragment,
);
