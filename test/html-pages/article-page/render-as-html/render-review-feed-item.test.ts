import { pipe } from 'fp-ts/function';
import { JSDOM } from 'jsdom';
import { renderReviewFeedItem } from '../../../../src/html-pages/article-page/render-as-html/render-review-feed-item';
import { missingFullTextAndSourceLink } from '../../../../src/html-pages/article-page/render-as-html/static-messages';
import { reviewIdCodec } from '../../../../src/types/evaluation-locator';
import { arbitraryNumber } from '../../../helpers';
import * as RFI from '../review-feed-item.helper';

describe('render-review-feed-item', () => {
  describe.each([
    ['en', 'Arbitrary full text of an evaluation'],
    ['es', 'Texto completo arbitrario de una evaluación'],
    ['pt', 'Texto completo arbitrário de uma avaliação'],
  ])('when the evaluation has full text in language %s', (code, fullText) => {
    describe('when the evaluation has long full text', () => {
      const teaserLength = 6;
      const item = pipe(
        RFI.arbitrary(),
        RFI.withFullText(fullText),
      );
      const rendered: DocumentFragment = pipe(
        renderReviewFeedItem(item, teaserLength),
        JSDOM.fragment,
      );
      const fullTextWrapper = rendered.querySelector('[data-full-text]');
      const teaserWrapper = rendered.querySelector('[data-teaser]');

      it('renders the full text', async () => {
        const toggleableContent = rendered.querySelector('[data-behaviour="collapse_to_teaser"]');

        expect(toggleableContent).not.toBeNull();
        expect(fullTextWrapper?.textContent).toStrictEqual(expect.stringContaining(fullText));
        expect(teaserWrapper?.textContent).toStrictEqual(expect.stringContaining('…'));
      });

      it('renders an id tag with the correct value', async () => {
        expect(rendered.getElementById(reviewIdCodec.encode(item.id))).not.toBeNull();
      });

      it('infers the language of the full text', () => {
        expect(fullTextWrapper?.innerHTML).toStrictEqual(expect.stringContaining(`<div lang="${code}">${fullText}</div>`));
      });

      it('infers the language of the teaser', () => {
        expect(teaserWrapper?.getAttribute('lang')).toBe(code);
      });
    });

    describe('when the evaluation has short full text', () => {
      const source = 'http://example.com/source';
      const item = pipe(
        RFI.arbitrary(),
        RFI.withFullText(fullText),
        RFI.withSource(source),
      );
      const rendered: DocumentFragment = pipe(
        renderReviewFeedItem(item, 200),
        JSDOM.fragment,
      );
      const fullTextWrapper = rendered.querySelector('.activity-feed__item__body');
      const teaserWrapper = rendered.querySelector('[data-teaser]');

      it('renders without a teaser', async () => {
        const toggleableContent = rendered.querySelector('[data-behaviour="collapse_to_teaser"]');
        const sourceLinkUrl = rendered.querySelector('.activity-feed__item__read_original_source')?.getAttribute('href');

        expect(toggleableContent).toBeNull();
        expect(teaserWrapper).toBeNull();
        expect(fullTextWrapper?.textContent).toStrictEqual(expect.stringContaining(fullText));
        expect(sourceLinkUrl).toStrictEqual(source);
      });

      it('renders an id tag with the correct value', async () => {
        expect(rendered.getElementById(reviewIdCodec.encode(item.id))).not.toBeNull();
      });

      it('infers the language of the full text', () => {
        expect(fullTextWrapper?.innerHTML).toStrictEqual(expect.stringContaining(`<div lang="${code}">${fullText}</div>`));
      });
    });
  });

  describe('when the evaluation has no full text', () => {
    describe('when there is a source link URL', () => {
      const source = 'http://example.com/source';
      let rendered: DocumentFragment;
      const item = pipe(
        RFI.arbitrary(),
        RFI.withNoFullText,
        RFI.withSource(source),
      );

      beforeEach(() => {
        rendered = pipe(
          renderReviewFeedItem(item, 6),
          JSDOM.fragment,
        );
      });

      it('renders without a teaser', async () => {
        const toggleableContent = rendered.querySelector('[data-behaviour="collapse_to_teaser"]');
        const sourceLinkUrl = rendered.querySelector('.activity-feed__item__read_original_source')?.getAttribute('href');

        expect(toggleableContent).toBeNull();
        expect(sourceLinkUrl).toStrictEqual(source);
      });

      it('renders an id tag with the correct value', async () => {
        expect(rendered.getElementById(reviewIdCodec.encode(item.id))).not.toBeNull();
      });
    });

    describe('when there is no source link URL', () => {
      let rendered: DocumentFragment;
      const item = pipe(
        RFI.arbitrary(),
        RFI.withNoFullText,
        RFI.withNoSource,
      );

      beforeEach(() => {
        rendered = pipe(
          renderReviewFeedItem(item, arbitraryNumber(6, 10)),
          JSDOM.fragment,
        );
      });

      it('displays a message that the evaluation is unavailable', () => {
        expect(rendered.textContent).toContain(missingFullTextAndSourceLink);
      });
    });
  });
});
