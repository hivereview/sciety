import { pipe } from 'fp-ts/function';
import { JSDOM } from 'jsdom';
import { load } from 'cheerio';
import { renderEvaluationPublishedFeedItem } from '../../../../src/html-pages/paper-activity-page/render-as-html/render-evaluation-published-feed-item';
import { missingFullTextAndSourceLink } from '../../../../src/html-pages/paper-activity-page/render-as-html/static-messages';
import { EvaluationLocator, evaluationLocatorCodec } from '../../../../src/types/evaluation-locator';
import { arbitraryNumber, arbitraryWord } from '../../../helpers';
import * as RFI from '../evaluation-feed-item.helper';

const toggleableContentSelector = '[data-behaviour="collapse_to_teaser"]';
const teaserSelector = '[data-teaser]';
const cheerioSafeIdSelector = (evaluationLocator: EvaluationLocator) => `#${evaluationLocatorCodec.encode(evaluationLocator).replace(':', '\\:')}`;

describe('render-evaluation-feed-item', () => {
  const fullText = arbitraryWord(100);

  describe('when the evaluation has long full text', () => {
    const teaserLength = 6;
    const item = pipe(
      RFI.arbitrary(),
      RFI.withFullText(fullText),
    );
    const parsedResult = pipe(
      renderEvaluationPublishedFeedItem(item, teaserLength),
      load,
    );

    it('renders the full text', async () => {
      expect(parsedResult(toggleableContentSelector)).toHaveLength(1);
      expect(parsedResult('[data-full-text]').text()).toStrictEqual(expect.stringContaining(fullText));
      expect(parsedResult(teaserSelector).text()).toStrictEqual(expect.stringContaining('…'));
    });

    it('renders an id tag with the correct value', async () => {
      expect(parsedResult(cheerioSafeIdSelector(item.id))).toHaveLength(1);
    });
  });

  describe('when the evaluation has short full text', () => {
    const source = 'http://example.com/source';
    const item = pipe(
      RFI.arbitrary(),
      RFI.withFullText(fullText),
      RFI.withSource(source),
    );
    const parsedResult = pipe(
      renderEvaluationPublishedFeedItem(item, 200),
      load,
    );

    it('renders without a teaser', async () => {
      expect(parsedResult(toggleableContentSelector)).toHaveLength(0);
      expect(parsedResult(teaserSelector)).toHaveLength(0);
      expect(parsedResult('.activity-feed__item__body').text()).toStrictEqual(expect.stringContaining(fullText));
      expect(parsedResult('.activity-feed__item__read_original_source').attr('href')).toStrictEqual(source);
    });

    it('renders an id tag with the correct value', async () => {
      expect(parsedResult(cheerioSafeIdSelector(item.id))).toHaveLength(1);
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
          renderEvaluationPublishedFeedItem(item, 6),
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
        expect(rendered.getElementById(evaluationLocatorCodec.encode(item.id))).not.toBeNull();
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
          renderEvaluationPublishedFeedItem(item, arbitraryNumber(6, 10)),
          JSDOM.fragment,
        );
      });

      it('displays a message that the evaluation is unavailable', () => {
        expect(rendered.textContent).toContain(missingFullTextAndSourceLink);
      });
    });
  });
});
