import * as E from 'fp-ts/Either';
import { pipe } from 'fp-ts/function';
import { deriveFullTextsOfEvaluations } from '../../../src/third-parties/access-microbiology/derive-full-texts-of-evaluations.js';
import { abortTest } from '../../framework/abort-test.js';
import { arbitraryWord } from '../../helpers.js';
import { SanitisedHtmlFragment } from '../../../src/types/sanitised-html-fragment.js';
import { dummyLogger } from '../../dummy-logger.js';

describe('derive-full-texts-of-evaluations', () => {
  describe('given an input that is not a string', () => {
    const result = deriveFullTextsOfEvaluations(dummyLogger)(undefined);

    it('fails', () => {
      expect(E.isLeft(result)).toBe(true);
    });
  });

  describe('given a string input that cannot be parsed', () => {
    let result: ReturnType<ReturnType<typeof deriveFullTextsOfEvaluations>>;

    beforeEach(() => {
      result = deriveFullTextsOfEvaluations(dummyLogger)('<><');
    });

    it('fails', () => {
      expect(E.isLeft(result)).toBe(true);
    });
  });

  describe('given an input containing a single sub-article without a body', () => {
    let result: ReadonlyMap<string, SanitisedHtmlFragment>;

    beforeEach(() => {
      result = pipe(
        `
          <article>
            <sub-article>
              <front-stub>
                <article-id>10.1099/acmi.0.000569.v1.1</article-id>
              </front-stub>
            </sub-article>
          </article>
        `,
        deriveFullTextsOfEvaluations(dummyLogger),
        E.getOrElseW(abortTest('returned on the left')),
      );
    });

    it('returns an empty map', () => {
      expect(result.size).toBe(0);
    });
  });

  describe('given an input containing a single sub-article with a body containing a single paragraph', () => {
    const subArticleId = arbitraryWord();
    let result: ReadonlyMap<string, SanitisedHtmlFragment>;

    beforeEach(() => {
      result = pipe(
        `
          <article>
            <sub-article>
              <front-stub>
                <article-id>${subArticleId}</article-id>
              </front-stub>
              <body>
                <p>The paragraph text.</p>
              </body>
            </sub-article>
          </article>
        `,
        deriveFullTextsOfEvaluations(dummyLogger),
        E.getOrElseW(abortTest('returned on the left')),
      );
    });

    it('returns a map with one element', () => {
      expect(result.size).toBe(1);
    });

    it('produces a key generated by the <article-id> of its <sub-article>', () => {
      expect(result.has(subArticleId)).toBe(true);
    });

    it('produces a value generated from the <body> unchanged', () => {
      expect(result.get(subArticleId)).toBe('<p>The paragraph text.</p>');
    });
  });

  describe('given an input containing a single sub-article with a body containing <bold> XML tags', () => {
    const subArticleId = arbitraryWord();
    let result: ReadonlyMap<string, SanitisedHtmlFragment>;

    beforeEach(() => {
      result = pipe(
        `
          <article>
            <sub-article>
              <front-stub>
                <article-id>${subArticleId}</article-id>
              </front-stub>
              <body>
                <p>The paragraph <bold>text</bold>.</p>
              </body>
            </sub-article>
          </article>
        `,
        deriveFullTextsOfEvaluations(dummyLogger),
        E.getOrElseW(abortTest('returned on the left')),
      );
    });

    it('produces a value containing <b> HTML tags', () => {
      expect(result.get(subArticleId)).toBe('<p>The paragraph <b>text</b>.</p>');
    });
  });

  describe('given an input containing two sub-articles with bodies', () => {
    let result: ReadonlyMap<string, SanitisedHtmlFragment>;

    beforeEach(() => {
      result = pipe(
        `
          <article>
            <sub-article>
              <front-stub>
                <article-id>${arbitraryWord()}</article-id>
              </front-stub>
              <body>
                <p>First sub-article's paragraph text.</p>
              </body>
            </sub-article>
            <sub-article>
              <front-stub>
                <article-id>${arbitraryWord()}</article-id>
              </front-stub>
              <body>
                <p>Second sub-article's paragraph text.</p>
              </body>
            </sub-article>
          </article>
        `,
        deriveFullTextsOfEvaluations(dummyLogger),
        E.getOrElseW(abortTest('returned on the left')),
      );
    });

    it('returns a map with two elements', () => {
      expect(result.size).toBe(2);
    });
  });
});
