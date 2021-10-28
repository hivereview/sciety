import { pipe } from 'fp-ts/function';
import { extractPrelights } from '../../../src/ingest/prelights/extract-prelights';
import {
  arbitraryDate, arbitraryNumber, arbitraryString, arbitraryWord,
} from '../../helpers';
import { arbitraryDoi } from '../../types/doi.helper';

describe('extract-prelights', () => {
  describe('given a valid evaluation with a preprintDoi', () => {
    const guid = `https://prelights.biologists.com/?post_type=highlight&p=${arbitraryNumber(1000, 100000)}`;
    const pubDate = arbitraryDate();
    const preprintDoi = arbitraryDoi('10.1101');
    const author = `${arbitraryString()}, ${arbitraryString()}`;
    const result = pipe(
      [{
        guid,
        category: '<a name = "highlight">highlight</a>',
        pubDate,
        preprintUrl: arbitraryWord(),
        preprintDoi: preprintDoi.value,
        author,
      }],
      extractPrelights,
    );

    it('records the evaluation', () => {
      expect(result).toStrictEqual(expect.objectContaining({
        evaluations: [
          {
            date: pubDate,
            articleDoi: preprintDoi.value,
            evaluationLocator: `prelights:${guid}`,
            authors: [author],
          },
        ],
        skippedItems: [],
      }));
    });
  });

  describe('given a valid evaluation without a preprintDoi', () => {
    const guid = `https://prelights.biologists.com/?post_type=highlight&p=${arbitraryNumber(1000, 100000)}`;
    const pubDate = arbitraryDate();
    const author = arbitraryString();
    const result = pipe(
      [{
        guid,
        category: '<a name = "highlight">highlight</a>',
        pubDate,
        preprintUrl: arbitraryWord(),
        preprintDoi: '',
        author,
      }],
      extractPrelights,
    );

    it('skips the evaluation', () => {
      expect(result).toStrictEqual(expect.objectContaining({
        evaluations: [],
        skippedItems: [expect.objectContaining({
          item: guid,
        })],
      }));
    });
  });

  describe('given an item that is not a highlight', () => {
    const guid = arbitraryWord();
    const result = pipe(
      [{
        guid,
        category: '<a name = "something">something</a>',
        pubDate: arbitraryDate(),
        preprintDoi: arbitraryString(),
        preprintUrl: arbitraryWord(),
        author: arbitraryString(),
      }],
      extractPrelights,
    );

    it('skips the item', () => {
      expect(result).toStrictEqual(expect.objectContaining({
        evaluations: [],
        skippedItems: [expect.objectContaining({
          item: guid,
        })],
      }));
    });
  });

  describe('when the preprint is not on a supported server', () => {
    it.todo('skips the item');
  });
});
