import * as O from 'fp-ts/Option';
import { pipe } from 'fp-ts/function';
import { extractPrelights } from '../../../src/ingest/prelights/extract-prelights';
import { arbitraryDate, arbitraryWord } from '../../helpers';
import { shouldNotBeCalled } from '../../should-not-be-called';

describe('extract-prelights', () => {
  describe('given a valid evaluation', () => {
    it.todo('records the evaluation');
  });

  describe('given an item that is not a highlight', () => {
    const guid = arbitraryWord();
    const fetchData = shouldNotBeCalled;
    const result = pipe(
      [{
        guid,
        category: '<a name = "something">something</a>',
        pubDate: arbitraryDate(),
        preprintUrl: arbitraryWord(),
      }],
      extractPrelights(fetchData),
    );

    it('skips the item', async () => {
      expect(await result()).toStrictEqual(expect.objectContaining({
        evaluations: [],
        skippedItems: O.some([expect.objectContaining({
          item: guid,
        })]),
      }));
    });
  });

  describe('when the preprint does not exist', () => {
    it.todo('skips the item');
  });

  describe('when the preprint does not have a DC.Identifier', () => {
    it.todo('skips the item');
  });

  describe('when the preprint is not on a supported server', () => {
    it.todo('skips the item');
  });
});
