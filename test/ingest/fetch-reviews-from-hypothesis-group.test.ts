import * as E from 'fp-ts/Either';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { processServer } from '../../src/ingest/fetch-reviews-from-hypothesis-group';
import { arbitraryWord } from '../helpers';

describe('fetch-reviews-from-hypothesis-group', () => {
  describe('when there is one page of annotations', () => {
    it('returns the annotations from that page', async () => {
      const fetchData = jest.fn()
        .mockReturnValueOnce(TE.right({
          rows: [
            {
              id: arbitraryWord(),
              created: arbitraryWord(),
              uri: arbitraryWord(),
            },
            {
              id: arbitraryWord(),
              created: arbitraryWord(),
              uri: arbitraryWord(),
            },
          ],
        }))
        .mockReturnValueOnce(TE.right({
          rows: [],
        }));
      const result = await processServer(arbitraryWord(), fetchData)('medrxiv')();

      expect(pipe(
        result,
        E.map((items) => items.length),
      )).toStrictEqual(E.right(2));
    });
  });

  describe('when the second page of annotations cannot be fetched', () => {
    it('returns an error', async () => {
      const fetchData = jest.fn()
        .mockReturnValueOnce(TE.right({
          rows: [
            {
              id: arbitraryWord(),
              created: arbitraryWord(),
              uri: arbitraryWord(),
            },
            {
              id: arbitraryWord(),
              created: arbitraryWord(),
              uri: arbitraryWord(),
            },
          ],
        }))
        .mockReturnValueOnce(TE.left('bad thing occurred'));
      const result = await processServer(arbitraryWord(), fetchData)('medrxiv')();

      expect(result).toStrictEqual(E.left('bad thing occurred'));
    });
  });
});
