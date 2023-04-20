import * as E from 'fp-ts/Either';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { fetchRapidReviews } from '../../src/ingest/fetch-rapid-reviews';
import { FeedData } from '../../src/ingest/update-all';
import { arbitraryDate, arbitraryUri, arbitraryWord } from '../helpers';
import { shouldNotBeCalled } from '../should-not-be-called';
import { arbitraryDoi } from '../types/doi.helper';

const ingest = (crossrefResponseItems: ReadonlyArray<unknown>) => pipe(
  {
    fetchData: <D>() => TE.right({ message: { items: crossrefResponseItems } } as unknown as D),
    fetchGoogleSheet: shouldNotBeCalled,
  },
  fetchRapidReviews(),
);

describe('fetch-rapid-reviews', () => {
  const arbitraryPrimaryUrl = `https://rrid.${arbitraryWord()}`;

  describe('when there are no Crossref reviews', () => {
    it('returns no evaluations and no skipped items', async () => {
      expect(await ingest([])()).toStrictEqual(E.right({
        evaluations: [],
        skippedItems: [],
      }));
    });
  });

  describe('when there is a valid Crossref review', () => {
    it('returns 1 evaluation and no skipped items', async () => {
      const articleDoi = arbitraryDoi().value;
      const date = arbitraryDate();
      const reviewUrl = arbitraryUri();
      const items = [
        {
          URL: reviewUrl,
          created: { 'date-time': date.toString() },
          relation: { 'is-review-of': [{ id: articleDoi }] },
          resource: {
            primary: {
              URL: arbitraryPrimaryUrl,
            },
          },
        },
      ];

      expect(await ingest(items)()).toStrictEqual(E.right({
        evaluations: [
          {
            articleDoi,
            date,
            evaluationLocator: `rapidreviews:${reviewUrl}`,
            authors: [],
          },
        ],
        skippedItems: [],
      }));
    });
  });

  describe('when there is a valid Crossref review that is not by Rapid Reviews Infectious Diseases', () => {
    it('returns a skipped item', async () => {
      const articleDoi = arbitraryDoi().value;
      const date = arbitraryDate();
      const reviewUrl = arbitraryUri();
      const items = [
        {
          URL: reviewUrl,
          created: { 'date-time': date.toString() },
          relation: { 'is-review-of': [{ id: articleDoi }] },
          resource: {
            primary: {
              URL: arbitraryUri(),
            },
          },
        },
      ];

      expect(await ingest(items)()).toStrictEqual(E.right({
        evaluations: [],
        skippedItems: [
          expect.objectContaining({
            item: reviewUrl,
          }),
        ],
      }));
    });
  });

  describe('when there is a valid Crossref review with multiple authors', () => {
    const items = [
      {
        URL: arbitraryUri(),
        created: { 'date-time': arbitraryDate().toString() },
        relation: { 'is-review-of': [{ id: arbitraryDoi().value }] },
        author: [
          { given: 'Fred', family: 'Blogs' },
          { given: 'Joe', family: 'Smith' },
        ],
        resource: {
          primary: {
            URL: arbitraryPrimaryUrl,
          },
        },
      },
    ];

    let authors: ReadonlyArray<string>;

    beforeEach(async () => {
      authors = await pipe(
        items,
        ingest,
        TE.getOrElse(shouldNotBeCalled),
        T.map((result) => result.evaluations[0].authors),
      )();
    });

    it('maintains the order of the authors', () => {
      expect(authors).toStrictEqual(['Fred Blogs', 'Joe Smith']);
    });
  });

  describe('when there is an Crossref review with no author field', () => {
    const items = [
      {
        URL: arbitraryUri(),
        created: { 'date-time': arbitraryDate().toString() },
        relation: { 'is-review-of': [{ id: arbitraryDoi().value }] },
        resource: {
          primary: {
            URL: arbitraryPrimaryUrl,
          },
        },
      },
    ];

    let result: FeedData;

    beforeEach(async () => {
      result = await pipe(
        items,
        ingest,
        TE.getOrElse(shouldNotBeCalled),
      )();
    });

    it('returns an evaluation with an empty array of authors', () => {
      expect(result.evaluations[0].authors).toStrictEqual([]);
    });
  });

  describe('when there is an Crossref review with an empty array for the author field', () => {
    const items = [
      {
        URL: arbitraryUri(),
        created: { 'date-time': arbitraryDate().toString() },
        relation: { 'is-review-of': [{ id: arbitraryDoi().value }] },
        author: [],
        resource: {
          primary: {
            URL: arbitraryPrimaryUrl,
          },
        },
      },
    ];

    let result: FeedData;

    beforeEach(async () => {
      result = await pipe(
        items,
        ingest,
        TE.getOrElse(shouldNotBeCalled),
      )();
    });

    it('returns an evaluation with an empty array of authors', () => {
      expect(result.evaluations[0].authors).toStrictEqual([]);
    });
  });

  describe('when there is an Crossref review from an author with only a family name', () => {
    const familyName = arbitraryWord();
    const items = [
      {
        URL: arbitraryUri(),
        created: { 'date-time': arbitraryDate().toString() },
        relation: { 'is-review-of': [{ id: arbitraryDoi().value }] },
        author: [{ family: familyName }],
        resource: {
          primary: {
            URL: arbitraryPrimaryUrl,
          },
        },
      },
    ];

    let result: FeedData;

    beforeEach(async () => {
      result = await pipe(
        items,
        ingest,
        TE.getOrElse(shouldNotBeCalled),
      )();
    });

    it('returns the evaluation including an author with that family name', () => {
      expect(result.evaluations[0].authors).toStrictEqual([
        familyName,
      ]);
    });
  });

  describe('when there is an Crossref review from an author with both a given name and a family name', () => {
    const givenName = arbitraryWord();
    const familyName = arbitraryWord();
    const items = [
      {
        URL: arbitraryUri(),
        created: { 'date-time': arbitraryDate().toString() },
        relation: { 'is-review-of': [{ id: arbitraryDoi().value }] },
        author: [{
          given: givenName,
          family: familyName,
        }],
        resource: {
          primary: {
            URL: arbitraryPrimaryUrl,
          },
        },
      },
    ];

    let result: FeedData;

    beforeEach(async () => {
      result = await pipe(
        items,
        ingest,
        TE.getOrElse(shouldNotBeCalled),
      )();
    });

    it('returns the evaluation including an author with both those names', () => {
      expect(result.evaluations[0].authors).toStrictEqual([
        `${givenName} ${familyName}`,
      ]);
    });
  });
});
