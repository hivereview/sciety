import * as O from 'fp-ts/Option';
import { buildBasePath } from '../../../../src/html-pages/search-results-page/render-as-html/wrap-with-pagination-information';

describe('buildBasePath', () => {
  it('encodes the cursor for http', () => {
    const nextLinkAnchor = buildBasePath({
      pageNumber: 2,
      category: 'articles',
      query: 'bats',
      evaluatedOnly: false,
      nextCursor: O.some('foo+/bar'),
      numberOfPages: 3,
    });

    const expectedHref = '/search?query=bats&category=articles&cursor=foo%2B%2Fbar&';

    expect(nextLinkAnchor).toStrictEqual(O.some(expectedHref));
  });

  it('encodes the query for http', () => {
    const nextLinkAnchor = buildBasePath({
      pageNumber: 2,
      category: 'articles',
      query: 'bats+bugs',
      evaluatedOnly: false,
      nextCursor: O.some('foo'),
      numberOfPages: 3,
    });

    const expectedHref = '/search?query=bats%2Bbugs&category=articles&cursor=foo&';

    expect(nextLinkAnchor).toStrictEqual(O.some(expectedHref));
  });

  describe('when the evaluatedOnly filter is set', () => {
    const nextLinkAnchor = buildBasePath({
      pageNumber: 2,
      category: 'articles',
      query: 'bats',
      evaluatedOnly: true,
      nextCursor: O.some('foo'),
      numberOfPages: 3,
    });

    it('includes the filter in the href', () => {
      const expectedHref = '/search?query=bats&category=articles&cursor=foo&evaluatedOnly=true&';

      expect(nextLinkAnchor).toStrictEqual(O.some(expectedHref));
    });
  });
});
