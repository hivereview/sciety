import * as O from 'fp-ts/Option';
import { buildPageUrl } from '../../../src/html-pages/search-results-page/build-page-url';
import { arbitraryBoolean, arbitraryString } from '../../helpers';

const arbitraryCategory = (): 'articles' | 'groups' => (arbitraryBoolean() ? 'articles' : 'groups');

describe('build-page-url', () => {
  const defaultParams = {
    category: arbitraryCategory(),
    query: arbitraryString(),
    evaluatedOnly: arbitraryBoolean(),
    cursor: O.none,
  };

  it('builds the URL with the correct category', () => {
    const result = buildPageUrl({
      ...defaultParams,
      category: 'articles',
    });

    expect(result).toContain('category=articles');
  });

  it('builds the URL with the correct query', () => {
    const query = 'a search term';
    const result = buildPageUrl({
      ...defaultParams,
      query,
    });

    expect(result).toContain('query=a%20search%20term');
  });

  it('builds the URL with a query with special characters', () => {
    const query = 'covid&cells';
    const result = buildPageUrl({
      ...defaultParams,
      query,
    });

    expect(result).toContain('query=covid%26cells');
  });

  describe('when evaluatedOnly is true', () => {
    it('builds the URL with the evaluatedOnly filter set', () => {
      const result = buildPageUrl({
        ...defaultParams,
        evaluatedOnly: true,
      });

      expect(result).toContain('evaluatedOnly=true');
    });
  });

  describe('when evaluatedOnly is false', () => {
    it('builds the URL without the evaluatedOnly filter set', () => {
      const result = buildPageUrl({
        ...defaultParams,
        evaluatedOnly: false,
      });

      expect(result).not.toContain('evaluatedOnly');
    });
  });

  describe('when there is a cursor', () => {
    it('includes the encoded cursor as part of the URL', () => {
      const result = buildPageUrl({
        ...defaultParams,
        cursor: O.some('12+34'),
      });

      expect(result).toContain('cursor=12%2B34');
    });
  });

  describe('when there is no cursor', () => {
    it('builds the URL without a cursor', () => {
      const result = buildPageUrl({
        ...defaultParams,
        cursor: O.none,
      });

      expect(result).not.toContain('cursor');
    });
  });
});
