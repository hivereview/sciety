import { URL } from 'url';
import { pipe } from 'fp-ts/function';
import * as RFI from './review-feed-item.helper';
import { renderFeed } from '../../../src/article-page/render-as-html/render-feed';

describe('render-feed', () => {
  const rendered = pipe(
    [
      RFI.arbitrary(),
      {
        type: 'article-version',
        source: new URL('http://example.com'),
        publishedAt: new Date(),
        version: 1,
        server: 'biorxiv',
      },
      {
        type: 'article-version-error',
        server: 'biorxiv',
      },
    ],
    renderFeed,
  );

  it('returns a list', () => {
    expect(rendered).toContain('<ol');
  });
});
