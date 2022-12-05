import { renderPage } from '../../../src/html-pages/about-page/render-page';

describe('render-about-page middleware', () => {
  it('returns a page with the given HTML embedded', async () => {
    const html = '<h1>About stuff</h1>';
    const rendered = renderPage(html);

    expect(rendered).toContain(html);
  });
});
