import * as E from 'fp-ts/Either';
import * as TE from 'fp-ts/TaskEither';
import { aboutPage } from '../../../src/html-pages/about-page';

const fetchStaticFile = (filename: string) => TE.right(`# Contents of ${filename}`);

describe('create render page', () => {
  it('inserts the HTML text into the response body', async () => {
    const renderPage = aboutPage(fetchStaticFile);
    const rendered = await renderPage();

    expect(rendered).toStrictEqual(E.right(expect.objectContaining({
      content: expect.stringContaining('<h1>Contents of about.md</h1>'),
    })));
  });
});
