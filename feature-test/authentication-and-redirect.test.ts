import dotenv from 'dotenv';
import {
  $, click, closeBrowser, currentURL, goBack, goto, link, openBrowser,
} from 'taiko';
import { authenticateViaTwitter } from './utilities';

describe('authentication-and-redirect', () => {
  beforeEach(async () => {
    dotenv.config();
    await openBrowser();
  });

  afterEach(closeBrowser);

  it('log in works', async () => {
    await goto('localhost:8080');
    await click('Log in');
    await authenticateViaTwitter();
    const result = await link('Log out').exists();

    expect(result).toBe(true);
  });

  describe('not logged in', () => {
    it('log in from the article page returns to the article page', async () => {
      await goto('localhost:8080/articles/10.1101/2020.07.13.199174');
      await click('Log in');
      await authenticateViaTwitter();

      const result = await currentURL();

      expect(result).toContain('/articles/10.1101/2020.07.13.199174');
    });

    it('respond command returns to review fragment on the article page', async () => {
      await goto('localhost:8080/articles/10.1101/2020.07.13.199174');
      await click('Got it!');
      await click($('.article-feed__item:first-child button[value="respond-helpful"]'));
      await authenticateViaTwitter();

      const result = await currentURL();

      expect(result).toMatch(/\/articles\/10\.1101\/2020\.07\.13\.199174#(hypothesis|doi):/);
    });

    it('follow command from the editorial community page returns to the editorial community page', async () => {
      await goto('localhost:8080/editorial-communities/4eebcec9-a4bb-44e1-bde3-2ae11e65daaa');
      await click('Got it!');
      await click('Follow');
      await authenticateViaTwitter();

      const result = await currentURL();

      expect(result).toContain('/editorial-communities/4eebcec9-a4bb-44e1-bde3-2ae11e65daaa');
    });
  });

  describe('logged in', () => {
    beforeEach(async () => {
      await goto('localhost:8080/');
      await click('Got it!');
      await click('Log in');
      await authenticateViaTwitter();
    });

    it('log out from the article page returns to the article page', async () => {
      await goto('localhost:8080/articles/10.1101/2020.07.13.199174');
      await click('Log out');

      const result = await currentURL();

      expect(result).toContain('/articles/10.1101/2020.07.13.199174');
    });

    it('respond command returns to review fragment on the article page', async () => {
      await goto('localhost:8080/articles/10.1101/2020.07.13.199174');
      await click($('.article-feed__item:first-child button[value="respond-not-helpful"]'));

      const result = await currentURL();

      expect(result).toMatch(/\/articles\/10\.1101\/2020\.07\.13\.199174#(hypothesis|doi):/);
    });

    it('follow command from the editorial community page returns to the editorial community page', async () => {
      await goto('localhost:8080/editorial-communities/10360d97-bf52-4aef-b2fa-2f60d319edd7');
      await click('Follow');

      const result = await currentURL();

      expect(result).toContain('/editorial-communities/10360d97-bf52-4aef-b2fa-2f60d319edd7');
    });

    it('back button doesn\'t break authentication', async () => {
      await goBack();

      const result = await currentURL();

      expect(result).not.toContain('/twitter/callback');
    });
  });
});
