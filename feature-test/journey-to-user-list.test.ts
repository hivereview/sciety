import dotenv from 'dotenv';
import {
  $, click, goto, openBrowser,
} from 'taiko';
import { screenshotTeardown } from './utilities';

describe('journey-to-user-list', () => {
  beforeEach(async () => {
    dotenv.config();
    await openBrowser();
  });

  afterEach(screenshotTeardown);

  describe('when logged in', () => {
    beforeEach(async () => {
      await goto('localhost:8080/');
      await click('Log in');
    });

    it('navigates to user list page via user page', async () => {
      await goto('localhost:8080/my-feed');
      await click('My profile');
      await click('Saved articles');
      const pageTitle = await $('h1').text();

      expect(pageTitle).toContain('Saved Articles');
    });

    it('navigates to the saved articles list from an article page', async () => {
      await goto('localhost:8080/articles/activity/10.1101/2021.06.09.21258556');
      await click('Save to my list');
      await click('Saved to my list');
      await click('Saved articles');
      const pageTitle = await $('h1').text();

      expect(pageTitle).toContain('Saved Articles');
    });
  });
});
