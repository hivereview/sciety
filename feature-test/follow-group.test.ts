import dotenv from 'dotenv';
import {
  $, click, goto, openBrowser, text, within,
} from 'taiko';
import { authenticateViaTwitter, screenshotTeardown } from './utilities';

describe('follow a group', () => {
  beforeEach(async () => {
    dotenv.config();
    await openBrowser();
  });

  afterEach(screenshotTeardown);

  describe('when logged in', () => {
    beforeEach(async () => {
      await goto('localhost:8080/');
      await click('Log in');
      await authenticateViaTwitter();
    });

    describe('from the PeerJ group page', () => {
      it('adds the group to my profile page', async () => {
        await goto('localhost:8080/groups');
        await click('PeerJ');
        await click('Follow');
        await click('My profile');
        await click('Following');
        const groupExists = await text('PeerJ', within($('.followed-groups-list'))).exists();

        expect(groupExists).toBe(true);
      });
    });
  });
});
