/* eslint-disable no-console */
import {
  click, currentURL, goto, openBrowser, openTab, switchTo,
} from 'taiko';
import { screenshotTeardown } from '../utilities';
import { completeLoginViaStubWithSpecifiedUserId } from '../helpers/complete-login-via-stub-with-specified-user-id';
import * as api from '../helpers/api-helpers';
import { arbitraryCreateUserAccountCommand } from '../../test/write-side/commands/create-user-account-command.helper';

describe('forced-login-from-outdated-page', () => {
  const createUserAccountCommand = arbitraryCreateUserAccountCommand();

  beforeAll(async () => {
    await api.createUser(createUserAccountCommand);
  });

  beforeEach(async () => {
    await openBrowser();
  });

  afterEach(screenshotTeardown);

  describe('when I am on the article page and I log in successfully', () => {
    const articleId = '10.1101/2022.09.23.22280264';
    const articlePage = `localhost:8080/articles/activity/${articleId}`;

    beforeEach(async () => {
      await goto(articlePage);
      await click('Log In');
      await completeLoginViaStubWithSpecifiedUserId(createUserAccountCommand.userId);
    });

    describe('when I log out from another tab', () => {
      beforeEach(async () => {
        await openTab('localhost:8080/');
        await click('Log out');
      });

      describe.skip('when I go back to the original tab and I attempt to save the article', () => {
        beforeEach(async () => {
          await switchTo(new RegExp(articlePage));
          console.log(await currentURL());
          await click('Save article');
        });

        describe('when I log back in again', () => {
          beforeEach(async () => {
            await completeLoginViaStubWithSpecifiedUserId(createUserAccountCommand.userId);
          });

          it('i am still on the article page', async () => {
            const result = await currentURL();

            expect(result).toContain(articlePage);
          });
        });
      });
    });
  });
});
