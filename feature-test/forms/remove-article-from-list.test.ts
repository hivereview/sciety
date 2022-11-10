import {
  $,
  click, closeBrowser,
  currentURL,
  goto,
  openBrowser,
} from 'taiko';
import { arbitraryArticleId } from '../../test/types/article-id.helper';
import { getFirstListOwnedBy } from '../get-first-list-owned-by.helper';

describe('remove-article-from-list', () => {
  beforeAll(async () => {
    await openBrowser();
  });

  afterAll(async () => {
    await closeBrowser();
  });

  describe('when the user is logged in', () => {
    const testUserId = '1302891230918053888';

    beforeAll(async () => {
      await goto(`localhost:8080/log-in-as?userId=${testUserId}`);
    });

    describe('and has saved an article', () => {
      const articleId = arbitraryArticleId().value;
      const articlePage = `localhost:8080/articles/activity/${articleId}`;

      beforeAll(async () => {
        await goto(articlePage);
        await click('Save to my list');
      });

      describe('and they click the trash can', () => {
        const contentSelector = 'main';
        let genericListPage: string;
        let content: string;

        beforeAll(async () => {
          const listId = await getFirstListOwnedBy(testUserId);
          genericListPage = `localhost:8080/lists/${listId}`;
          await goto(genericListPage);
          const articleCardDeleteButtonSelector = '.article-card form[action="/forms/remove-article-from-list"]';
          const deleteButton = $(articleCardDeleteButtonSelector);
          await click(deleteButton);
        });

        it('they should be redirected to the generic list page', async () => {
          expect(await currentURL()).toContain(genericListPage);
        });

        it('the article should no longer be in the list', async () => {
          await goto(genericListPage);
          content = await $(contentSelector).text();

          expect(content).toContain('This list is currently empty. Try coming back later!');
        });
      });
    });
  });
});
