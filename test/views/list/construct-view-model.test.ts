import * as RA from 'fp-ts/ReadonlyArray';
import * as TE from 'fp-ts/TaskEither';
import * as T from 'fp-ts/Task';
import { pipe } from 'fp-ts/function';
import { shouldNotBeCalled } from '../../should-not-be-called';
import { arbitraryArticleId } from '../../types/article-id.helper';
import { createTestFramework, TestFramework } from '../../framework';
import { arbitraryUserDetails } from '../../types/user-details.helper';
import * as LOID from '../../../src/types/list-owner-id';
import { constructViewModel } from '../../../src/views/list/construct-view-model';
import { Doi } from '../../../src/types/doi';

describe('construct-view-model', () => {
  let framework: TestFramework;

  beforeEach(() => {
    framework = createTestFramework();
  });

  describe('when the list contains two articles', () => {
    const articleId1 = arbitraryArticleId();
    const articleId2 = arbitraryArticleId();
    let orderedArticleIds: ReadonlyArray<Doi>;
    const createList = async () => {
      const userDetails = arbitraryUserDetails();
      await framework.commandHelpers.deprecatedCreateUserAccount(userDetails);
      const list = framework.queries.selectAllListsOwnedBy(LOID.fromUserId(userDetails.id))[0];
      return list.id;
    };

    beforeEach(async () => {
      const listId = await createList();
      await framework.commandHelpers.addArticleToList(articleId1, listId);
      await framework.commandHelpers.addArticleToList(articleId2, listId);
      orderedArticleIds = await pipe(
        { id: listId },
        constructViewModel(framework.dependenciesForViews),
        TE.getOrElse(shouldNotBeCalled),
        T.map((viewModel) => viewModel.articles),
        T.map(RA.map((article) => article.articleId)),
      )();
    });

    it('sorts the articles in reverse order of being added to the list', () => {
      expect(orderedArticleIds).toStrictEqual([articleId2, articleId1]);
    });
  });
});
