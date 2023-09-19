import * as O from 'fp-ts/Option';
import { TestFramework, createTestFramework } from '../../../framework';
import * as LOID from '../../../../src/types/list-owner-id';
import { List } from '../../../../src/types/list';
import { arbitraryUserDetails } from '../../../types/user-details.helper';
import { constructViewModel } from '../../../../src/html-pages/lists-page/construct-view-model/construct-view-model';
import { ViewModel } from '../../../../src/html-pages/lists-page/view-model';
import { arbitraryArticleId } from '../../../types/article-id.helper';
import { dummyLogger } from '../../../dummy-logger';
import { arbitraryUserId } from '../../../types/user-id.helper';
import { degradedAvatarUrl } from '../../../../src/shared-components/list-card/construct-list-card-view-model-with-avatar';
import { arbitraryCreateListCommand } from '../../../write-side/commands/create-list-command.helper';

describe('construct-view-model', () => {
  let framework: TestFramework;
  const user = arbitraryUserDetails();

  beforeEach(() => {
    framework = createTestFramework();
  });

  describe('when there are two populated user lists', () => {
    let initialUserList: List;
    const command = {
      ...arbitraryCreateListCommand(),
      ownerId: LOID.fromUserId(user.id),
    };
    let viewmodel: ViewModel;

    beforeEach(async () => {
      await framework.commandHelpers.deprecatedCreateUserAccount(user);
      initialUserList = framework.queries.selectAllListsOwnedBy(LOID.fromUserId(user.id))[0];
      await framework.commandHelpers.addArticleToList(arbitraryArticleId(), initialUserList.id);
      await framework.commandHelpers.createList(command);
      await framework.commandHelpers.addArticleToList(arbitraryArticleId(), command.listId);

      viewmodel = constructViewModel({ ...framework.queries, logger: dummyLogger });
    });

    it('the user avatar is included in each card', () => {
      expect(viewmodel).toStrictEqual([
        expect.objectContaining({ avatarUrl: O.some(user.avatarUrl) }),
        expect.objectContaining({ avatarUrl: O.some(user.avatarUrl) }),
      ]);
    });

    it('the most recently updated list is shown first', async () => {
      expect(viewmodel).toStrictEqual([
        expect.objectContaining({ listId: command.listId }),
        expect.objectContaining({ listId: initialUserList.id }),
      ]);
    });
  });

  describe('when there is a populated user list', () => {
    describe('when the user information cannot be retrieved', () => {
      const createListCommand = {
        ...arbitraryCreateListCommand(),
        ownerId: LOID.fromUserId(arbitraryUserId()),
      };
      let viewmodel: ViewModel;

      beforeEach(async () => {
        await framework.commandHelpers.createList(createListCommand);
        await framework.commandHelpers.addArticleToList(arbitraryArticleId(), createListCommand.listId);

        viewmodel = constructViewModel({ ...framework.queries, logger: dummyLogger });
      });

      it('returns a degraded avatarUrl in place of the list owner avatarUrl', () => {
        expect(viewmodel).toStrictEqual([
          expect.objectContaining({ avatarUrl: O.some(degradedAvatarUrl) }),
        ]);
      });
    });
  });
});
