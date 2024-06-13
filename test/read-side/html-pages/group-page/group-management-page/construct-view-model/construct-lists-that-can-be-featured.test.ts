import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import { pipe } from 'fp-ts/function';
import { constructListsThatCanBeFeatured } from '../../../../../../src/read-side/html-pages/group-page/group-management-page/construct-view-model/construct-lists-that-can-be-featured';
import { Group } from '../../../../../../src/types/group';
import { ListId } from '../../../../../../src/types/list-id';
import * as LOID from '../../../../../../src/types/list-owner-id';
import { CreateListCommand } from '../../../../../../src/write-side/commands';
import { TestFramework, createTestFramework } from '../../../../../framework';
import { shouldNotBeCalled } from '../../../../../should-not-be-called';
import { arbitraryUserId } from '../../../../../types/user-id.helper';
import { arbitraryAddGroupCommand } from '../../../../../write-side/commands/add-group-command.helper';
import { arbitraryCreateListCommand } from '../../../../../write-side/commands/create-list-command.helper';

describe('construct-lists-that-can-be-featured', () => {
  let framework: TestFramework;

  beforeEach(() => {
    framework = createTestFramework();
  });

  describe('given a group', () => {
    let group: Group;

    beforeEach(async () => {
      const addGroupCommand = arbitraryAddGroupCommand();
      await framework.commandHelpers.addGroup(addGroupCommand);
      group = pipe(
        addGroupCommand.groupId,
        framework.queries.getGroup,
        O.getOrElseW(shouldNotBeCalled),
      );
    });

    describe('given a list owned by the current user', () => {
      const currentUserId = arbitraryUserId();
      const listOwnerId = LOID.fromUserId(currentUserId);
      const createListCommand: CreateListCommand = {
        ...arbitraryCreateListCommand(),
        ownerId: listOwnerId,
      };
      const listId = createListCommand.listId;

      beforeEach(async () => {
        await framework.commandHelpers.createList(createListCommand);
      });

      describe('and is currently featured by this group', () => {
        it.todo('is not included');
      });

      describe('and is not currently featured by this group', () => {
        let availableListIds: ReadonlyArray<ListId>;

        beforeEach(() => {
          availableListIds = pipe(
            constructListsThatCanBeFeatured(framework.dependenciesForViews, group),
            RA.map((listThatCanBeFeatured) => listThatCanBeFeatured.listId),
          );
        });

        it.failing('is included', () => {
          expect(availableListIds).toContain(listId);
        });
      });

      describe('and was featured by this group, but is not longer featured', () => {
        it.todo('is included');
      });

      describe('and is currently featured only by a different group', () => {
        it.todo('is included');
      });
    });

    describe('given a list owned by a different user', () => {
      it.todo('is not included');
    });

    describe('given a list owned by the group', () => {
      it.todo('is not included');
    });

    describe('given a list owned by a different group', () => {
      it.todo('is not included');
    });
  });
});
