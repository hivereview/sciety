import * as O from 'fp-ts/Option';

import { pipe } from 'fp-ts/function';
import * as LOID from '../../../src/types/list-owner-id';
import { arbitraryCreateUserAccountCommand } from '../../write-side/commands/create-user-account-command.helper';
import { TestFramework, createTestFramework } from '../../framework';
import {
  constructAnnotation,
} from '../../../src/shared-components/article-card-with-controls-and-annotation/construct-annotation';
import { arbitraryArticleId } from '../../types/article-id.helper';
import { arbitraryListId } from '../../types/list-id.helper';
import { shouldNotBeCalled } from '../../should-not-be-called';
import { arbitraryCreateListCommand } from '../../write-side/commands/create-list-command.helper';
import { arbitraryHtmlFragment } from '../../helpers';
import { HtmlFragment } from '../../../src/types/html-fragment';
import { arbitraryAddGroupCommand } from '../../write-side/commands/add-group-command.helper';
import { unknownAuthor } from '../../../src/shared-components/article-card-with-controls-and-annotation/static-content';

describe('construct-annotation', () => {
  let framework: TestFramework;

  beforeEach(() => {
    framework = createTestFramework();
  });

  describe('when there is no annotation', () => {
    it('returns none', () => {
      const result = constructAnnotation(framework.dependenciesForViews)(arbitraryListId(), arbitraryArticleId());

      expect(result).toStrictEqual(O.none);
    });
  });

  describe('when there is an annotation', () => {
    const articleId = arbitraryArticleId();
    const content = arbitraryHtmlFragment();
    let result: {
      author: string,
      content: HtmlFragment,
    };

    describe('on a list owned by a user', () => {
      const createUserAccountCommand = arbitraryCreateUserAccountCommand();
      const createListCommand = {
        ...arbitraryCreateListCommand(),
        ownerId: LOID.fromUserId(createUserAccountCommand.userId),
      };
      const target = { listId: createListCommand.listId, articleId };

      beforeEach(async () => {
        await framework.commandHelpers.createUserAccount(createUserAccountCommand);
        await framework.commandHelpers.createList(createListCommand);
        await framework.commandHelpers.addArticleToList(articleId, createListCommand.listId);
        await framework.commandHelpers.createAnnotation({ content, target });
        result = pipe(
          constructAnnotation(framework.dependenciesForViews)(target.listId, target.articleId),
          O.getOrElseW(shouldNotBeCalled),
        );
      });

      it('returns its content', async () => {
        expect(result.content).toStrictEqual(content);
      });

      it('returns its author', () => {
        expect(result.author).toStrictEqual(createUserAccountCommand.displayName);
      });
    });

    describe('on a list owned by a group', () => {
      const addGroupCommand = arbitraryAddGroupCommand();
      const createListCommand = {
        ...arbitraryCreateListCommand(),
        ownerId: LOID.fromGroupId(addGroupCommand.groupId),
      };
      const target = { listId: createListCommand.listId, articleId };

      beforeEach(async () => {
        await framework.commandHelpers.addGroup(addGroupCommand);
        await framework.commandHelpers.createList(createListCommand);
        await framework.commandHelpers.addArticleToList(articleId, createListCommand.listId);
        await framework.commandHelpers.createAnnotation({ content, target });
        result = pipe(
          constructAnnotation(framework.dependenciesForViews)(target.listId, target.articleId),
          O.getOrElseW(shouldNotBeCalled),
        );
      });

      it('returns its content', () => {
        expect(result.content).toStrictEqual(content);
      });

      it('returns the group name as the author', () => {
        expect(result.author).toStrictEqual(addGroupCommand.name);
      });
    });

    describe('but the list owner information is not available', () => {
      const listId = arbitraryListId();
      const target = { listId, articleId };

      beforeEach(async () => {
        await framework.commandHelpers.createAnnotation({ content, target });
        result = pipe(
          constructAnnotation(framework.dependenciesForViews)(target.listId, target.articleId),
          O.getOrElseW(shouldNotBeCalled),
        );
      });

      it('returns its content', () => {
        expect(result.content).toStrictEqual(content);
      });

      it('returns a static value as the author', () => {
        expect(result.author).toBe(unknownAuthor);
      });
    });
  });
});
