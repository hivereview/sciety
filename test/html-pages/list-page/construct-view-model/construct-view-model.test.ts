import * as E from 'fp-ts/Either';
import { pipe } from 'fp-ts/function';
import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import * as TE from 'fp-ts/TaskEither';
import { shouldNotBeCalled } from '../../../should-not-be-called';
import { hasContentWithPagination, ViewModel } from '../../../../src/html-pages/list-page/view-model';
import { constructViewModel } from '../../../../src/html-pages/list-page/construct-view-model/construct-view-model';
import { createTestFramework, TestFramework } from '../../../framework';
import * as LOID from '../../../../src/types/list-owner-id';
import { arbitraryCreateUserAccountCommand } from '../../../write-side/commands/create-user-account-command.helper';
import { arbitraryExpressionDoi } from '../../../types/expression-doi.helper';
import { ArticleId } from '../../../../src/types/article-id';

describe('construct-view-model', () => {
  let framework: TestFramework;

  beforeEach(() => {
    framework = createTestFramework();
  });

  describe('when a user saves a paper that is not in any list', () => {
    let viewModel: ViewModel;
    const expressionDoi = arbitraryExpressionDoi();

    beforeEach(async () => {
      const createUserAccountCommand = arbitraryCreateUserAccountCommand();
      await framework.commandHelpers.createUserAccount(createUserAccountCommand);
      const list = framework.queries.selectAllListsOwnedBy(LOID.fromUserId(createUserAccountCommand.userId))[0];
      const listId = list.id;
      await framework.commandHelpers.addArticleToList(new ArticleId(expressionDoi), listId);
      viewModel = await pipe(
        {
          page: 1,
          id: listId,
          user: O.some({ id: createUserAccountCommand.userId }),
          success: false,
        },
        constructViewModel(framework.dependenciesForViews),
        TE.getOrElse(shouldNotBeCalled),
      )();
    });

    it('the paper\'s details are included in the page content', () => {
      expect(viewModel.content).toStrictEqual(expect.objectContaining({
        articles: [
          E.right(expect.objectContaining({
            articleCard: expect.objectContaining({
              paperActivityPageHref: expect.stringContaining(expressionDoi),
            }),
          })),
        ],
      }));
    });

    it('displays a link to related papers', () => {
      expect(viewModel.relatedArticlesLink).toStrictEqual(O.some(expect.anything()));
    });
  });

  describe('ordering of list contents', () => {
    const createList = async () => {
      const createUserAccountCommand = arbitraryCreateUserAccountCommand();
      await framework.commandHelpers.createUserAccount(createUserAccountCommand);
      const list = framework.queries.selectAllListsOwnedBy(LOID.fromUserId(createUserAccountCommand.userId))[0];
      return list.id;
    };

    describe('when the list contains two papers', () => {
      const expressionDoi1 = arbitraryExpressionDoi();
      const expressionDoi2 = arbitraryExpressionDoi();
      let viewModel: ViewModel;

      beforeEach(async () => {
        const listId = await createList();
        await framework.commandHelpers.addArticleToList(new ArticleId(expressionDoi1), listId);
        await framework.commandHelpers.addArticleToList(new ArticleId(expressionDoi2), listId);
        viewModel = await pipe(
          {
            page: 1,
            id: listId,
            user: O.none,
            success: false,
          },
          constructViewModel(framework.dependenciesForViews),
          TE.getOrElse(shouldNotBeCalled),
        )();
      });

      it('sorts the papers in reverse order of being added to the list', () => {
        expect(viewModel.content).toStrictEqual(expect.objectContaining({
          articles: [
            E.right(expect.objectContaining({
              articleCard: expect.objectContaining({ paperActivityPageHref: expect.stringContaining(expressionDoi2) }),
            })),
            E.right(expect.objectContaining({
              articleCard: expect.objectContaining({ paperActivityPageHref: expect.stringContaining(expressionDoi1) }),
            })),
          ],
        }));
      });
    });

    describe('when the list contains a paper that has been removed and re-added', () => {
      let viewModel: ViewModel;
      const expressionDoi1 = arbitraryExpressionDoi();
      const expressionDoi2 = arbitraryExpressionDoi();

      beforeEach(async () => {
        const listId = await createList();
        await framework.commandHelpers.addArticleToList(new ArticleId(expressionDoi1), listId);
        await framework.commandHelpers.addArticleToList(new ArticleId(expressionDoi2), listId);
        await framework.commandHelpers.removeArticleFromList(new ArticleId(expressionDoi1), listId);
        await framework.commandHelpers.addArticleToList(new ArticleId(expressionDoi1), listId);
        viewModel = await pipe(
          {
            page: 1,
            id: listId,
            user: O.none,
            success: false,
          },
          constructViewModel(framework.dependenciesForViews),
          TE.getOrElse(shouldNotBeCalled),
        )();
      });

      it('sorts the papers in reverse order of being added to the list', () => {
        expect(viewModel.content).toStrictEqual(expect.objectContaining({
          articles: [
            E.right(expect.objectContaining({
              articleCard: expect.objectContaining({ paperActivityPageHref: expect.stringContaining(expressionDoi1) }),
            })),
            E.right(expect.objectContaining({
              articleCard: expect.objectContaining({ paperActivityPageHref: expect.stringContaining(expressionDoi2) }),
            })),
          ],
        }));
      });
    });

    describe('when a paper has been removed from the list', () => {
      const expressionDoi1 = arbitraryExpressionDoi();
      const expressionDoi2 = arbitraryExpressionDoi();
      const expressionDoi3 = arbitraryExpressionDoi();
      let viewModel: ViewModel;
      let result: ReadonlyArray<string>;

      beforeEach(async () => {
        const listId = await createList();
        await framework.commandHelpers.addArticleToList(new ArticleId(expressionDoi1), listId);
        await framework.commandHelpers.addArticleToList(new ArticleId(expressionDoi2), listId);
        await framework.commandHelpers.addArticleToList(new ArticleId(expressionDoi3), listId);
        await framework.commandHelpers.removeArticleFromList(new ArticleId(expressionDoi3), listId);
        viewModel = await pipe(
          {
            page: 1,
            id: listId,
            user: O.none,
            success: false,
          },
          constructViewModel(framework.dependenciesForViews),
          TE.getOrElse(shouldNotBeCalled),
        )();
        result = pipe(
          viewModel.content,
          O.some,
          O.filter(hasContentWithPagination),
          O.getOrElseW(shouldNotBeCalled),
          (content) => content.articles,
          RA.map(E.getOrElseW(shouldNotBeCalled)),
          RA.map((model) => model.articleCard.paperActivityPageHref),
        );
      });

      it('sorts the remaining papers in reverse order of being added to the list', () => {
        expect(result).toHaveLength(2);
        expect(result[0]).toContain(expressionDoi2);
        expect(result[1]).toContain(expressionDoi1);
      });
    });
  });
});
