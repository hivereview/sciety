import { pipe } from 'fp-ts/function';
import * as E from 'fp-ts/Either';
import * as RA from 'fp-ts/ReadonlyArray';
import * as PH from '../../src/types/publishing-history.js';
import { findAllListsContainingPaper } from '../../src/read-side/find-all-lists-containing-paper.js';
import { ArticleId } from '../../src/types/article-id.js';
import { ListId } from '../../src/types/list-id.js';
import { TestFramework, createTestFramework } from '../framework/index.js';
import { shouldNotBeCalled } from '../should-not-be-called.js';
import { arbitraryExpressionDoi } from '../types/expression-doi.helper.js';
import { arbitraryPaperExpression } from '../types/paper-expression.helper.js';
import { arbitraryPublishingHistoryOnlyPreprints } from '../types/publishing-history.helper.js';
import { arbitraryCreateListCommand } from '../write-side/commands/create-list-command.helper.js';

describe('find-all-lists-containing-paper', () => {
  let framework: TestFramework;
  const expressionDoi = arbitraryExpressionDoi();
  const articleId = new ArticleId(expressionDoi);

  beforeEach(() => {
    framework = createTestFramework();
  });

  let result: ReadonlyArray<ListId>;

  describe('when the paper is not in any list', () => {
    beforeEach(() => {
      result = pipe(
        arbitraryPublishingHistoryOnlyPreprints({ earliestExpressionDoi: expressionDoi }),
        findAllListsContainingPaper(framework.dependenciesForViews),
        RA.map((list) => list.id),
      );
    });

    it('returns empty', () => {
      expect(result).toStrictEqual([]);
    });
  });

  describe('when the paper is in a list', () => {
    const createList = arbitraryCreateListCommand();

    beforeEach(async () => {
      await framework.commandHelpers.createList(createList);
      await framework.commandHelpers.addArticleToList(articleId, createList.listId);
      result = pipe(
        arbitraryPublishingHistoryOnlyPreprints({ earliestExpressionDoi: expressionDoi }),
        findAllListsContainingPaper(framework.dependenciesForViews),
        RA.map((list) => list.id),
      );
    });

    it('returns the list id', () => {
      expect(result).toStrictEqual([createList.listId]);
    });
  });

  describe('when two different expressions of the paper are in two different lists', () => {
    const createList1 = arbitraryCreateListCommand();
    const createList2 = arbitraryCreateListCommand();
    const expressionDoi2 = arbitraryExpressionDoi();
    const publishingHistory: PH.PublishingHistory = pipe(
      [
        {
          ...arbitraryPaperExpression(),
          expressionDoi,
        },
        {
          ...arbitraryPaperExpression(),
          expressionDoi: expressionDoi2,
        },
      ],
      PH.fromExpressions,
      E.getOrElseW(shouldNotBeCalled),
    );

    beforeEach(async () => {
      await framework.commandHelpers.createList(createList1);
      await framework.commandHelpers.createList(createList2);
      await framework.commandHelpers.addArticleToList(new ArticleId(expressionDoi), createList1.listId);
      await framework.commandHelpers.addArticleToList(new ArticleId(expressionDoi2), createList2.listId);
      result = pipe(
        publishingHistory,
        findAllListsContainingPaper(framework.dependenciesForViews),
        RA.map((list) => list.id),
      );
    });

    it('returns both lists', () => {
      expect(result).toHaveLength(2);
      expect(result).toContain(createList1.listId);
      expect(result).toContain(createList2.listId);
    });
  });

  describe('when two different expressions of the paper are in the same list', () => {
    const createList1 = arbitraryCreateListCommand();
    const expressionDoi2 = arbitraryExpressionDoi();
    const publishingHistory: PH.PublishingHistory = pipe(
      [
        {
          ...arbitraryPaperExpression(),
          expressionDoi,
        },
        {
          ...arbitraryPaperExpression(),
          expressionDoi: expressionDoi2,
        },
      ],
      PH.fromExpressions,
      E.getOrElseW(shouldNotBeCalled),
    );

    beforeEach(async () => {
      await framework.commandHelpers.createList(createList1);
      await framework.commandHelpers.addArticleToList(new ArticleId(expressionDoi), createList1.listId);
      await framework.commandHelpers.addArticleToList(new ArticleId(expressionDoi2), createList1.listId);
      result = pipe(
        publishingHistory,
        findAllListsContainingPaper(framework.dependenciesForViews),
        RA.map((list) => list.id),
      );
    });

    it('returns the list only once', () => {
      expect(result).toHaveLength(1);
      expect(result).toContain(createList1.listId);
    });
  });
});
