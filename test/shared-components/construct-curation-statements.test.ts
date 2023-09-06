import { URL } from 'url';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import * as T from 'fp-ts/Task';
import * as RA from 'fp-ts/ReadonlyArray';
import { arbitraryAddGroupCommand } from '../write-side/commands/add-group-command.helper';
import { createTestFramework, TestFramework } from '../framework';
import {
  constructCurationStatements,
} from '../../src/shared-components/construct-curation-statements';
import * as DE from '../../src/types/data-error';
import { arbitraryArticleId } from '../types/article-id.helper';
import { arbitraryEvaluationLocator } from '../types/evaluation-locator.helper';
import { EvaluationLocator } from '../../src/types/evaluation-locator';
import { arbitrarySanitisedHtmlFragment, arbitraryUri } from '../helpers';
import { arbitraryRecordEvaluationPublicationCommand } from '../write-side/commands/record-evaluation-publication-command.helper';
import { Dependencies } from '../../src/html-pages/article-page/construct-view-model/dependencies';

describe('construct-curation-statements', () => {
  let framework: TestFramework;

  beforeEach(() => {
    framework = createTestFramework();
  });

  const articleId = arbitraryArticleId();
  const addGroupA = arbitraryAddGroupCommand();
  const recordCurationStatementByGroupA = {
    ...arbitraryRecordEvaluationPublicationCommand(),
    groupId: addGroupA.groupId,
    articleId,
    evaluationType: 'curation-statement' as const,
  };

  const getEvaluationLocator = async (dependencies: Dependencies) => pipe(
    constructCurationStatements(dependencies, articleId),
    T.map(RA.map((curationStatements) => curationStatements.evaluationLocator)),
  )();

  let result: ReadonlyArray<EvaluationLocator>;

  describe('when there are multiple curation statements but only one of the groups exists', () => {
    const evaluation2Command = {
      ...arbitraryRecordEvaluationPublicationCommand(),
      articleId,
      evaluationType: 'curation-statement' as const,
    };

    beforeEach(async () => {
      await framework.commandHelpers.addGroup(addGroupA);
      await framework.commandHelpers.recordEvaluationPublication(recordCurationStatementByGroupA);
      await framework.commandHelpers.recordEvaluationPublication(evaluation2Command);
      result = await getEvaluationLocator(framework.dependenciesForViews);
    });

    it('only returns the curation statement by the existing group', () => {
      expect(result).toStrictEqual([recordCurationStatementByGroupA.evaluationLocator]);
    });
  });

  describe('when no curation statements can be retrieved', () => {
    beforeEach(async () => {
      await framework.commandHelpers.addGroup(addGroupA);
      await framework.commandHelpers.recordEvaluationPublication(recordCurationStatementByGroupA);
      result = await getEvaluationLocator({
        ...framework.dependenciesForViews,
        fetchReview: () => TE.left(DE.unavailable),
      });
    });

    it('returns no curation statements', () => {
      expect(result).toStrictEqual([]);
    });
  });

  describe('when one curation statement can be retrieved and one cannot', () => {
    const addGroupB = arbitraryAddGroupCommand();
    const retrievableEvaluationLocator = arbitraryEvaluationLocator();
    const recordRetrievableCurationStatementByGroupB = {
      ...arbitraryRecordEvaluationPublicationCommand(),
      evaluationLocator: retrievableEvaluationLocator,
      groupId: addGroupB.groupId,
      articleId,
      evaluationType: 'curation-statement' as const,
    };

    beforeEach(async () => {
      await framework.commandHelpers.addGroup(addGroupA);
      await framework.commandHelpers.addGroup(addGroupB);
      await framework.commandHelpers.recordEvaluationPublication(recordRetrievableCurationStatementByGroupB);
      await framework.commandHelpers.recordEvaluationPublication(recordCurationStatementByGroupA);
      result = await getEvaluationLocator({
        ...framework.dependenciesForViews,
        fetchReview: (evaluationLocator: EvaluationLocator) => (evaluationLocator === retrievableEvaluationLocator
          ? TE.right({
            url: new URL(arbitraryUri()),
            fullText: arbitrarySanitisedHtmlFragment(),
          })
          : TE.left(DE.unavailable)),
      });
    });

    it('only returns the retrievable curation statement', () => {
      expect(result).toStrictEqual([retrievableEvaluationLocator]);
    });
  });

  describe('when there are multiple curation statements by the same group', () => {
    const latestEvaluationLocator = arbitraryEvaluationLocator();
    const recordLaterCurationStatementByGroupA = {
      ...recordCurationStatementByGroupA,
      evaluationLocator: latestEvaluationLocator,
      publishedAt: new Date('2023-08-05'),
    };

    beforeEach(async () => {
      await framework.commandHelpers.addGroup(addGroupA);
      await framework.commandHelpers.recordEvaluationPublication(recordCurationStatementByGroupA);
      await framework.commandHelpers.recordEvaluationPublication(recordLaterCurationStatementByGroupA);
      result = await getEvaluationLocator(framework.dependenciesForViews);
    });

    it('includes only the latest curation statement', () => {
      expect(result).toStrictEqual([latestEvaluationLocator]);
    });
  });
});
