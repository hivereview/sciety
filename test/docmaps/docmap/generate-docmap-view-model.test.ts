import { URL } from 'url';
import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import {
  DocmapModel,
  generateDocmapViewModel,
  Ports,
} from '../../../src/docmaps/docmap/generate-docmap-view-model';
import * as DE from '../../../src/types/data-error';
import { inferredSourceUrl } from '../../../src/types/evaluation-locator';
import { arbitraryUri } from '../../helpers';
import { shouldNotBeCalled } from '../../should-not-be-called';
import { arbitraryArticleId } from '../../types/article-id.helper';
import { arbitraryGroupId } from '../../types/group-id.helper';
import { arbitraryGroup } from '../../types/group.helper';
import { arbitraryNcrcId, arbitraryReviewDoi } from '../../types/evaluation-locator.helper';
import { TestFramework, createTestFramework } from '../../framework';
import { RecordedEvaluation } from '../../../src/types/recorded-evaluation';
import { arbitraryRecordedEvaluation } from '../../types/recorded-evaluation.helper';

const indexedGroupId = arbitraryGroupId();
const articleId = arbitraryArticleId();

describe('generate-docmap-view-model', () => {
  let framework: TestFramework;
  let defaultAdapters: Ports;

  beforeEach(async () => {
    framework = createTestFramework();
    defaultAdapters = {
      ...framework.queries,
      ...framework.happyPathThirdParties,
    };
  });

  describe('when there is an evaluation by the selected group', () => {
    const group = arbitraryGroup();
    const evaluation: RecordedEvaluation = {
      ...arbitraryRecordedEvaluation(),
      articleId,
      groupId: group.id,
    };
    let result: DocmapModel;

    beforeEach(async () => {
      await framework.commandHelpers.deprecatedCreateGroup(group);
      await framework.commandHelpers.deprecatedRecordEvaluation(evaluation);
      result = await pipe(
        { articleId, groupId: group.id },
        generateDocmapViewModel(defaultAdapters),
        TE.getOrElse(framework.abortTest('generateDocmapViewModel')),
      )();
    });

    it('includes the article id', async () => {
      expect(result).toStrictEqual(expect.objectContaining({ articleId }));
    });

    it('includes the group', async () => {
      expect(result).toStrictEqual(expect.objectContaining({ group }));
    });
  });

  describe('when there are multiple evaluations by the selected group', () => {
    const group = arbitraryGroup();
    const firstEvaluation = {
      ...arbitraryRecordedEvaluation(),
      articleId,
      groupId: group.id,
    };
    const secondEvaluation = {
      ...arbitraryRecordedEvaluation(),
      articleId,
      groupId: group.id,
    };

    let result: DocmapModel;

    beforeEach(async () => {
      await framework.commandHelpers.deprecatedCreateGroup(group);
      await framework.commandHelpers.deprecatedRecordEvaluation(firstEvaluation);
      await framework.commandHelpers.deprecatedRecordEvaluation(secondEvaluation);
      result = await pipe(
        generateDocmapViewModel(defaultAdapters)({ articleId, groupId: group.id }),
        TE.getOrElse(framework.abortTest('generateDocmapViewModel')),
      )();
    });

    it('returns all evaluations', () => {
      expect(result.evaluations).toStrictEqual([
        expect.objectContaining({
          evaluationLocator: firstEvaluation.evaluationLocator,
        }),
        expect.objectContaining({
          evaluationLocator: secondEvaluation.evaluationLocator,
        }),
      ]);
    });
  });

  describe('when we can infer a source URL for the evaluations', () => {
    const evaluationLocatorWithInferrableSourceUrl = arbitraryReviewDoi();
    const sourceUrl = pipe(
      inferredSourceUrl(evaluationLocatorWithInferrableSourceUrl),
      O.getOrElseW(shouldNotBeCalled),
    );
    let result: DocmapModel;

    beforeEach(async () => {
      const group = arbitraryGroup();
      const evaluation: RecordedEvaluation = {
        ...arbitraryRecordedEvaluation(),
        groupId: group.id,
        evaluationLocator: evaluationLocatorWithInferrableSourceUrl,
        articleId,
      };
      const ports: Ports = {
        ...defaultAdapters,
        fetchReview: shouldNotBeCalled,
      };
      await framework.commandHelpers.deprecatedCreateGroup(group);
      await framework.commandHelpers.deprecatedRecordEvaluation(evaluation);
      result = await pipe(
        generateDocmapViewModel(ports)({ articleId, groupId: group.id }),
        TE.getOrElse(framework.abortTest('generateDocmapViewModel')),
      )();
    });

    it('returns the inferred source URL rather than calling the port', () => {
      expect(result.evaluations).toStrictEqual([
        expect.objectContaining({
          sourceUrl,
        }),
      ]);
    });
  });

  describe('when we cannot infer a source URL for the evaluations', () => {
    const evaluationLocatorWithUninferrableSourceUrl = arbitraryNcrcId();
    const sourceUrl = new URL(arbitraryUri());
    let result: DocmapModel;

    beforeEach(async () => {
      const group = arbitraryGroup();
      const evaluation: RecordedEvaluation = {
        ...arbitraryRecordedEvaluation(),
        groupId: group.id,
        evaluationLocator: evaluationLocatorWithUninferrableSourceUrl,
        articleId,
      };
      const ports: Ports = {
        ...defaultAdapters,
        fetchReview: () => TE.right({ url: sourceUrl }),
      };
      await framework.commandHelpers.deprecatedCreateGroup(group);
      await framework.commandHelpers.deprecatedRecordEvaluation(evaluation);
      result = await pipe(
        generateDocmapViewModel(ports)({ articleId, groupId: group.id }),
        TE.getOrElse(framework.abortTest('generateDocmapViewModel')),
      )();
    });

    it('obtains the source URL by calling the port', () => {
      expect(result.evaluations).toStrictEqual([
        expect.objectContaining({
          sourceUrl,
        }),
      ]);
    });
  });

  describe('when there are no evaluations by the selected group', () => {
    it('returns an E.left of not-found', async () => {
      const result = await generateDocmapViewModel(defaultAdapters)({ articleId, groupId: indexedGroupId })();

      expect(result).toStrictEqual(E.left('not-found'));
    });
  });

  describe('when there are evaluations by other groups', () => {
    it('only uses the evaluation by the selected group', async () => {
      const group = {
        ...arbitraryGroup(),
        id: indexedGroupId,
      };
      const otherGroup = arbitraryGroup();
      const evaluationByThisGroup: RecordedEvaluation = {
        ...arbitraryRecordedEvaluation(),
        articleId,
        groupId: indexedGroupId,
      };
      await framework.commandHelpers.deprecatedCreateGroup(group);
      await framework.commandHelpers.deprecatedCreateGroup(otherGroup);
      await framework.commandHelpers.deprecatedRecordEvaluation({
        ...arbitraryRecordedEvaluation(),
        groupId: otherGroup.id,
        articleId,
      });
      await framework.commandHelpers.deprecatedRecordEvaluation(evaluationByThisGroup);

      const result = await pipe(
        {
          articleId,
          groupId: indexedGroupId,
        },
        generateDocmapViewModel(defaultAdapters),
        TE.getOrElse(framework.abortTest('generateDocmapViewModel')),
      )();

      expect(result.evaluations).toStrictEqual([
        expect.objectContaining({
          evaluationLocator: evaluationByThisGroup.evaluationLocator,
        }),
      ]);
    });
  });

  describe('when there is a single evaluation by the selected group', () => {
    const group = arbitraryGroup();
    const evaluation: RecordedEvaluation = {
      ...arbitraryRecordedEvaluation(),
      groupId: indexedGroupId,
      articleId,
    };
    let result: DocmapModel;

    beforeEach(async () => {
      await framework.commandHelpers.deprecatedCreateGroup({ ...group, id: indexedGroupId });
      await framework.commandHelpers.deprecatedRecordEvaluation(evaluation);
      result = await pipe(
        {
          articleId,
          groupId: indexedGroupId,
        },
        generateDocmapViewModel(defaultAdapters),
        TE.getOrElse(framework.abortTest('generateDocmapViewModel')),
      )();
    });

    it('returns O.none for the input published date', async () => {
      expect(result.inputPublishedDate).toStrictEqual(O.none);
    });
  });

  describe('when the group cannot be retrieved', () => {
    let result: E.Either<DE.DataError, DocmapModel>;

    beforeEach(async () => {
      result = await pipe(
        {
          articleId,
          groupId: indexedGroupId,
        },
        generateDocmapViewModel(defaultAdapters),
      )();
    });

    it('returns not-found', async () => {
      expect(result).toStrictEqual(E.left('not-found'));
    });
  });
});
