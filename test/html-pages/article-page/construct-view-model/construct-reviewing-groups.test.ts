import { pipe } from 'fp-ts/function';
import * as RA from 'fp-ts/ReadonlyArray';
import { arbitraryString } from '../../../helpers';
import { createTestFramework, TestFramework } from '../../../framework';
import { arbitraryAddGroupCommand } from '../../../write-side/commands/add-group-command.helper';
import {
  arbitraryRecordEvaluationPublicationCommand,
} from '../../../write-side/commands/record-evaluation-publication-command.helper';
import { arbitraryArticleId } from '../../../types/article-id.helper';
import {
  constructReviewingGroups,
} from '../../../../src/html-pages/article-page/construct-view-model/construct-reviewing-groups';
import { ViewModel } from '../../../../src/html-pages/article-page/view-model';

describe('construct-reviewing-groups', () => {
  const article = arbitraryArticleId();
  let framework: TestFramework;

  beforeEach(() => {
    framework = createTestFramework();
  });

  describe('which groups to include', () => {
    const getReviewingGroupNames = () => pipe(
      constructReviewingGroups(framework.dependenciesForViews, article),
      RA.map((reviewingGroup) => reviewingGroup.name),
    );

    let result: ReadonlyArray<string>;

    describe('when an article has been reviewed by one group once', () => {
      const reviewingGroupName = arbitraryString();
      const addGroupCommand = {
        ...arbitraryAddGroupCommand(),
        name: reviewingGroupName,
      };

      beforeEach(async () => {
        await framework.commandHelpers.addGroup(addGroupCommand);
        await framework.commandHelpers.recordEvaluationPublication({
          ...arbitraryRecordEvaluationPublicationCommand(),
          articleId: article,
          groupId: addGroupCommand.groupId,
          evaluationType: 'review',
        });
        result = getReviewingGroupNames();
      });

      it('returns an array containing the group\'s name once', () => {
        expect(result).toStrictEqual([reviewingGroupName]);
      });
    });

    describe('when an article has been reviewed by one group twice', () => {
      const reviewingGroupName = arbitraryString();
      const addGroupCommand = {
        ...arbitraryAddGroupCommand(),
        name: reviewingGroupName,
      };

      beforeEach(async () => {
        await framework.commandHelpers.addGroup(addGroupCommand);
        await framework.commandHelpers.recordEvaluationPublication({
          ...arbitraryRecordEvaluationPublicationCommand(),
          articleId: article,
          groupId: addGroupCommand.groupId,
          evaluationType: 'review',
        });
        await framework.commandHelpers.recordEvaluationPublication({
          ...arbitraryRecordEvaluationPublicationCommand(),
          articleId: article,
          groupId: addGroupCommand.groupId,
          evaluationType: 'review',
        });
        result = getReviewingGroupNames();
      });

      it('returns an array containing the group\'s name once', () => {
        expect(result).toStrictEqual([reviewingGroupName]);
      });
    });

    describe('when an article has been reviewed by more than one group', () => {
      const reviewingGroup1Name = arbitraryString();
      const reviewingGroup2Name = arbitraryString();
      const addGroup1Command = {
        ...arbitraryAddGroupCommand(),
        name: reviewingGroup1Name,
      };
      const addGroup2Command = {
        ...arbitraryAddGroupCommand(),
        name: reviewingGroup2Name,
      };

      beforeEach(async () => {
        await framework.commandHelpers.addGroup(addGroup1Command);
        await framework.commandHelpers.addGroup(addGroup2Command);
        await framework.commandHelpers.recordEvaluationPublication({
          ...arbitraryRecordEvaluationPublicationCommand(),
          articleId: article,
          groupId: addGroup1Command.groupId,
          evaluationType: 'review',
        });
        await framework.commandHelpers.recordEvaluationPublication({
          ...arbitraryRecordEvaluationPublicationCommand(),
          articleId: article,
          groupId: addGroup2Command.groupId,
          evaluationType: 'review',
        });
        result = getReviewingGroupNames();
      });

      it('returns an array containing all the groups\' names once', () => {
        expect(new Set(result)).toStrictEqual(new Set([reviewingGroup1Name, reviewingGroup2Name]));
      });
    });

    describe('when an article has not been reviewed by any groups', () => {
      beforeEach(async () => {
        result = getReviewingGroupNames();
      });

      it('returns an empty array', () => {
        expect(result).toStrictEqual([]);
      });
    });

    describe('when an article has been evaluated by one group once, with a curation statement', () => {
      const addGroupCommand = arbitraryAddGroupCommand();

      beforeEach(async () => {
        await framework.commandHelpers.addGroup(addGroupCommand);
        await framework.commandHelpers.recordEvaluationPublication({
          ...arbitraryRecordEvaluationPublicationCommand(),
          articleId: article,
          groupId: addGroupCommand.groupId,
          evaluationType: 'curation-statement',
        });
        result = getReviewingGroupNames();
      });

      it('the group\'s name is not in the array returned', () => {
        expect(result).toStrictEqual([]);
      });
    });

    describe('when an article has been evaluated by one group twice, with both a curation statement and an evaluation of an unknown type', () => {
      const evaluatingGroupName = arbitraryString();
      const addGroupCommand = {
        ...arbitraryAddGroupCommand(),
        name: evaluatingGroupName,
      };

      beforeEach(async () => {
        await framework.commandHelpers.addGroup(addGroupCommand);
        await framework.commandHelpers.recordEvaluationPublication({
          ...arbitraryRecordEvaluationPublicationCommand(),
          articleId: article,
          groupId: addGroupCommand.groupId,
          evaluationType: undefined,
        });
        await framework.commandHelpers.recordEvaluationPublication({
          ...arbitraryRecordEvaluationPublicationCommand(),
          articleId: article,
          groupId: addGroupCommand.groupId,
          evaluationType: 'curation-statement',
        });
        result = getReviewingGroupNames();
      });

      it('returns an array containing the group\'s name once', () => {
        expect(result).toStrictEqual([evaluatingGroupName]);
      });
    });

    describe('when an article has been evaluated by one group once, and the evaluation type is unknown', () => {
      const evaluatingGroupName = arbitraryString();
      const addGroupCommand = {
        ...arbitraryAddGroupCommand(),
        name: evaluatingGroupName,
      };

      beforeEach(async () => {
        await framework.commandHelpers.addGroup(addGroupCommand);
        await framework.commandHelpers.recordEvaluationPublication({
          ...arbitraryRecordEvaluationPublicationCommand(),
          articleId: article,
          groupId: addGroupCommand.groupId,
          evaluationType: undefined,
        });
        result = getReviewingGroupNames();
      });

      it('returns an array containing the group\'s name once', () => {
        expect(result).toStrictEqual([evaluatingGroupName]);
      });
    });
  });

  describe('a reviewing group', () => {
    const addGroupCommand = arbitraryAddGroupCommand();
    const linkToTheGroupsPage = `/groups/${addGroupCommand.slug}`;
    let result: ViewModel['reviewingGroups'];

    beforeEach(async () => {
      await framework.commandHelpers.addGroup(addGroupCommand);
      await framework.commandHelpers.recordEvaluationPublication({
        ...arbitraryRecordEvaluationPublicationCommand(),
        articleId: article,
        groupId: addGroupCommand.groupId,
        evaluationType: undefined,
      });
      result = constructReviewingGroups(framework.dependenciesForViews, article);
    });

    it('has a name', () => {
      expect(result).toStrictEqual([expect.objectContaining({ name: addGroupCommand.name })]);
    });

    it.skip('links to the group\'s page', () => {
      expect(result).toStrictEqual([expect.objectContaining({ href: linkToTheGroupsPage })]);
    });
  });
});
