import { ensureEvaluationsAreListed } from '../../../src/sagas/ensure-evaluations-are-listed/ensure-evaluations-are-listed';
import { dummyLogger } from '../../dummy-logger';
import { TestFramework, createTestFramework } from '../../framework';
import { arbitraryGroup } from '../../types/group.helper';
import { arbitraryRecordedEvaluation } from '../../types/recorded-evaluation.helper';
import * as LOID from '../../../src/types/list-owner-id';

describe('ensure-evaluations-are-listed', () => {
  let framework: TestFramework;

  beforeEach(() => {
    framework = createTestFramework();
  });

  describe('when there are listed evaluations', () => {
    const group = arbitraryGroup();
    const evaluation = {
      ...arbitraryRecordedEvaluation(),
      groupId: group.id,
    };
    let listedArticleIds: ReadonlyArray<string>;

    beforeEach(async () => {
      await framework.commandHelpers.createGroup(group);
      await framework.commandHelpers.recordEvaluation(evaluation);

      await ensureEvaluationsAreListed({
        ...framework.queries,
        getAllEvents: framework.getAllEvents,
        commitEvents: framework.commitEvents,
        logger: dummyLogger,
      });

      const list = framework.queries.selectAllListsOwnedBy(LOID.fromGroupId(group.id))[0];
      listedArticleIds = list.articleIds;
    });

    it('adds the article to the appropriate list', () => {
      expect(listedArticleIds).toContain(evaluation.articleId.value);
    });
  });
});
