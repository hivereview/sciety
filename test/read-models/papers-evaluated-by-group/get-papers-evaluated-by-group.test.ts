/* eslint-disable jest/expect-expect */
import * as RA from 'fp-ts/ReadonlyArray';
import { pipe } from 'fp-ts/function';
import { DomainEvent, EventOfType } from '../../../src/domain-events';
import {
  getPapersEvaluatedByGroup,
} from '../../../src/read-models/papers-evaluated-by-group/get-papers-evaluated-by-group';
import { initialState, handleEvent, EvaluatedPaper } from '../../../src/read-models/papers-evaluated-by-group/handle-event';
import { ExpressionDoi } from '../../../src/types/expression-doi';
import { GroupId } from '../../../src/types/group-id';
import { arbitraryPaperSnapshotRecordedEvent } from '../../domain-events/arbitrary-paper-snapshot-event.helper';
import { arbitraryEvaluationPublicationRecordedEvent } from '../../domain-events/evaluation-resource-events.helper';
import { arbitraryExpressionDoi } from '../../types/expression-doi.helper';
import { arbitraryGroupId } from '../../types/group-id.helper';

const groupId = arbitraryGroupId();
const anotherGroupId = arbitraryGroupId();
const consideredGroupIds = [groupId, anotherGroupId];

const someTimeAfter = (date: Date) => {
  const later = new Date(date);
  later.setFullYear(date.getFullYear() + 1);
  return later;
};

const someTimeBefore = (date: Date) => {
  const later = new Date(date);
  later.setFullYear(date.getFullYear() - 1);
  return later;
};

const runQuery = (events: ReadonlyArray<DomainEvent>, queriedGroupId: GroupId) => {
  const readModel = pipe(
    events,
    RA.reduce(initialState(), handleEvent(consideredGroupIds)),
  );
  return getPapersEvaluatedByGroup(readModel)(queriedGroupId);
};

const expectSingleExpressionDoiIn = (
  result: ReadonlySet<EvaluatedPaper>,
  representative: ExpressionDoi,
) => {
  expect(result.size).toBe(1);

  const onlyElementInTheSet: EvaluatedPaper = result.values().next().value;

  expect(onlyElementInTheSet.representative).toStrictEqual(representative);
};

const expectLastEvaluatedAt = (
  result: ReadonlySet<EvaluatedPaper>,
  lastEvaluatedAt: Date,
) => {
  expect(result.size).toBe(1);

  const onlyElementInTheSet: EvaluatedPaper = result.values().next().value;

  expect(
    onlyElementInTheSet.lastEvaluatedAt,
  ).toStrictEqual(
    lastEvaluatedAt,
  );
};

describe('get-papers-evaluated-by-group', () => {
  const expressionDoiA = arbitraryExpressionDoi();
  const expressionDoiB = arbitraryExpressionDoi();
  const expressionDoiC = arbitraryExpressionDoi();
  const evaluationRecordedAgainstExpressionDoiA: EventOfType<'EvaluationPublicationRecorded'> = {
    ...arbitraryEvaluationPublicationRecordedEvent(),
    groupId,
    articleId: expressionDoiA,
  };
  const evaluationRecordedAgainstExpressionDoiB: EventOfType<'EvaluationPublicationRecorded'> = {
    ...arbitraryEvaluationPublicationRecordedEvent(),
    groupId,
    articleId: expressionDoiB,
  };
  const evaluationRecordedAgainstExpressionDoiC: EventOfType<'EvaluationPublicationRecorded'> = {
    ...arbitraryEvaluationPublicationRecordedEvent(),
    groupId,
    articleId: expressionDoiC,
  };
  const paperSnapshotWithExpressionDoisAB: EventOfType<'PaperSnapshotRecorded'> = {
    ...arbitraryPaperSnapshotRecordedEvent(),
    expressionDois: new Set([expressionDoiA, expressionDoiB]),
  };
  const paperSnapshotWithExpressionDoisABC: EventOfType<'PaperSnapshotRecorded'> = {
    ...arbitraryPaperSnapshotRecordedEvent(),
    expressionDois: new Set([expressionDoiA, expressionDoiB, expressionDoiC]),
  };
  let result: ReadonlySet<EvaluatedPaper>;

  describe('given activity by considered groups', () => {
    describe('when an evaluation has been recorded against an expression, but no corresponding paper snapshot is available', () => {
      const events = [
        evaluationRecordedAgainstExpressionDoiA,
      ] satisfies ReadonlyArray<DomainEvent>;

      beforeEach(() => {
        result = runQuery(events, groupId);
      });

      it('does not return anything', () => {
        expect(result.size).toBe(0);
      });
    });

    describe('when an evaluation has been recorded against an expression which is part of a subsequently recorded paper snapshot', () => {
      const events = [
        evaluationRecordedAgainstExpressionDoiA,
        paperSnapshotWithExpressionDoisAB,
      ] satisfies ReadonlyArray<DomainEvent>;

      beforeEach(() => {
        result = runQuery(events, groupId);
      });

      it('returns the paper representative', () => {
        expectSingleExpressionDoiIn(result, expressionDoiA);
      });

      it('returns a lastEvaluatedAt', () => {
        expectLastEvaluatedAt(result, evaluationRecordedAgainstExpressionDoiA.publishedAt);
      });
    });

    describe('when a paper snapshot has been recorded and then an evaluation is recorded against one of its expressions', () => {
      const events = [
        paperSnapshotWithExpressionDoisAB,
        evaluationRecordedAgainstExpressionDoiA,
      ] satisfies ReadonlyArray<DomainEvent>;

      beforeEach(() => {
        result = runQuery(events, groupId);
      });

      it('returns the paper representative', () => {
        expectSingleExpressionDoiIn(result, expressionDoiA);
      });

      it('returns a lastEvaluatedAt', () => {
        expectLastEvaluatedAt(result, evaluationRecordedAgainstExpressionDoiA.publishedAt);
      });
    });

    describe('when an evaluation has been recorded, a paper snapshot recorded and then a newly published evaluation is recorded for the same group', () => {
      const anotherEvaluationRecordedAgainstExpressionDoiA = {
        ...arbitraryEvaluationPublicationRecordedEvent(),
        groupId,
        articleId: expressionDoiA,
        publishedAt: someTimeAfter(evaluationRecordedAgainstExpressionDoiA.publishedAt),
      };
      const events = [
        evaluationRecordedAgainstExpressionDoiA,
        paperSnapshotWithExpressionDoisAB,
        anotherEvaluationRecordedAgainstExpressionDoiA,
      ] satisfies ReadonlyArray<DomainEvent>;

      beforeEach(() => {
        result = runQuery(events, groupId);
      });

      it('returns the paper representative', () => {
        expectSingleExpressionDoiIn(result, expressionDoiA);
      });

      it('returns a lastEvaluatedAt', () => {
        expectLastEvaluatedAt(result, anotherEvaluationRecordedAgainstExpressionDoiA.publishedAt);
      });
    });

    describe('when a paper snapshot has been recorded, followed by an evaluation, and then an earlier published evaluation', () => {
      const anotherEvaluationRecordedAgainstExpressionDoiA = {
        ...arbitraryEvaluationPublicationRecordedEvent(),
        groupId,
        articleId: expressionDoiA,
        publishedAt: someTimeBefore(evaluationRecordedAgainstExpressionDoiA.publishedAt),
      };
      const events = [
        paperSnapshotWithExpressionDoisAB,
        evaluationRecordedAgainstExpressionDoiA,
        anotherEvaluationRecordedAgainstExpressionDoiA,
      ] satisfies ReadonlyArray<DomainEvent>;

      beforeEach(() => {
        result = runQuery(events, groupId);
      });

      it('returns the paper representative', () => {
        expectSingleExpressionDoiIn(result, expressionDoiA);
      });

      it('returns a lastEvaluatedAt', () => {
        expectLastEvaluatedAt(result, evaluationRecordedAgainstExpressionDoiA.publishedAt);
      });
    });

    describe('when an evaluation has been recorded, a paper snapshot recorded and then an earlier published evaluation is recorded for the same group', () => {
      const anotherEvaluationRecordedAgainstExpressionDoiA = {
        ...arbitraryEvaluationPublicationRecordedEvent(),
        groupId,
        articleId: expressionDoiA,
        publishedAt: someTimeBefore(evaluationRecordedAgainstExpressionDoiA.publishedAt),
      };
      const events = [
        evaluationRecordedAgainstExpressionDoiA,
        paperSnapshotWithExpressionDoisAB,
        anotherEvaluationRecordedAgainstExpressionDoiA,
      ] satisfies ReadonlyArray<DomainEvent>;

      beforeEach(() => {
        result = runQuery(events, groupId);
      });

      it('returns the paper representative', () => {
        expectSingleExpressionDoiIn(result, expressionDoiA);
      });

      it('returns a lastEvaluatedAt', () => {
        expectLastEvaluatedAt(result, evaluationRecordedAgainstExpressionDoiA.publishedAt);
      });
    });

    describe('when two expressions have been evaluated and belong to the same paper according to a snapshot', () => {
      const newlyPublishedEvaluationRecordedAgainstExpressionDoiB = {
        ...evaluationRecordedAgainstExpressionDoiB,
        publishedAt: someTimeAfter(evaluationRecordedAgainstExpressionDoiA.publishedAt),
      };

      const events = [
        evaluationRecordedAgainstExpressionDoiA,
        newlyPublishedEvaluationRecordedAgainstExpressionDoiB,
        paperSnapshotWithExpressionDoisAB,
      ] satisfies ReadonlyArray<DomainEvent>;

      beforeEach(() => {
        result = runQuery(events, groupId);
      });

      it('returns the paper representative', () => {
        expectSingleExpressionDoiIn(result, expressionDoiA);
      });

      it('returns a lastEvaluatedAt', () => {
        expectLastEvaluatedAt(result, newlyPublishedEvaluationRecordedAgainstExpressionDoiB.publishedAt);
      });
    });

    describe('when another expression of an already evaluated paper is evaluated', () => {
      const newlyPublishedEvaluationRecordedAgainstExpressionDoiB = {
        ...evaluationRecordedAgainstExpressionDoiB,
        publishedAt: someTimeAfter(evaluationRecordedAgainstExpressionDoiA.publishedAt),
      };
      const events = [
        evaluationRecordedAgainstExpressionDoiA,
        paperSnapshotWithExpressionDoisAB,
        newlyPublishedEvaluationRecordedAgainstExpressionDoiB,
      ] satisfies ReadonlyArray<DomainEvent>;

      beforeEach(() => {
        result = runQuery(events, groupId);
      });

      it('returns the paper representative', () => {
        expectSingleExpressionDoiIn(result, expressionDoiA);
      });

      it('returns a lastEvaluatedAt', () => {
        expectLastEvaluatedAt(result, newlyPublishedEvaluationRecordedAgainstExpressionDoiB.publishedAt);
      });
    });

    describe('when two expressions have been evaluated for a known paper snapshot', () => {
      const newlyPublishedEvaluationRecordedAgainstExpressionDoiB = {
        ...evaluationRecordedAgainstExpressionDoiB,
        publishedAt: someTimeAfter(evaluationRecordedAgainstExpressionDoiA.publishedAt),
      };
      const events = [
        paperSnapshotWithExpressionDoisAB,
        evaluationRecordedAgainstExpressionDoiA,
        newlyPublishedEvaluationRecordedAgainstExpressionDoiB,
      ] satisfies ReadonlyArray<DomainEvent>;

      beforeEach(() => {
        result = runQuery(events, groupId);
      });

      it('returns the paper representative', () => {
        expectSingleExpressionDoiIn(result, expressionDoiA);
      });

      it('returns a lastEvaluatedAt', () => {
        expectLastEvaluatedAt(result, newlyPublishedEvaluationRecordedAgainstExpressionDoiB.publishedAt);
      });
    });

    describe('when an expression is evaluated that was not in the first snapshot but in the second snapshot', () => {
      const newlyPublishedEvaluationRecordedAgainstExpressionDoiB = {
        ...evaluationRecordedAgainstExpressionDoiB,
        publishedAt: someTimeAfter(evaluationRecordedAgainstExpressionDoiA.publishedAt),
      };
      const newlyPublishedEvaluationRecordedAgainstExpressionDoiC = {
        ...evaluationRecordedAgainstExpressionDoiC,
        publishedAt: someTimeAfter(newlyPublishedEvaluationRecordedAgainstExpressionDoiB.publishedAt),
      };
      const events = [
        evaluationRecordedAgainstExpressionDoiA,
        paperSnapshotWithExpressionDoisAB,
        newlyPublishedEvaluationRecordedAgainstExpressionDoiB,
        newlyPublishedEvaluationRecordedAgainstExpressionDoiC,
        paperSnapshotWithExpressionDoisABC,
      ] satisfies ReadonlyArray<DomainEvent>;

      beforeEach(() => {
        result = runQuery(events, groupId);
      });

      it('returns the paper representative', () => {
        expectSingleExpressionDoiIn(result, expressionDoiA);
      });

      it('returns a lastEvaluatedAt', () => {
        expectLastEvaluatedAt(result, newlyPublishedEvaluationRecordedAgainstExpressionDoiC.publishedAt);
      });
    });

    describe('when the paper snapshot has been recorded and then another group evaluates the paper', () => {
      const evaluationRecordedAgainstExpressionDoiAByAnotherGroup = {
        ...arbitraryEvaluationPublicationRecordedEvent(),
        articleId: expressionDoiA,
        groupId: anotherGroupId,
      };

      const events = [
        evaluationRecordedAgainstExpressionDoiA,
        paperSnapshotWithExpressionDoisAB,
        evaluationRecordedAgainstExpressionDoiAByAnotherGroup,
      ] satisfies ReadonlyArray<DomainEvent>;

      describe('when queried for the first group', () => {
        beforeEach(() => {
          result = runQuery(events, groupId);
        });

        it('returns the paper representative', () => {
          expectSingleExpressionDoiIn(result, expressionDoiA);
        });

        it('returns a lastEvaluatedAt', () => {
          expectLastEvaluatedAt(result, evaluationRecordedAgainstExpressionDoiA.publishedAt);
        });
      });

      describe('when queried for the other group', () => {
        it('returns the paper representative', () => {
          result = runQuery(events, anotherGroupId);

          expectSingleExpressionDoiIn(result, expressionDoiA);
        });

        it('returns a lastEvaluatedAt', () => {
          expectLastEvaluatedAt(result, evaluationRecordedAgainstExpressionDoiAByAnotherGroup.publishedAt);
        });
      });
    });

    describe('when two groups evaluate different expressions that belong to the same paper', () => {
      const evaluationRecordedAgainstExpressionDoiCByAnotherGroup = {
        ...evaluationRecordedAgainstExpressionDoiC,
        groupId: anotherGroupId,
      };
      const events = [
        evaluationRecordedAgainstExpressionDoiA,
        paperSnapshotWithExpressionDoisAB,
        evaluationRecordedAgainstExpressionDoiCByAnotherGroup,
        paperSnapshotWithExpressionDoisABC,
      ];

      describe('when queried for the first group', () => {
        beforeEach(() => {
          result = runQuery(events, groupId);
        });

        it('returns the paper representative', () => {
          expectSingleExpressionDoiIn(result, expressionDoiA);
        });

        it.skip('returns a lastEvaluatedAt', () => {
          expectLastEvaluatedAt(result, evaluationRecordedAgainstExpressionDoiA.publishedAt);
        });
      });

      describe('when queried for the other group', () => {
        beforeEach(() => {
          result = runQuery(events, anotherGroupId);
        });

        it('returns the paper representative', () => {
          expectSingleExpressionDoiIn(result, expressionDoiA);
        });

        it.skip('returns a lastEvaluatedAt', () => {
          expectLastEvaluatedAt(result, evaluationRecordedAgainstExpressionDoiCByAnotherGroup.publishedAt);
        });
      });
    });
  });

  describe('given activity by a group not considered', () => {
    const groupIdNotConsidered = arbitraryGroupId();

    describe('when an evaluation has been recorded against an expression which is the only member of a subsequently recorded paper snapshot', () => {
      const paperSnapshotWithExpressionDoiA: EventOfType<'PaperSnapshotRecorded'> = {
        ...arbitraryPaperSnapshotRecordedEvent(),
        expressionDois: new Set([expressionDoiA]),
      };
      const events = [
        {
          ...evaluationRecordedAgainstExpressionDoiA,
          groupId: groupIdNotConsidered,
        },
        paperSnapshotWithExpressionDoiA,
      ] satisfies ReadonlyArray<DomainEvent>;

      beforeEach(() => {
        result = runQuery(events, groupIdNotConsidered);
      });

      it('does not return anything', () => {
        expect(result.size).toBe(0);
      });
    });

    describe('when an evaluation has been recorded against an expression which is part of a subsequently recorded paper snapshot', () => {
      const events = [
        {
          ...evaluationRecordedAgainstExpressionDoiA,
          groupId: groupIdNotConsidered,
        },
        paperSnapshotWithExpressionDoisAB,
      ] satisfies ReadonlyArray<DomainEvent>;

      beforeEach(() => {
        result = runQuery(events, groupIdNotConsidered);
      });

      it('does not return anything', () => {
        expect(result.size).toBe(0);
      });
    });
  });

  describe('failure from staging', () => {
    const biorxivExpressionDoi = arbitraryExpressionDoi();
    const acmiExpressionDoi = arbitraryExpressionDoi();
    const evaluationPublicationRecordedForConsideredGroup = {
      ...arbitraryEvaluationPublicationRecordedEvent(),
      groupId,
      articleId: acmiExpressionDoi,
    };
    const events = [
      evaluationPublicationRecordedForConsideredGroup,
      {
        ...arbitraryPaperSnapshotRecordedEvent(),
        expressionDois: new Set([
          biorxivExpressionDoi,
          acmiExpressionDoi,
        ]),
      },
    ] satisfies ReadonlyArray<DomainEvent>;

    beforeEach(() => {
      result = runQuery(events, groupId);
    });

    it.todo('returns the paper representative');

    it('returns a lastEvaluatedAt', () => {
      expectLastEvaluatedAt(result, evaluationPublicationRecordedForConsideredGroup.publishedAt);
    });
  });
});
