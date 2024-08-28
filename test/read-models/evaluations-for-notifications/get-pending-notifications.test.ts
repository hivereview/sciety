/* eslint-disable jest/max-expects */
import * as RA from 'fp-ts/ReadonlyArray';
import { pipe } from 'fp-ts/function';
import { DomainEvent, EventOfType } from '../../../src/domain-events';
import { getPendingNotifications } from '../../../src/read-models/evaluations-for-notifications/get-pending-notifications';
import { handleEvent, initialState } from '../../../src/read-models/evaluations-for-notifications/handle-event';
import { arbitraryEvaluationPublicationRecordedEvent, arbitraryEvaluationRemovalRecordedEvent, arbitraryIncorrectlyRecordedEvaluationErasedEvent } from '../../domain-events/evaluation-resource-events.helper';
import { arbitraryUrl } from '../../helpers';
import { arbitraryGroupId } from '../../types/group-id.helper';

const groupId = arbitraryGroupId();
const anotherGroupId = arbitraryGroupId();
const groupWithTwoTargetsId = arbitraryGroupId();
const target = {
  id: arbitraryUrl(),
  inbox: arbitraryUrl(),
};
const targetOfAnotherGroup = {
  id: arbitraryUrl(),
  inbox: arbitraryUrl(),
};
const multipleTargetsCase1 = {
  id: arbitraryUrl(),
  inbox: arbitraryUrl(),
};
const multipleTargetsCase2 = {
  id: arbitraryUrl(),
  inbox: arbitraryUrl(),
};
const consideredGroupIds = new Map([
  [groupId, [target]],
  [anotherGroupId, [targetOfAnotherGroup]],
  [groupWithTwoTargetsId, [multipleTargetsCase1, multipleTargetsCase2]],
]);

const runQuery = (events: ReadonlyArray<DomainEvent>) => {
  const readModel = pipe(
    events,
    RA.reduce(initialState(), handleEvent(consideredGroupIds)),
  );
  return getPendingNotifications(readModel)();
};

describe('get-pending-notifications', () => {
  describe('given activity by groups configured for different targets', () => {
    describe('when no evaluation publications have been recorded', () => {
      const result = runQuery([]);

      it('returns no notifications', () => {
        expect(result).toHaveLength(0);
      });
    });

    describe('when an evaluation publication has been recorded', () => {
      const evaluationPublicationRecorded: EventOfType<'EvaluationPublicationRecorded'> = {
        ...arbitraryEvaluationPublicationRecordedEvent(),
        groupId,
      };
      const evaluationRemovalRecorded: EventOfType<'EvaluationRemovalRecorded'> = {
        ...arbitraryEvaluationRemovalRecordedEvent(),
        evaluationLocator: evaluationPublicationRecorded.evaluationLocator,
      };
      const evaluationErased: EventOfType<'IncorrectlyRecordedEvaluationErased'> = {
        ...arbitraryIncorrectlyRecordedEvaluationErasedEvent(),
        evaluationLocator: evaluationPublicationRecorded.evaluationLocator,
      };

      describe('and nothing else happened', () => {
        const events = [
          evaluationPublicationRecorded,
        ];
        const result = runQuery(events);

        it('returns one notification', () => {
          expect(result).toHaveLength(1);
          expect(result[0].evaluationLocator).toStrictEqual(evaluationPublicationRecorded.evaluationLocator);
          expect(result[0].expressionDoi).toStrictEqual(evaluationPublicationRecorded.articleId);
          expect(result[0].target).toStrictEqual(target);
        });
      });

      describe('and its removal has been recorded', () => {
        const events = [
          evaluationPublicationRecorded,
          evaluationRemovalRecorded,
        ];
        const result = runQuery(events);

        it('returns no notifications', () => {
          expect(result).toHaveLength(0);
        });
      });

      describe('and the recording was erased', () => {
        const events = [
          evaluationPublicationRecorded,
          evaluationErased,
        ];
        const result = runQuery(events);

        it('returns no notifications', () => {
          expect(result).toHaveLength(0);
        });
      });

      describe('and the recording was erased, and the publication recorded again', () => {
        const evaluationPublicationRecordedAgain: EventOfType<'EvaluationPublicationRecorded'> = {
          ...arbitraryEvaluationPublicationRecordedEvent(),
          groupId: anotherGroupId,
          evaluationLocator: evaluationPublicationRecorded.evaluationLocator,
        };
        const events = [
          evaluationPublicationRecorded,
          evaluationErased,
          evaluationPublicationRecordedAgain,
        ];
        const result = runQuery(events);

        it('returns one notification', () => {
          expect(result).toHaveLength(1);
          expect(result[0].evaluationLocator).toStrictEqual(evaluationPublicationRecordedAgain.evaluationLocator);
          expect(result[0].expressionDoi).toStrictEqual(evaluationPublicationRecordedAgain.articleId);
          expect(result[0].target).toStrictEqual(targetOfAnotherGroup);
        });
      });

      describe('and its removal has been recorded, and the recordings were erased', () => {
        const events = [
          evaluationPublicationRecorded,
          evaluationRemovalRecorded,
          evaluationErased,
        ];
        const result = runQuery(events);

        it('returns no notifications', () => {
          expect(result).toHaveLength(0);
        });
      });
    });

    describe('when two evaluation publications by the same group have been recorded', () => {
      const evaluationPublicationRecorded1: EventOfType<'EvaluationPublicationRecorded'> = {
        ...arbitraryEvaluationPublicationRecordedEvent(),
        groupId,
      };
      const evaluationPublicationRecorded2: EventOfType<'EvaluationPublicationRecorded'> = {
        ...arbitraryEvaluationPublicationRecordedEvent(),
        groupId,
      };
      const events = [
        evaluationPublicationRecorded1,
        evaluationPublicationRecorded2,
      ];
      const result = runQuery(events);

      it('returns two notifications', () => {
        expect(result).toHaveLength(2);
        expect(result[0].evaluationLocator).toStrictEqual(evaluationPublicationRecorded1.evaluationLocator);
        expect(result[0].expressionDoi).toStrictEqual(evaluationPublicationRecorded1.articleId);
        expect(result[0].target).toStrictEqual(target);
        expect(result[1].evaluationLocator).toStrictEqual(evaluationPublicationRecorded2.evaluationLocator);
        expect(result[1].expressionDoi).toStrictEqual(evaluationPublicationRecorded2.articleId);
        expect(result[1].target).toStrictEqual(target);
      });
    });

    describe('when two evaluation publications by two different groups have been recorded', () => {
      const evaluationPublicationRecorded1: EventOfType<'EvaluationPublicationRecorded'> = {
        ...arbitraryEvaluationPublicationRecordedEvent(),
        groupId,
      };
      const evaluationPublicationRecorded2: EventOfType<'EvaluationPublicationRecorded'> = {
        ...arbitraryEvaluationPublicationRecordedEvent(),
        groupId: anotherGroupId,
      };
      const events = [
        evaluationPublicationRecorded1,
        evaluationPublicationRecorded2,
      ];
      const result = runQuery(events);

      it('returns two notifications', () => {
        expect(result).toHaveLength(2);
        expect(result[0].evaluationLocator).toStrictEqual(evaluationPublicationRecorded1.evaluationLocator);
        expect(result[0].expressionDoi).toStrictEqual(evaluationPublicationRecorded1.articleId);
        expect(result[0].target).toStrictEqual(target);
        expect(result[1].evaluationLocator).toStrictEqual(evaluationPublicationRecorded2.evaluationLocator);
        expect(result[1].expressionDoi).toStrictEqual(evaluationPublicationRecorded2.articleId);
        expect(result[1].target).toStrictEqual(targetOfAnotherGroup);
      });
    });
  });

  describe('given activity by a group configured for two targets', () => {
    describe('when an evaluation publication has been recorded', () => {
      describe('and nothing else happened', () => {
        const evaluationPublicationRecorded: EventOfType<'EvaluationPublicationRecorded'> = {
          ...arbitraryEvaluationPublicationRecordedEvent(),
          groupId: groupWithTwoTargetsId,
        };
        const events = [
          evaluationPublicationRecorded,
        ];
        const result = runQuery(events);

        it('returns two notifications', () => {
          expect(result).toHaveLength(2);
          expect(result[0].evaluationLocator).toStrictEqual(evaluationPublicationRecorded.evaluationLocator);
          expect(result[0].expressionDoi).toStrictEqual(evaluationPublicationRecorded.articleId);
          expect(result[0].target).toStrictEqual(multipleTargetsCase1);
          expect(result[1].evaluationLocator).toStrictEqual(evaluationPublicationRecorded.evaluationLocator);
          expect(result[1].expressionDoi).toStrictEqual(evaluationPublicationRecorded.articleId);
          expect(result[1].target).toStrictEqual(multipleTargetsCase2);
        });
      });
    });
  });

  describe('given activity by a group that is not configured for any target', () => {
    const evaluationPublicationRecorded = arbitraryEvaluationPublicationRecordedEvent();
    const events = [
      evaluationPublicationRecorded,
    ];
    const result = runQuery(events);

    it('returns no notifications', () => {
      expect(result).toHaveLength(0);
    });
  });
});
