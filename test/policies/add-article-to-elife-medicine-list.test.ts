import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { evaluationRecorded, userSavedArticle } from '../../src/domain-events';
import { addArticleToElifeMedicineList } from '../../src/policies/add-article-to-elife-medicine-list';
import * as DE from '../../src/types/data-error';
import * as Gid from '../../src/types/group-id';
import { shouldNotBeCalled } from '../should-not-be-called';
import { arbitraryDoi } from '../types/doi.helper';
import { arbitraryGroupId } from '../types/group-id.helper';
import { arbitraryReviewId } from '../types/review-id.helper';
import { arbitraryUserId } from '../types/user-id.helper';

describe('add-article-to-elife-medicine-list', () => {
  describe('when an EvaluationRecorded event by eLife is received', () => {
    const elifeGroupId = Gid.fromValidatedString('b560187e-f2fb-4ff9-a861-a204f3fc0fb0');

    describe('and the subject area belongs to the Medicine list', () => {
      it.todo('calls the AddArticleToList command');
    });

    describe('and the subject area does not belong to the Medicine list', () => {
      it.todo('does not call the AddArticleToList command');
    });

    describe('and subject area cannot be retrieved', () => {
      const ports = {
        getAllEvents: T.of([]),
        commitEvents: jest.fn(() => T.of('no-events-created' as const)),
        logger: shouldNotBeCalled,
        fetchSubjectArea: () => TE.left(DE.unavailable),
      };
      const event = evaluationRecorded(elifeGroupId, arbitraryDoi(), arbitraryReviewId());

      beforeEach(async () => {
        await addArticleToElifeMedicineList(ports)(event)();
      });

      it('does not call the AddArticleToList command', () => {
        expect(ports.commitEvents).not.toHaveBeenCalled();
      });

      it.todo('logs an error');
    });
  });

  describe('when an EvaluationRecorded event by another group is received', () => {
    const anotherGroupId = arbitraryGroupId();
    const ports = {
      getAllEvents: T.of([]),
      commitEvents: jest.fn(() => T.of('no-events-created' as const)),
      logger: shouldNotBeCalled,
      fetchSubjectArea: shouldNotBeCalled,
    };
    const event = evaluationRecorded(anotherGroupId, arbitraryDoi(), arbitraryReviewId());

    beforeEach(async () => {
      await addArticleToElifeMedicineList(ports)(event)();
    });

    it('does not call the AddArticleToList command', () => {
      expect(ports.commitEvents).not.toHaveBeenCalled();
    });
  });

  describe('when any other event is received', () => {
    const ports = {
      getAllEvents: T.of([]),
      commitEvents: jest.fn(() => T.of('no-events-created' as const)),
      logger: shouldNotBeCalled,
      fetchSubjectArea: shouldNotBeCalled,
    };
    const event = userSavedArticle(arbitraryUserId(), arbitraryDoi());

    beforeEach(async () => {
      await addArticleToElifeMedicineList(ports)(event)();
    });

    it('does not call the AddArticleToList command', () => {
      expect(ports.commitEvents).not.toHaveBeenCalled();
    });
  });
});
