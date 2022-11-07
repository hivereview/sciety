import * as E from 'fp-ts/Either';
import { pipe } from 'fp-ts/function';
import { evaluationRecorded } from '../../src/domain-events/evaluation-recorded-event';
import { subjectAreaRecorded } from '../../src/domain-events/subject-area-recorded-event';
import { executeCommand } from '../../src/record-subject-area/execute-command';
import { shouldNotBeCalled } from '../should-not-be-called';
import { arbitraryArticleId } from '../types/article-id.helper';
import { arbitraryGroupId } from '../types/group-id.helper';
import { arbitraryReviewId } from '../types/review-id.helper';
import { arbitrarySubjectArea } from '../types/subject-area.helper';

describe('record-subject-area-command-handler', () => {
  const articleId = arbitraryArticleId();
  const subjectArea = arbitrarySubjectArea();
  const command = {
    articleId,
    subjectArea,
  };

  describe('given no events for the given article id', () => {
    const result = pipe(
      [subjectAreaRecorded(arbitraryArticleId(), arbitrarySubjectArea())],
      executeCommand(command),
      E.getOrElseW(shouldNotBeCalled),
    );

    it('raises an event', () => {
      expect(result).toStrictEqual([expect.objectContaining(
        { type: 'SubjectAreaRecorded' },
      )]);
    });

    it('raises an event, containing the article id from the command', () => {
      expect(result).toStrictEqual([expect.objectContaining(
        { articleId },
      )]);
    });

    it('raises an event, containing the subject area from the command', () => {
      expect(result).toStrictEqual([expect.objectContaining(
        { subjectArea },
      )]);
    });
  });

  describe('when an evaluation was recorded', () => {
    const result = pipe(
      [
        evaluationRecorded(arbitraryGroupId(), articleId, arbitraryReviewId()),
      ],
      executeCommand(command),
      E.getOrElseW(shouldNotBeCalled),
    );

    it('raises an event', () => {
      expect(result).toStrictEqual([expect.objectContaining(
        { type: 'SubjectAreaRecorded' },
      )]);
    });
  });

  describe('the same subject area was recorded', () => {
    const result = pipe(
      [
        subjectAreaRecorded(articleId, subjectArea),
      ],
      executeCommand(command),
      E.getOrElseW(shouldNotBeCalled),
    );

    it('raises no events', () => {
      expect(result).toStrictEqual([]);
    });
  });

  describe('a different subject area was recorded for the article', () => {
    const result = pipe(
      [
        subjectAreaRecorded(articleId, arbitrarySubjectArea()),
      ],
      executeCommand(command),
    );

    it('returns an error message', () => {
      expect(result).toStrictEqual(E.left(expect.anything()));
    });
  });
});
