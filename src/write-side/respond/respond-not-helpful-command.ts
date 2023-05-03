import { ReviewResponse } from './review-response';
import {
  userFoundReviewNotHelpful,
  UserFoundReviewNotHelpfulEvent, userRevokedFindingReviewHelpful,
  UserRevokedFindingReviewHelpfulEvent,
} from '../../domain-events';
import { EvaluationLocator } from '../../types/evaluation-locator';
import { UserId } from '../../types/user-id';

type RespondNotHelpful = (currentResponse: ReviewResponse, userId: UserId, reviewId: EvaluationLocator)
=> ReadonlyArray<UserRevokedFindingReviewHelpfulEvent | UserFoundReviewNotHelpfulEvent>;

export const respondNotHelpful: RespondNotHelpful = (currentResponse, userId, reviewId) => {
  switch (currentResponse) {
    case 'none':
      return [
        userFoundReviewNotHelpful(userId, reviewId),
      ];
    case 'not-helpful':
      return [];
    case 'helpful':
      return [
        userRevokedFindingReviewHelpful(userId, reviewId),
        userFoundReviewNotHelpful(userId, reviewId),
      ];
  }
};
