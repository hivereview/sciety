import { evaluationsStatus } from './evaluations-status';
import { getEvaluationsByGroup } from './get-evaluations-by-group';
import { getEvaluationsOfExpression } from './get-evaluations-of-expression';
import { getEvaluationsWithNoType } from './get-evaluations-with-no-type';
import { handleEvent, initialState } from './handle-event';

export const evaluations = {
  queries: {
    evaluationsStatus,
    getEvaluationsOfExpression,
    getEvaluationsByGroup,
    getEvaluationsWithNoType,
  },
  initialState,
  handleEvent,
};
