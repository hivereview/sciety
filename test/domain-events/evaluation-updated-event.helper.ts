import { EventOfType, constructEvent } from '../../src/domain-events';
import { arbitraryEvaluationLocator } from '../types/evaluation-locator.helper';
import { arbitraryEvaluationType } from '../types/evaluation-type.helper';

export const arbitraryEvaluationUpdatedEvent = (): EventOfType<'EvaluationUpdated'> => constructEvent('EvaluationUpdated')({
  evaluationLocator: arbitraryEvaluationLocator(),
  evaluationType: arbitraryEvaluationType(),
});
