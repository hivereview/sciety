import * as t from 'io-ts';
import { evaluationLocatorCodec } from '../../types/evaluation-locator';
import { evaluationTypeCodec } from './evaluation-type';

export const updateEvaluationCommandCodec = t.intersection([
  t.strict({
    evaluationLocator: evaluationLocatorCodec,
  }),
  t.partial({
    evaluationType: evaluationTypeCodec,
    authors: t.readonlyArray(t.string),
  }),
]);

export type UpdateEvaluationCommand = t.TypeOf<typeof updateEvaluationCommandCodec>;
