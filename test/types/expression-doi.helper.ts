import * as EDOI from '../../src/types/expression-doi';
import { arbitraryNumber } from '../helpers';

export const arbitraryExpressionDoi = (recognizableValue?: string): EDOI.ExpressionDoi => (
  EDOI.fromValidatedString(`10.1101/${arbitraryNumber(100000000000, 999999999999)}${recognizableValue ?? ''}`)
);
