import * as t from 'io-ts';
import * as tt from 'io-ts-types';

export const paramsCodec = t.type({
  query: t.string,
  cursor: tt.optionFromNullable(t.string),
  page: tt.optionFromNullable(tt.NumberFromString),
  evaluatedOnly: tt.optionFromNullable(t.unknown),
});

export type Params = t.TypeOf<typeof paramsCodec>;
