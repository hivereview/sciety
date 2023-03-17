import * as t from 'io-ts';
import { listIdCodec } from '../../types/list-id';
import { userGeneratedInputCodec } from '../../types/user-generated-input';

export const listNameMaxLength = 100;
export const listDescriptionMaxLength = 250;

export const editListDetailsCommandCodec = t.type({
  name: userGeneratedInputCodec(listNameMaxLength),
  description: userGeneratedInputCodec(listDescriptionMaxLength, true),
  listId: listIdCodec,
});

export type EditListDetailsCommand = t.TypeOf<typeof editListDetailsCommandCodec>;
