import * as PR from 'io-ts/PathReporter';
import axios from 'axios';
import { flow, pipe } from 'fp-ts/function';
import * as TE from 'fp-ts/TaskEither';
import * as E from 'fp-ts/Either';
import * as t from 'io-ts';
import * as tt from 'io-ts-types';
import * as GID from '../../src/types/group-id.js';
import * as UID from '../../src/types/user-id.js';
import * as LOID from '../../src/types/list-owner-id.js';
import { listCodec } from '../../src/types/list.js';
import { ListId } from '../../src/types/list-id.js';

const responseCodec = t.type({
  data: t.type({
    items: tt.nonEmptyArray(listCodec),
  }),
});

const fetchFirstListOwnedBy = async (ownerId: string): Promise<ListId> => pipe(
  TE.tryCatch(
    async () => axios.get(`http://localhost:8080/api/lists/owned-by/${ownerId}`),
    String,
  ),
  TE.chainEitherK(flow(
    responseCodec.decode,
    E.mapLeft((errors) => PR.failure(errors).join('')),
  )),
  TE.map((response) => response.data),
  TE.match(
    (error) => { throw new Error(error); },
    (response) => response.items[0].id,
  ),
)();

export const getIdOfFirstListOwnedByUser = async (userId: UID.UserId): Promise<ListId> => pipe(
  userId,
  LOID.fromUserId,
  LOID.toString,
  fetchFirstListOwnedBy,
);

export const getIdOfFirstListOwnedByGroup = async (groupId: GID.GroupId): Promise<ListId> => pipe(
  groupId,
  LOID.fromGroupId,
  LOID.toString,
  fetchFirstListOwnedBy,
);
