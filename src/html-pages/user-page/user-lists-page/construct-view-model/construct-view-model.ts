import * as t from 'io-ts';
import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import * as tt from 'io-ts-types';
import * as DE from '../../../../types/data-error';
import * as LOID from '../../../../types/list-owner-id';
import { ViewModel } from '../view-model';
import { constructListsTab } from './construct-lists-tab';
import { candidateUserHandleCodec } from '../../../../types/candidate-user-handle';
import { userIdCodec } from '../../../../types/user-id';
import { Queries } from '../../../../read-models';

export const userPageParams = t.type({
  handle: candidateUserHandleCodec,
  user: tt.optionFromNullable(t.type({
    id: userIdCodec,
  })),
});

export type Params = t.TypeOf<typeof userPageParams>;

type ConstructViewModel = (queries: Queries)
=> (params: Params)
=> TE.TaskEither<DE.DataError, ViewModel>;

export const constructViewModel: ConstructViewModel = (queries) => (params) => pipe(
  params.handle,
  queries.lookupUserByHandle,
  E.fromOption(() => DE.notFound),
  E.map((user) => ({
    groupIds: queries.getGroupsFollowedBy(user.id),
    userDetails: user,
    lists: queries.selectAllListsOwnedBy(LOID.fromUserId(user.id)),
  })),
  E.map(({ groupIds, userDetails, lists }) => ({
    groupIds,
    userDetails,
    listCount: lists.length,
    ...constructListsTab(
      lists,
      userDetails.id,
      pipe(
        params.user,
        O.map((user) => user.id),
      ),
    ),
  })),
  TE.fromEither,
);
