import * as O from 'fp-ts/Option';
import * as RM from 'fp-ts/ReadonlyMap';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import * as S from 'fp-ts/string';
import { StatusCodes } from 'http-status-codes';
import { Middleware } from 'koa';
import { GetListsEvents } from './get-lists-events';
import { Logger } from '../infrastructure/logger';
import { constructListsReadModelKeyedOnGroupId } from '../shared-read-models/lists/construct-lists-read-model-keyed-on-group-id';

type Ports = {
  getListsEvents: GetListsEvents,
  logger: Logger,
};

export const ownedBy = (ports: Ports): Middleware => async ({ params, response }, next) => {
  response.set({ 'Content-Type': 'application/json' });
  ports.logger('debug', 'Started ownedBy query');
  await pipe(
    ports.getListsEvents,
    TE.chainFirst(() => {
      ports.logger('debug', 'Loaded lists events');
      return TE.right('everything is ok');
    }),
    TE.chainTaskK(constructListsReadModelKeyedOnGroupId),
    TE.chainFirst(() => {
      ports.logger('debug', 'Constructed read model');
      return TE.right('everything is ok');
    }),
    TE.map(RM.lookup(S.Eq)(params.groupId)),
    TE.map(O.fold(
      () => [],
      (list) => [list],
    )),
    TE.match(
      () => {
        response.status = StatusCodes.SERVICE_UNAVAILABLE;
      },
      (items) => {
        response.status = StatusCodes.OK;
        response.body = { items };
      },
    ),
  )();

  await next();
};
