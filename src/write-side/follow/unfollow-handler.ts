import { Middleware } from '@koa/router';
import * as O from 'fp-ts/Option';
import { pipe } from 'fp-ts/function';
import { StatusCodes } from 'http-status-codes';
import * as t from 'io-ts';
import { getLoggedInScietyUser, Ports as GetLoggedInScietyUserPorts } from '../../http/authentication-and-logging-in-of-sciety-users';
import { Ports as UnfollowCommandPorts, unfollowCommand } from './unfollow-command';
import { Logger } from '../../shared-ports';
import { GroupIdFromString } from '../../types/codecs/GroupIdFromString';

type Ports = GetLoggedInScietyUserPorts & UnfollowCommandPorts & {
  logger: Logger,
};

const bodyCodec = t.type({
  editorialcommunityid: GroupIdFromString,
});

export const unfollowHandler = (ports: Ports): Middleware => async (context, next) => {
  await pipe(
    context.request.body,
    bodyCodec.decode,
    O.fromEither,
    O.map((body) => body.editorialcommunityid),
    O.fold(
      () => context.throw(StatusCodes.BAD_REQUEST),
      async (groupId) => pipe(
        getLoggedInScietyUser(ports, context),
        O.match(
          () => {
            ports.logger('error', 'Logged in user not found', { context });
            context.response.status = StatusCodes.INTERNAL_SERVER_ERROR;
          },
          async (userDetails) => {
            context.redirect('back');
            await unfollowCommand(ports)(userDetails.id, groupId)();
          },
        ),
      ),
    ),
  );

  await next();
};
