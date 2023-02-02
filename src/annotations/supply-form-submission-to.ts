import * as O from 'fp-ts/Option';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { StatusCodes } from 'http-status-codes';
import { Middleware } from 'koa';
import bodyParser from 'koa-bodyparser';
import compose from 'koa-compose';
import { sequenceS } from 'fp-ts/Apply';
import { redirectBack } from '../http/redirect-back';
import { CommandResult } from '../types/command-result';
import { userIdCodec } from '../types/user-id';
import { getLoggedInScietyUser, Ports as GetLoggedInScietyUserPorts } from '../http/authentication-and-logging-in-of-sciety-users';

type CommandHandler = (input: unknown) => TE.TaskEither<unknown, CommandResult>;

const requireUserToBeAvasthiReading = (adapters: GetLoggedInScietyUserPorts): Middleware => async (context, next) => {
  pipe(
    {
      loggedInUser: getLoggedInScietyUser(adapters, context),
      avasthiReadingUserId: pipe(
        '1412019815619911685',
        userIdCodec.decode,
        O.fromEither,
      ),
    },
    sequenceS(O.Apply),
    O.filter(({ loggedInUser, avasthiReadingUserId }) => loggedInUser.id === avasthiReadingUserId),
    O.match(
      () => {
        context.response.status = StatusCodes.FORBIDDEN;
        context.response.body = 'Only @AvasthiReading is allowed to annotate their list.';
      },
      async () => { await next(); },
    ),
  );
};

type SupplyFormSubmissionTo = (
  adapters: GetLoggedInScietyUserPorts,
  handler: CommandHandler,
) => Middleware;

export const supplyFormSubmissionTo: SupplyFormSubmissionTo = (adapters, handler) => compose([
  bodyParser({ enableTypes: ['form'] }),
  requireUserToBeAvasthiReading(adapters),
  async (context, next) => {
    await pipe(
      context.request.body,
      handler,
    )();

    await next();
  },
  redirectBack,
]);
