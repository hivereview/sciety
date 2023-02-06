import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as TE from 'fp-ts/TaskEither';
import { flow, pipe } from 'fp-ts/function';
import * as PR from 'io-ts/PathReporter';
import { Middleware } from 'koa';
import { checkUserOwnsList } from './check-user-owns-list';
import { removeArticleFromListCommandCodec } from '../../write-side/commands/remove-article-from-list';
import {
  CommitEvents, GetAllEvents, GetList, Logger, RemoveArticleFromList,
} from '../../shared-ports';
import { getLoggedInScietyUser, Ports as GetLoggedInScietyUserPorts } from '../authentication-and-logging-in-of-sciety-users';
import { UserDetails } from '../../types/user-details';

type Ports = GetLoggedInScietyUserPorts & {
  logger: Logger,
  getAllEvents: GetAllEvents,
  commitEvents: CommitEvents,
  getList: GetList,
  removeArticleFromList: RemoveArticleFromList,
};

type FormBody = {
  articleid: unknown,
  listid: unknown,
};

const handleFormSubmission = (adapters: Ports, userDetails: O.Option<UserDetails>) => (formBody: FormBody) => pipe(
  {
    articleId: formBody.articleid,
    listId: formBody.listid,
  },
  removeArticleFromListCommandCodec.decode,
  E.bimap(
    (errors) => pipe(
      errors,
      PR.failure,
      (fails) => adapters.logger('error', 'invalid remove article from list form command', { fails }),
    ),
    (command) => {
      adapters.logger('info', 'received remove article from list form command', { command });
      return command;
    },
  ),
  E.chainW((command) => pipe(
    userDetails,
    O.match(
      () => {
        adapters.logger('error', 'Logged in user not found', { command });
        return E.left(undefined);
      },
      (user) => E.right({
        command,
        userId: user.id,
      }),
    ),
  )),
  TE.fromEither,
  TE.chainFirstW(flow(
    ({ command, userId }) => checkUserOwnsList(adapters, command.listId, userId),
    TE.mapLeft((logEntry) => {
      adapters.logger('error', logEntry.message, logEntry.payload);
      return logEntry;
    }),
  )),
  TE.map(({ command }) => command),
  TE.chainW(adapters.removeArticleFromList),
);

export const removeArticleFromList = (adapters: Ports): Middleware => async (context, next) => {
  const user = getLoggedInScietyUser(adapters, context);
  await pipe(
    context.request.body,
    handleFormSubmission(adapters, user),
    TE.mapLeft(() => {
      context.redirect('/action-failed');
    }),
    TE.chainTaskK(() => async () => {
      await next();
    }),
  )();
};
