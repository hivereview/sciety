import { Middleware } from '@koa/router';
import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import { pipe } from 'fp-ts/function';
import { StatusCodes } from 'http-status-codes';
import * as t from 'io-ts';
import * as PR from 'io-ts/PathReporter';
import { Logger } from '../../shared-ports';
import { GroupIdFromStringCodec } from '../../types/group-id';
import { unfollowCommandHandler } from '../../write-side/command-handlers';
import { DependenciesForCommands } from '../../write-side/dependencies-for-commands';
import { Dependencies as GetLoggedInScietyUserDependencies, getAuthenticatedUserIdFromContext } from '../authentication-and-logging-in-of-sciety-users';
import { sendDefaultErrorHtmlResponse } from '../send-default-error-html-response';

type Dependencies = DependenciesForCommands & GetLoggedInScietyUserDependencies & {
  logger: Logger,
};

const formBodyCodec = t.strict({
  editorialcommunityid: GroupIdFromStringCodec,
});

type FormBody = t.TypeOf<typeof formBodyCodec>;

export const unfollowHandler = (dependencies: Dependencies): Middleware => async (context) => {
  const decoded = formBodyCodec.decode(context.request.body);
  if (E.isLeft(decoded)) {
    dependencies.logger('error', 'Failed to decode a form submission', {
      codec: formBodyCodec.name,
      codecDecodingError: PR.failure(decoded.left),
      requestBody: context.request.body,
    });
    sendDefaultErrorHtmlResponse(dependencies, context, StatusCodes.BAD_REQUEST, 'Form submission failed unexpectedly.');
  }
  const formBody = decoded as E.Either<unknown, FormBody>;
  if (E.isLeft(formBody)) {
    return;
  }

  const groupId = formBody.right.editorialcommunityid;

  pipe(
    getAuthenticatedUserIdFromContext(context),
    O.match(
      () => {
        dependencies.logger('error', 'Logged in user not found', { context });
        context.response.status = StatusCodes.INTERNAL_SERVER_ERROR;
      },
      async (userId) => {
        context.redirect('back');
        await pipe(
          {
            userId,
            groupId,
          },
          unfollowCommandHandler(dependencies),
        )();
      },
    ),
  );
};
