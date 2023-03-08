import * as E from 'fp-ts/Either';
import * as t from 'io-ts';
import * as O from 'fp-ts/Option';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { StatusCodes } from 'http-status-codes';
import { Middleware } from 'koa';
import { getLoggedInScietyUser, Ports as GetLoggedInScietyUserPorts } from '../../http/authentication-and-logging-in-of-sciety-users';
import { followCommand, Ports as FollowCommandPorts } from './follow-command';
import { renderErrorPage } from '../../http/render-error-page';
import { constructRedirectUrl } from '../../http/require-logged-in-user';
import { standardPageLayout } from '../../shared-components/standard-page-layout';
import { GetGroup, Logger } from '../../shared-ports';
import * as DE from '../../types/data-error';
import * as GroupId from '../../types/group-id';
import { toHtmlFragment } from '../../types/html-fragment';
import { GroupIdFromString } from '../../types/codecs/GroupIdFromString';

type Ports = GetLoggedInScietyUserPorts & FollowCommandPorts & {
  logger: Logger,
  getGroup: GetGroup,
};

const validate = (ports: Ports) => (groupId: GroupId.GroupId) => pipe(
  ports.getGroup(groupId),
  E.fromOption(() => DE.notFound),
  E.map((group) => ({
    groupId: group.id,
  })),
);

export const groupProperty = 'groupid';

const bodyCodec = t.type({
  [groupProperty]: GroupIdFromString,
});

export const followHandler = (ports: Ports): Middleware => async (context, next) => {
  await pipe(
    context.request.body,
    bodyCodec.decode,
    TE.fromEither,
    TE.bimap(
      () => DE.notFound,
      (body) => body[groupProperty],
    ),
    TE.chainEitherK(validate(ports)),
    TE.fold(
      () => {
        ports.logger('error', 'Problem with /follow', { error: StatusCodes.BAD_REQUEST });

        context.response.status = StatusCodes.INTERNAL_SERVER_ERROR;
        context.response.body = standardPageLayout(O.none)({
          title: 'Error',
          content: renderErrorPage(toHtmlFragment('Something went wrong; we\'re looking into it.')),
        });
        return T.of(undefined);
      },
      (params) => pipe(
        getLoggedInScietyUser(ports, context),
        O.match(
          () => {
            if (context.session) {
              context.session.successRedirect = constructRedirectUrl(context);
            }
            context.redirect('/log-in');
            return T.of(undefined);
          },
          (userDetails) => {
            context.redirect('back');
            return pipe(
              followCommand(ports)(userDetails.id, params.groupId),
              T.chain(() => next),
            );
          },
        ),
      ),
    ),
  )();
};
