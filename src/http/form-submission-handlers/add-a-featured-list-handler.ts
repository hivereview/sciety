import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { StatusCodes } from 'http-status-codes';
import * as t from 'io-ts';
import * as tt from 'io-ts-types';
import { Middleware } from 'koa';
import { decodeFormSubmission, Dependencies as DecodeFormSubmissionDependencies } from './decode-form-submission';
import { ensureUserIsLoggedIn, Dependencies as EnsureUserIsLoggedInDependencies } from './ensure-user-is-logged-in';
import { promoteListCommandCodec } from '../../write-side/commands';
import { DependenciesForCommands } from '../../write-side/dependencies-for-commands';
import * as listPromotion from '../../write-side/resources/list-promotion';
import { sendDefaultErrorHtmlResponse } from '../send-default-error-html-response';

const formBodyCodec = t.intersection([
  promoteListCommandCodec,
  t.strict({
    successRedirectPath: tt.NonEmptyString,
  }),
]);

type Dependencies = EnsureUserIsLoggedInDependencies
& DecodeFormSubmissionDependencies & DependenciesForCommands;

export const addAFeaturedListHandler = (dependencies: Dependencies): Middleware => async (context) => {
  if (process.env.EXPERIMENT_ENABLED === 'true') {
    const loggedInUser = ensureUserIsLoggedIn(dependencies, context, 'You must be logged in to feature a list.');
    if (O.isNone(loggedInUser)) {
      return;
    }
    const formBody = decodeFormSubmission(
      dependencies,
      context,
      formBodyCodec,
      loggedInUser.value.id,
    );
    if (E.isLeft(formBody)) {
      return;
    }
    const command = {
      forGroup: formBody.right.forGroup,
      listId: formBody.right.listId,
    };

    const commandResult = await pipe(
      dependencies.getAllEvents,
      T.map(listPromotion.create(command)),
      TE.chainW(dependencies.commitEvents),
    )();

    if (E.isRight(commandResult)) {
      context.redirect(formBody.right.successRedirectPath);
      return;
    }
    dependencies.logger('error', 'Command execution failed', { command });
    sendDefaultErrorHtmlResponse(dependencies, context, StatusCodes.INTERNAL_SERVER_ERROR, 'An unexpected error occurred.');
  }
};
