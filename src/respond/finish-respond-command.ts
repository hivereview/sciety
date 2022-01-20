import { sequenceS } from 'fp-ts/Apply';
import * as O from 'fp-ts/Option';
import * as T from 'fp-ts/Task';
import { flow, pipe } from 'fp-ts/function';
import { Middleware } from 'koa';
import {
  commandHandler, CommitEvents, GetAllEvents, toCommand,
} from './command-handler';
import { reviewIdCodec } from '../types/review-id';

type Ports = {
  commitEvents: CommitEvents,
  getAllEvents: GetAllEvents,
};

export const finishRespondCommand = (ports: Ports): Middleware => async (context, next) => {
  const userId = context.state.user.id;
  await pipe(
    // TODO: move userId, reviewId, command into a new type that gets constructed by a validator
    {
      reviewId: pipe(context.session.reviewId, reviewIdCodec.decode, O.fromEither),
      command: pipe(context.session.command, toCommand),
    },
    sequenceS(O.Apply),
    O.fold(
      () => T.of(undefined),
      flow(
        commandHandler(
          ports.commitEvents,
          ports.getAllEvents,
          userId,
        ),
        T.map(() => {
          delete context.session.command;
          delete context.session.reviewId;
          return undefined;
        }),
      ),
    ),
  )();

  await next();
};
