import * as E from 'fp-ts/Either';
import * as T from 'fp-ts/Task';
import { pipe } from 'fp-ts/function';
import { Middleware } from 'koa';
import { encodedCommandFieldName } from './save-command';
import { generateEvents, SaveArticleEvents, Ports as SavedArticlePorts } from '../save-article';
import { CommandFromString } from '../types/command';
import { User } from '../types/user';

type Ports = SavedArticlePorts & {
  commitEvents: (events: ReadonlyArray<SaveArticleEvents>) => T.Task<void>,
};

export const finishSavedCommand = (
  ports: Ports,
): Middleware => async (context, next) => {
  const user = context.state.user as User;
  await pipe(
    context.session[encodedCommandFieldName],
    CommandFromString.decode,
    E.fold(
      () => T.of(undefined),
      (command) => pipe(
        generateEvents(ports)(user, command),
        T.chain(ports.commitEvents),
        T.map(() => {
          delete context.session[encodedCommandFieldName];
          return undefined;
        }),
      ),
    ),
  )();

  context.redirect('back');
  await next();
};
