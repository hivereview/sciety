import { pipe } from 'fp-ts/function';
import * as t from 'io-ts';
import { Middleware, ParameterizedContext } from 'koa';
import * as E from 'fp-ts/Either';

const referringPageCodec = t.type({
  session: t.type({
    startOfJourney: t.string,
  }),
});

const getStartOfJourney = (context: ParameterizedContext) => pipe(
  context,
  referringPageCodec.decode,
  E.map((ctx) => ctx.session.startOfJourney),
);

export const rememberPreviousPageAsStartOfJourney: Middleware = async (context: ParameterizedContext, next) => {
  context.session.startOfJourney = context.request.headers.referer ?? '/';
  await next();
};

export const redirectToStartOfJourney = (context: ParameterizedContext) => {
  const target = pipe(
    context,
    getStartOfJourney,
    E.getOrElse(() => '/'),
  );
  context.redirect(target);
  delete context.session.startOfJourney;
};
