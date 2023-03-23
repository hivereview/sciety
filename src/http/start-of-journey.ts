import { pipe } from 'fp-ts/function';
import * as t from 'io-ts';
import { ParameterizedContext } from 'koa';
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

const referringPage = (context: ParameterizedContext): string => pipe(
  context,
  getStartOfJourney,
  E.getOrElse(() => '/'),
);

const constructRedirectUrl = (context: ParameterizedContext): string => (
  context.request.headers.referer ?? '/'
);

export const rememberPreviousPageAsStartOfJourney = (context: ParameterizedContext) => {
  context.session.startOfJourney = constructRedirectUrl(context);
};

export const rememberPreviousPageAsStartOfJourneyIfWeDontAlreadyKnowIt = (context: ParameterizedContext) => {
  if (!context.session.startOfJourney) {
    context.session.startOfJourney = context.request.headers.referer ?? '/';
  }
};

export const redirectToStartOfJourney = (context: ParameterizedContext) => {
  context.redirect(referringPage(context));
};
