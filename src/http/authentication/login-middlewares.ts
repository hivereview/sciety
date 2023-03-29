import {
  DefaultContext, DefaultState, Middleware,
} from 'koa';
import * as O from 'fp-ts/Option';
import koaPassport from 'koa-passport';
import { pipe } from 'fp-ts/function';
import * as tt from 'io-ts-types';
import { getLoggedInScietyUser, Ports as GetLoggedInScietyUserPorts } from '../authentication-and-logging-in-of-sciety-users';
import { redirectToAuthenticationDestination } from '../authentication-destination';

const oAuthScope = 'openid profile';

// https://auth0.com/docs/authenticate/login/auth0-universal-login/new-experience#signup
// https://github.com/auth0/passport-auth0/pull/131
const takeUsersDirectlyToAuth0Signup = 'signup';

const customSignUpParameters = {
  screen_hint: takeUsersDirectlyToAuth0Signup,
};

// Workaround for out of date @types/koa-passport
// https://github.com/DefinitelyTyped/DefinitelyTyped/pull/64621
type ContextPatchedForKoaPassport = Exclude<DefaultContext, 'logout'> & { 'logout': () => Promise<void> };

const removeKoaPassportDataFromSession = async (context: ContextPatchedForKoaPassport) => {
  await context.logout();
};

const targetPageAfterLogOut = '/';

export const signUpAuth0: Middleware = koaPassport.authenticate('auth0', {
  failureRedirect: '/',
  scope: oAuthScope,
  ...customSignUpParameters,
});

export const logInAuth0: Middleware = koaPassport.authenticate('auth0', {
  failureRedirect: '/',
  scope: oAuthScope,
});

export type Config = {
  APP_ORIGIN: tt.NonEmptyString,
};

export const logOutAuth0 = (
  config: Config,
): Middleware<DefaultState, ContextPatchedForKoaPassport> => async (context, next) => {
  await removeKoaPassportDataFromSession(context);
  const domain = process.env.AUTH0_DOMAIN ?? '';
  const clientId = process.env.AUTH0_CLIENT_ID ?? '';
  const app = config.APP_ORIGIN;
  const auth0logout = `https://${domain}/v2/logout?client_id=${clientId}&returnTo=${app}${targetPageAfterLogOut}`;
  context.redirect(auth0logout);

  await next();
};

export const stubSignUpAuth0 = koaPassport.authenticate('local', {
  failureRedirect: '/local/log-in-form',
});

export const stubLogInAuth0 = koaPassport.authenticate('local', {
  failureRedirect: '/local/log-in-form',
});

export const stubLogOutAuth0: Middleware<DefaultState, ContextPatchedForKoaPassport> = async (context, next) => {
  await removeKoaPassportDataFromSession(context);
  context.redirect(targetPageAfterLogOut);

  await next();
};

export const completeAuthenticationJourney = (
  adapters: GetLoggedInScietyUserPorts,
): Middleware => async (context, next) => {
  pipe(
    getLoggedInScietyUser(adapters, context),
    O.match(
      () => context.redirect('/create-account-form'),
      () => redirectToAuthenticationDestination(context),
    ),
  );
  await next();
};
