import { Middleware, ParameterizedContext } from 'koa';
import * as O from 'fp-ts/Option';
import koaPassport from 'koa-passport';
import { pipe } from 'fp-ts/function';
import * as tt from 'io-ts-types';
import { getLoggedInScietyUser, Ports as GetLoggedInScietyUserPorts, referringPage } from '../authentication-and-logging-in-of-sciety-users';

const oAuthScope = 'openid profile';

// https://auth0.com/docs/authenticate/login/auth0-universal-login/new-experience#signup
// https://github.com/auth0/passport-auth0/pull/131
const takeUsersDirectlyToAuth0Signup = 'signup';

const customSignUpParameters = {
  screen_hint: takeUsersDirectlyToAuth0Signup,
};

const removeLocalBrowserSession = (context: ParameterizedContext) => {
  context.logout();
  delete context.session.successRedirect;
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

export const logOutAuth0 = (config: Config): Middleware => async (context, next) => {
  removeLocalBrowserSession(context);
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

export const stubLogOutAuth0: Middleware = async (context, next) => {
  removeLocalBrowserSession(context);
  context.redirect(targetPageAfterLogOut);

  await next();
};

export const completeAuthenticationJourney = (
  adapters: GetLoggedInScietyUserPorts,
): Middleware => async (context, next) => {
  pipe(
    getLoggedInScietyUser(adapters, context),
    O.match(
      () => '/create-account-form',
      () => referringPage(context),
    ),
    (page) => context.redirect(page),
  );
  await next();
};
