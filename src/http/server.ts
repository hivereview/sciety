import { createServer, Server } from 'http';
import Router from '@koa/router';
import rTracer from 'cls-rtracer';
import * as E from 'fp-ts/Either';
import Koa, { Middleware } from 'koa';
import koaPassport from 'koa-passport';
import koaSession from 'koa-session';
import { setupAuth0Strategy } from './authentication/setup-auth0-strategy';
import { setupLocalStrategy } from './authentication/setup-local-strategy';
import { setupTwitterStrategy } from './authentication/setup-twitter-strategy';
import { routeNotFound } from './route-not-found';
import { CollectedPorts } from '../infrastructure';

export const createApplicationServer = (router: Router, ports: CollectedPorts): E.Either<string, Server> => {
  const app = new Koa();
  const { logger } = ports;

  app.use(rTracer.koaMiddleware());

  app.use(async ({ request, res }, next) => {
    const logLevel = request.url.startsWith('/static') ? 'debug' : 'info';
    logger(logLevel, 'Received HTTP request', {
      method: request.method,
      url: request.url,
      referer: request.headers.referer,
    });

    res.once('finish', () => {
      logger(logLevel, 'Sent HTTP response', {
        status: res.statusCode,
      });
    });

    res.once('close', () => {
      if (res.writableFinished) {
        return;
      }

      logger('warn', 'HTTP response may not have been completely sent', {
        status: res.statusCode,
      });
    });

    await next();
  });

  const requiredEnvironmentVariables = [
    'APP_ORIGIN',
    'APP_SECRET',
    'PGUSER',
    'PGHOST',
    'PGPASSWORD',
    'PGDATABASE',
    'TWITTER_API_KEY',
    'TWITTER_API_SECRET_KEY',
    'TWITTER_API_BEARER_TOKEN',
  ];

  const missingVariables = requiredEnvironmentVariables.filter((variableName) => !process.env[variableName]);
  if (missingVariables.length) {
    logger('error', 'Missing environment variables', { missingVariables });
    return E.left(`Missing ${missingVariables.join(', ')} from environment variables`);
  }

  const isSecure = process.env.APP_ORIGIN?.startsWith('https:');
  if (isSecure) {
    app.use(async (ctx, next) => {
      ctx.cookies.secure = true;
      await next();
    });
  }

  app.keys = [process.env.APP_SECRET ?? 'this-is-not-secret'];
  app.use(koaSession(
    {
      maxAge: 365 * 24 * 60 * 60 * 1000,
      secure: isSecure,
    },
    app,
  ));

  if (process.env.AUTHENTICATION_STRATEGY === 'local') {
    koaPassport.use(setupLocalStrategy(ports));
  } else {
    koaPassport.use(setupTwitterStrategy(ports));
    koaPassport.use(setupAuth0Strategy(ports));
  }

  app.use(koaPassport.initialize());
  app.use(koaPassport.session());

  koaPassport.serializeUser((user, done) => {
    done(null, user);
  });

  koaPassport.deserializeUser((user, done) => {
    done(null, user);
  });

  const checkUserDetails: Middleware = async (context, next) => {
    if (context.state.user) {
      if (!context.state.user.handle || !context.state.user.avatarUrl) {
        logger('info', 'Logging out user', { user: context.state.user });
        context.logout();
      }
    }

    await next();
  };

  app.use(checkUserDetails);
  app.use(router.middleware());
  app.use(routeNotFound);

  app.on('error', (error) => {
    const payload = { error };
    if (error instanceof Error) {
      payload.error = {
        message: error.message,
        stack: error.stack,
      };
    }
    logger('error', 'Unhandled Error', payload);
  });

  const server = createServer(app.callback());

  server.on('clientError', (error, socket) => {
    if (!socket.writable) {
      logger('info', 'Non writable socket, no response sent', {
        error,
      });
      return;
    }

    socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');

    logger('info', 'Sent early HTTP response due to client error', {
      status: 400,
      error,
    });
  });

  return E.right(server);
};
