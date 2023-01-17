import path from 'path';
import Router from '@koa/router';
import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as R from 'fp-ts/Record';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { flow, pipe } from 'fp-ts/function';
import { StatusCodes } from 'http-status-codes';
import * as t from 'io-ts';
import * as tt from 'io-ts-types';
import { ParameterizedContext } from 'koa';
import bodyParser from 'koa-bodyparser';
import send from 'koa-send';
import { handleScietyApiCommand } from './api/handle-sciety-api-command';
import {
  logIn, logInAsSpecificUser, logInCallback, signUpAuth0,
} from './authentication/login-middlewares';
import { catchErrors } from './catch-errors';
import { finishCommand } from './finish-command';
import { createUserAccount } from './forms/create-user-account';
import { editListDetails } from './forms/edit-list-details';
import { removeArticleFromList } from './forms/remove-article-from-list';
import { loadStaticFile } from './load-static-file';
import { logOut } from './log-out';
import { onlyIfNotAuthenticated } from './only-if-authenticated';
import { ownedBy } from './owned-by-api';
import { pageHandler } from './page-handler';
import { ping } from './ping';
import { redirectBack } from './redirect-back';
import { redirectUserIdToHandle } from './redirects/redirect-user-id-to-handle';
import { redirectUserListPageToGenericListPage } from './redirects/redirect-user-list-page-to-generic-list-page';
import { redirectAfterAuthenticating, requireAuthentication } from './require-authentication';
import { robots } from './robots';
import { readModelStatus } from '../add-article-to-elife-subject-area-list';
import { addArticleToListCommandHandler } from '../write-side/add-article-to-list';
import { addGroupCommandHandler } from '../write-side/add-group';
import { createAnnotationFormPage, paramsCodec as createAnnotationFormPageParamsCodec } from '../annotations/create-annotation-form-page';
import { handleCreateAnnotationCommand } from '../annotations/handle-create-annotation-command';
import { supplyFormSubmissionTo } from '../annotations/supply-form-submission-to';
import {
  addArticleToListCommandCodec, editListDetailsCommandCodec, removeArticleFromListCommandCodec,
} from '../write-side/commands';
import { validateInputShape } from '../write-side/commands/validate-input-shape';
import { createUserAccountFormPage } from '../create-user-account-form-page/create-user-account-form-page';
import { generateDocmaps } from '../docmaps/docmap';
import { docmapIndex } from '../docmaps/docmap-index';
import { hardcodedDocmaps } from '../docmaps/hardcoded-elife-docmaps';
import { editListDetailsFormPage, editListDetailsFormPageParamsCodec } from '../edit-list-details-form-page';
import { evaluationContent, paramsCodec as evaluationContentParams } from '../evaluation-content';
import {
  executeIfAuthenticated, finishUnfollowCommand, saveUnfollowCommand, unfollowHandler,
} from '../write-side/follow';
import { aboutPage } from '../html-pages/about-page';
import { actionFailedPage, actionFailedPageParamsCodec } from '../html-pages/action-failed';
import { articlePage } from '../html-pages/article-page';
import {
  groupPage, paramsCodec as groupPageParamsCodec, groupPageTabs,
} from '../html-pages/group-page/group-page';
import { groupsPage } from '../html-pages/groups-page';
import { homePage, homePageLayout } from '../html-pages/home-page';
import { page as listPage, paramsCodec as listPageParams } from '../html-pages/list-page/page';
import { CollectedPorts } from '../infrastructure';
import { learnAboutPage } from '../learn-about-page';
import { legalPage } from '../legal-page';
import { menuPageLayout } from '../menu-page/menu-page-layout';
import { myFeedPage, myFeedParams } from '../my-feed-page';
import { recordEvaluationCommandHandler } from '../write-side/record-evaluation';
import { removeArticleFromListCommandHandler } from '../write-side/remove-article-from-list';
import { respondHandler } from '../write-side/respond';
import { finishRespondCommand } from '../write-side/respond/finish-respond-command';
import { saveRespondCommand } from '../write-side/respond/save-respond-command';
import { finishSaveArticleCommand } from '../write-side/save-article/finish-save-article-command';
import { saveSaveArticleCommand } from '../write-side/save-article/save-save-article-command';
import { scietyFeedCodec, scietyFeedPage } from '../sciety-feed-page/sciety-feed-page';
import { searchPage } from '../search-page';
import { searchResultsPage, paramsCodec as searchResultsPageParams } from '../search-results-page';
import { signUpPage } from '../sign-up-page';
import { DoiFromString } from '../types/codecs/DoiFromString';
import { UserIdFromString } from '../types/codecs/UserIdFromString';
import { CommandHandler, GenericCommand } from '../types/command-handler';
import * as DE from '../types/data-error';
import { toHtmlFragment } from '../types/html-fragment';
import { Page } from '../types/page';
import { RenderPageError } from '../types/render-page-error';
import { userCodec } from '../types/user';
import { userPage } from '../user-page/user-page';
import { getLoggedInScietyUser } from './get-logged-in-sciety-user';

const toNotFound = () => ({
  type: DE.notFound,
  message: toHtmlFragment('Page not found'),
});

const createApiRouteForCommand = <C extends GenericCommand>(
  adapters: CollectedPorts,
  codec: t.Decoder<unknown, C>,
  commandHandler: CommandHandler<C>,
) => handleScietyApiCommand(adapters, flow(
    validateInputShape(codec),
    TE.fromEither,
    TE.chain(commandHandler),
  ));

type GeneratePage<P> = (params: P) => TE.TaskEither<RenderPageError, Page>;

const createPageFromParams = <P>(codec: t.Decoder<unknown, P>, generatePage: GeneratePage<P>) => flow(
  codec.decode,
  E.mapLeft(toNotFound),
  TE.fromEither,
  TE.chain(generatePage),
);

const articlePageParams = t.type({
  doi: DoiFromString,
  user: tt.optionFromNullable(userCodec),
});

const userPageParams = t.type({
  handle: t.string,
  user: tt.optionFromNullable(t.type({
    id: UserIdFromString,
  })),
});

export const createRouter = (adapters: CollectedPorts): Router => {
  const router = new Router();

  const toSuccessResponse = (body: string) => ({
    body,
    status: StatusCodes.OK,
  });

  // PAGES

  router.get(
    '/',
    async (context, next) => {
      const response = pipe(
        adapters,
        homePage,
        homePageLayout(getLoggedInScietyUser(adapters, context)),
        toSuccessResponse,
      );

      context.response.status = response.status;
      context.response.type = 'html';
      context.response.body = response.body;
      await next();
    },
  );

  router.get(
    '/my-feed',
    pageHandler(adapters, createPageFromParams(
      myFeedParams,
      myFeedPage(adapters),
    )),
  );

  router.get(
    '/sciety-feed',
    pageHandler(adapters, createPageFromParams(
      scietyFeedCodec,
      scietyFeedPage(adapters)(20),
    )),
  );

  router.get(
    '/menu',
    async (context, next) => {
      context.response.status = StatusCodes.OK;
      context.response.type = 'html';
      context.response.body = menuPageLayout(
        getLoggedInScietyUser(adapters, context),
        O.fromNullable(context.request.header.referer),
      );
      context.set('Vary', 'Referer');

      await next();
    },
  );

  router.get(
    '/about',
    pageHandler(adapters, () => aboutPage(adapters.fetchStaticFile)),
  );

  router.get(
    '/action-failed',
    pageHandler(adapters,
      createPageFromParams(
        actionFailedPageParamsCodec,
        actionFailedPage,
      )),
  );

  router.get(
    '/learn-about',
    pageHandler(adapters, () => pipe(learnAboutPage, TE.right)),
  );

  router.get(
    '/users/:descriptor',
    async (context, next) => {
      context.status = StatusCodes.TEMPORARY_REDIRECT;
      context.redirect(`/users/${context.params.descriptor}/lists`);

      await next();
    },
  );

  router.get(
    '/users/:id/followed-groups',
    async (context, next) => {
      context.status = StatusCodes.PERMANENT_REDIRECT;
      context.redirect(`/users/${context.params.id}/following`);

      await next();
    },
  );

  const matchHandle = '[^0-9][^/]+';

  router.get(
    `/users/:handle(${matchHandle})/saved-articles`,
    async (context, next) => {
      context.status = StatusCodes.PERMANENT_REDIRECT;
      context.redirect(`/users/${context.params.handle}/lists`);

      await next();
    },
  );

  router.get(
    `/users/:handle(${matchHandle})/lists`,
    pageHandler(adapters, createPageFromParams(
      userPageParams,
      userPage(adapters)('lists'),
    )),
  );

  router.get(
    '/users/:id([0-9]+)/lists',
    redirectUserIdToHandle(adapters, 'lists'),
  );

  router.get(
    `/users/:handle(${matchHandle})/following`,
    pageHandler(adapters, createPageFromParams(
      userPageParams,
      userPage(adapters)('followed-groups'),
    )),
  );

  router.get(
    '/users/:id([0-9]+)/following',
    redirectUserIdToHandle(adapters, 'following'),
  );

  router.get(
    `/users/:handle(${matchHandle})/lists/saved-articles`,
    redirectUserListPageToGenericListPage(adapters),
  );

  router.get(
    '/users/:id([0-9]+)/lists/saved-articles',
    redirectUserIdToHandle(adapters, 'lists/saved-articles'),
  );

  router.get(
    '/articles',
    async (context, next) => {
      context.status = StatusCodes.PERMANENT_REDIRECT;
      context.redirect('/search');

      await next();
    },
  );

  router.get(
    '/search',
    async (context, next) => {
      context.response.set('X-Robots-Tag', 'noindex');
      await next();
    },
    pageHandler(adapters, flow(
      searchResultsPageParams.decode,
      E.fold(
        () => TE.right(searchPage),
        searchResultsPage(adapters)(20),
      ),
    )),
  );

  router.get(
    '/articles/:doi(10\\..+)',
    async (context, next) => {
      context.status = StatusCodes.PERMANENT_REDIRECT;
      context.redirect(`/articles/activity/${context.params.doi}`);

      await next();
    },
  );

  router.get(
    '/articles/meta/:doi(.+)',
    async (context, next) => {
      context.status = StatusCodes.TEMPORARY_REDIRECT;
      context.redirect(`/articles/activity/${context.params.doi}`);

      await next();
    },
  );

  router.get(
    '/evaluations/:reviewid/content',
    pageHandler(adapters, createPageFromParams(
      evaluationContentParams,
      evaluationContent(adapters),
    ), false),
  );

  router.get(
    '/articles/activity/:doi(.+)',
    pageHandler(adapters, flow(
      articlePageParams.decode,
      E.mapLeft(toNotFound),
      TE.fromEither,
      TE.chain(articlePage(adapters)),
    )),
  );

  router.get(
    '/groups',
    pageHandler(adapters, () => groupsPage(adapters)),
  );

  router.get(
    '/groups/:idOrSlug',
    async (context, next) => {
      context.status = StatusCodes.TEMPORARY_REDIRECT;
      context.redirect(`/groups/${context.params.idOrSlug}/about`);

      await next();
    },
  );

  router.get(
    '/groups/:slug/lists',
    pageHandler(adapters, createPageFromParams(
      groupPageParamsCodec,
      groupPage(adapters)(groupPageTabs.lists),
    )),
  );

  router.get(
    '/groups/:slug/about',
    pageHandler(adapters, createPageFromParams(
      groupPageParamsCodec,
      groupPage(adapters)(groupPageTabs.about),
    )),
  );

  router.get(
    '/groups/:slug/followers',
    pageHandler(adapters, createPageFromParams(
      groupPageParamsCodec,
      groupPage(adapters)(groupPageTabs.followers),
    )),
  );

  router.get(
    '/groups/:slug/evaluated-articles',
    async (context, next) => {
      context.status = StatusCodes.PERMANENT_REDIRECT;
      context.redirect(`/groups/${context.params.slug}`);

      await next();
    },
  );

  router.get(
    '/lists/:id',
    pageHandler(adapters, createPageFromParams(
      listPageParams,
      listPage(adapters),
    )),
  );

  router.get(
    '/lists/:id/edit-details',
    pageHandler(adapters, createPageFromParams(
      editListDetailsFormPageParamsCodec,
      editListDetailsFormPage(adapters),
    )),
  );

  router.get(
    '/create-account-form',
    pageHandler(adapters, () => pipe(createUserAccountFormPage, TE.right)),
  );

  router.get(
    '/annotations/create-annotation-form-avasthi-reading',
    pageHandler(adapters, createPageFromParams(
      createAnnotationFormPageParamsCodec,
      createAnnotationFormPage,
    )),
  );

  router.redirect('/privacy', '/legal', StatusCodes.PERMANENT_REDIRECT);

  router.redirect('/terms', '/legal', StatusCodes.PERMANENT_REDIRECT);

  router.redirect('/blog', 'https://blog.sciety.org', StatusCodes.PERMANENT_REDIRECT);

  router.redirect('/feedback', 'http://eepurl.com/hBml3D', StatusCodes.PERMANENT_REDIRECT);

  const mailChimpUrl = 'https://us10.list-manage.com/contact-form?u=cdd934bce0d72af033c181267&form_id=4034dccf020ca9b50c404c32007ee091';
  router.redirect('/contact-us', mailChimpUrl, StatusCodes.PERMANENT_REDIRECT);

  router.redirect('/subscribe-to-mailing-list', 'http://eepurl.com/hBml3D', StatusCodes.PERMANENT_REDIRECT);

  router.get(
    '/legal',
    pageHandler(adapters, () => pipe(legalPage, TE.right)),
  );

  router.get(
    '/sign-up',
    pageHandler(adapters, () => pipe(signUpPage, TE.right)),
  );

  // COMMANDS

  router.post(
    '/follow',
    bodyParser({ enableTypes: ['form'] }),
    executeIfAuthenticated(adapters),
  );

  router.post(
    '/unfollow',
    bodyParser({ enableTypes: ['form'] }),
    saveUnfollowCommand(),
    requireAuthentication(adapters),
    unfollowHandler(adapters),
  );

  router.post(
    '/respond',
    bodyParser({ enableTypes: ['form'] }),
    saveRespondCommand,
    requireAuthentication(adapters),
    respondHandler(adapters),
  );

  router.post(
    '/save-article',
    bodyParser({ enableTypes: ['form'] }),
    saveSaveArticleCommand,
    requireAuthentication(adapters),
    finishSaveArticleCommand(adapters),
    redirectBack,
  );

  router.post(
    '/forms/remove-article-from-list',
    bodyParser({ enableTypes: ['form'] }),
    requireAuthentication(adapters),
    removeArticleFromList(adapters),
    redirectBack,
  );

  router.post(
    '/forms/edit-list-details',
    bodyParser({ enableTypes: ['form'] }),
    editListDetails(adapters),
  );

  router.post(
    '/forms/create-user-account',
    bodyParser({ enableTypes: ['form'] }),
    createUserAccount(adapters),
  );

  router.get('/api/lists/owned-by/:ownerId', ownedBy(adapters));

  router.post('/api/record-evaluation', handleScietyApiCommand(adapters, recordEvaluationCommandHandler(adapters)));

  router.post('/api/add-article-to-list', createApiRouteForCommand(adapters, addArticleToListCommandCodec, addArticleToListCommandHandler(adapters)));

  router.post('/api/remove-article-from-list', createApiRouteForCommand(adapters, removeArticleFromListCommandCodec, removeArticleFromListCommandHandler(adapters)));

  router.post('/api/edit-list-details', createApiRouteForCommand(adapters, editListDetailsCommandCodec, adapters.editListDetails));

  router.post('/api/add-group', handleScietyApiCommand(adapters, addGroupCommandHandler(adapters)));

  router.post(
    '/annotations/create-annotation',
    supplyFormSubmissionTo(handleCreateAnnotationCommand(adapters)),
  );

  // AUTHENTICATION

  router.get(
    '/log-in',
    async (context: ParameterizedContext, next) => {
      if (!context.session.successRedirect) {
        context.session.successRedirect = context.request.headers.referer ?? '/';
      }
      await next();
    },
    logIn(process.env.AUTHENTICATION_STRATEGY === 'local' ? 'local' : 'twitter'),
  );

  if (process.env.AUTHENTICATION_STRATEGY === 'local') {
    router.get('/log-in-as', logInAsSpecificUser);
  }

  router.get(
    '/sign-up-auth0',
    async (context: ParameterizedContext, next) => {
      if (!context.session.successRedirect) {
        context.session.successRedirect = context.request.headers.referer ?? '/';
      }
      await next();
    },
    signUpAuth0,
  );

  router.get(
    '/sign-up-call-to-action',
    async (context: ParameterizedContext, next) => {
      context.session.successRedirect = '/';
      await next();
    },
    logIn(process.env.AUTHENTICATION_STRATEGY === 'local' ? 'local' : 'twitter'),
  );

  router.get('/log-out', logOut);

  // TODO set commands as an object on the session rather than individual properties
  router.get(
    '/twitter/callback',
    catchErrors(
      adapters.logger,
      'Detected Twitter callback error',
      'Something went wrong, please try again.',
    ),
    onlyIfNotAuthenticated(adapters, logInCallback(process.env.AUTHENTICATION_STRATEGY === 'local' ? 'local' : 'twitter')),
    finishCommand(adapters),
    finishUnfollowCommand(adapters),
    finishRespondCommand(adapters),
    finishSaveArticleCommand(adapters),
    redirectAfterAuthenticating(),
  );

  router.get(
    '/auth0/callback',
    catchErrors(
      adapters.logger,
      'Detected Auth0 callback error',
      'Something went wrong, please try again.',
    ),
    logInCallback('auth0'),
    async (context) => {
      context.redirect('/create-account-form');
    },
  );

  // DOCMAPS
  router.get('/docmaps/v1/index', async (context, next) => {
    const response = await docmapIndex(adapters)(context.query)();

    context.response.status = response.status;
    context.response.body = response.body;

    await next();
  });

  router.get('/docmaps/v1/articles/:doi(.+).docmap.json', async (context, next) => {
    const response = await pipe(
      context.params.doi,
      generateDocmaps(adapters),
      TE.foldW(
        (error) => T.of({
          body: { message: error.message },
          status: error.status,
        }),
        (body) => T.of({
          body,
          status: StatusCodes.OK,
        }),
      ),
    )();

    context.response.status = response.status;
    context.response.body = response.body;
    await next();
  });

  router.get('/docmaps/v1/evaluations-by/elife/:doi(.+).docmap.json', async (context, next) => {
    pipe(
      hardcodedDocmaps,
      R.lookup(context.params.doi),
      O.fold(
        () => {
          context.status = StatusCodes.NOT_FOUND;
          context.body = { message: 'No such hardcoded docmap.' };
          adapters.logger('error', 'No such hardcoded docmap.', { doi: context.params.doi });
        },
        (json) => {
          context.response.status = 200;
          context.response.body = json;
        },
      ),
    );

    await next();
  });

  router.get('/docmaps/v1', async (context, next) => {
    const staticFolder = path.resolve(__dirname, '../../static');
    await send(context, 'docmaps-v1-api-docs.html', { root: staticFolder });

    await next();
  });

  // OBSERVABILITY

  router.get('/elife-subject-area-read-model-status', async (context, next) => {
    context.response.body = readModelStatus(adapters);

    await next();
  });

  // MISC

  router.get('/ping', ping());

  router.get('/robots.txt', robots());

  router.get(
    '/static/:file(.+)',
    loadStaticFile(adapters.logger),
  );

  return router;
};
