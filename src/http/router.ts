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
import bodyParser from 'koa-bodyparser';
import send from 'koa-send';
import { handleScietyApiCommand } from './api/handle-sciety-api-command';
import { editListDetails } from './forms/edit-list-details';
import { removeArticleFromList } from './forms/remove-article-from-list';
import { loadStaticFile } from './load-static-file';
import { ownedBy } from './owned-by-api';
import { pageHandler } from './page-handler';
import { ping } from './ping';
import { redirectBack } from './redirect-back';
import { requireLoggedInUser } from './require-logged-in-user';
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
import { generateDocmaps } from '../docmaps/docmap';
import { docmapIndex } from '../docmaps/docmap-index';
import { hardcodedDocmaps } from '../docmaps/hardcoded-elife-docmaps';
import { editListDetailsFormPage, editListDetailsFormPageParamsCodec } from '../html-pages/edit-list-details-form-page';
import { evaluationContent, paramsCodec as evaluationContentParams } from '../evaluation-content';
import {
  executeFollowCommandIfUserLoggedIn, saveUnfollowCommand, unfollowHandler,
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
import { learnAboutPage } from '../html-pages/learn-about-page';
import { legalPage } from '../html-pages/legal-page';
import { menuPage, menuPageLayout } from '../html-pages/menu-page/menu-page-layout';
import { myFeedPage, myFeedParams } from '../html-pages/my-feed-page';
import { recordEvaluationCommandHandler } from '../write-side/record-evaluation';
import { removeArticleFromListCommandHandler } from '../write-side/remove-article-from-list';
import { respondHandler } from '../write-side/respond';
import { saveRespondCommand } from '../write-side/respond/save-respond-command';
import { finishSaveArticleCommand } from '../write-side/save-article/finish-save-article-command';
import { saveSaveArticleCommand } from '../write-side/save-article/save-save-article-command';
import { scietyFeedCodec, scietyFeedPage } from '../html-pages/sciety-feed-page/sciety-feed-page';
import { searchPage } from '../html-pages/search-page';
import { searchResultsPage, paramsCodec as searchResultsPageParams } from '../html-pages/search-results-page';
import { DoiFromString } from '../types/codecs/DoiFromString';
import { userIdCodec } from '../types/user-id';
import { CommandHandler, GenericCommand } from '../types/command-handler';
import * as DE from '../types/data-error';
import { toHtmlFragment } from '../types/html-fragment';
import { Page } from '../types/page';
import { RenderPageError } from '../types/render-page-error';
import { userPage, userPageParams } from '../html-pages/user-page';
import { getLoggedInScietyUser } from './authentication-and-logging-in-of-sciety-users';
import * as authentication from './authentication';
import { createUserAccountCommandHandler } from '../write-side/create-user-account';
import { createUserAccountCommandCodec } from '../write-side/commands/create-user-account';
import { contentOnlyLayout } from '../shared-components/content-only-layout';

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
  user: tt.optionFromNullable(t.type({ id: userIdCodec })),
});

export const createRouter = (adapters: CollectedPorts): Router => {
  const router = new Router();

  // PAGES

  router.get(
    '/',
    pageHandler(adapters, () => TE.right(homePage(adapters)), homePageLayout),
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
      context.response.body = pipe(
        context.request.header.referer,
        O.fromNullable,
        menuPage(getLoggedInScietyUser(adapters, context)),
        menuPageLayout(getLoggedInScietyUser(adapters, context)),
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
    '/users/:handle/lists',
    pageHandler(adapters, createPageFromParams(
      userPageParams,
      userPage(adapters)('lists'),
    )),
  );

  router.get(
    '/users/:handle/following',
    pageHandler(adapters, createPageFromParams(
      userPageParams,
      userPage(adapters)('followed-groups'),
    )),
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
    '/articles',
    async (context, next) => {
      context.status = StatusCodes.PERMANENT_REDIRECT;
      context.redirect('/search');

      await next();
    },
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
    '/articles/activity/:doi(.+)',
    pageHandler(adapters, flow(
      articlePageParams.decode,
      E.mapLeft(toNotFound),
      TE.fromEither,
      TE.chain(articlePage(adapters)),
    )),
  );

  router.get(
    '/evaluations/:reviewid/content',
    pageHandler(
      adapters,
      createPageFromParams(
        evaluationContentParams,
        evaluationContent(adapters),
      ),
      contentOnlyLayout,
    ),
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
    '/annotations/create-annotation-form-avasthi-reading',
    pageHandler(adapters, createPageFromParams(
      createAnnotationFormPageParamsCodec,
      createAnnotationFormPage,
    )),
  );

  router.redirect('/blog', 'https://blog.sciety.org', StatusCodes.PERMANENT_REDIRECT);

  const mailChimpUrl = 'https://us10.list-manage.com/contact-form?u=cdd934bce0d72af033c181267&form_id=4034dccf020ca9b50c404c32007ee091';
  router.redirect('/contact-us', mailChimpUrl, StatusCodes.PERMANENT_REDIRECT);

  router.redirect('/subscribe-to-mailing-list', 'http://eepurl.com/hBml3D', StatusCodes.PERMANENT_REDIRECT);

  router.get(
    '/legal',
    pageHandler(adapters, () => pipe(legalPage, TE.right)),
  );

  // COMMANDS

  router.post(
    '/follow',
    bodyParser({ enableTypes: ['form'] }),
    executeFollowCommandIfUserLoggedIn(adapters),
  );

  router.post(
    '/unfollow',
    bodyParser({ enableTypes: ['form'] }),
    saveUnfollowCommand(),
    requireLoggedInUser(adapters),
    unfollowHandler(adapters),
  );

  router.post(
    '/respond',
    bodyParser({ enableTypes: ['form'] }),
    saveRespondCommand,
    requireLoggedInUser(adapters),
    respondHandler(adapters),
  );

  router.post(
    '/save-article',
    bodyParser({ enableTypes: ['form'] }),
    saveSaveArticleCommand,
    requireLoggedInUser(adapters),
    finishSaveArticleCommand(adapters),
    redirectBack,
  );

  router.post(
    '/forms/remove-article-from-list',
    bodyParser({ enableTypes: ['form'] }),
    requireLoggedInUser(adapters),
    removeArticleFromList(adapters),
    redirectBack,
  );

  router.post(
    '/forms/edit-list-details',
    bodyParser({ enableTypes: ['form'] }),
    editListDetails(adapters),
  );

  router.get('/api/lists/owned-by/:ownerId', ownedBy(adapters));

  router.post('/api/record-evaluation', handleScietyApiCommand(adapters, recordEvaluationCommandHandler(adapters)));

  router.post('/api/add-article-to-list', createApiRouteForCommand(adapters, addArticleToListCommandCodec, addArticleToListCommandHandler(adapters)));

  router.post('/api/remove-article-from-list', createApiRouteForCommand(adapters, removeArticleFromListCommandCodec, removeArticleFromListCommandHandler(adapters)));

  router.post('/api/edit-list-details', createApiRouteForCommand(adapters, editListDetailsCommandCodec, adapters.editListDetails));

  router.post('/api/add-group', handleScietyApiCommand(adapters, addGroupCommandHandler(adapters)));

  router.post('/api/create-user', createApiRouteForCommand(adapters, createUserAccountCommandCodec, createUserAccountCommandHandler(adapters)));

  router.post(
    '/annotations/create-annotation',
    supplyFormSubmissionTo(adapters, handleCreateAnnotationCommand(adapters)),
  );

  // AUTHENTICATION

  authentication.configureRoutes(router, adapters);

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
