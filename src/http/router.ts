import path from 'path';
import Router from '@koa/router';
import * as E from 'fp-ts/Either';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { flow, pipe } from 'fp-ts/function';
import { StatusCodes } from 'http-status-codes';
import send from 'koa-send';
import { loadStaticFile } from './load-static-file';
import { ownedBy } from './owned-by-api';
import { pageHandler } from './page-handler';
import { ping } from './ping';
import { requireLoggedInUser } from './require-logged-in-user';
import { robots } from './robots';
import { createAnnotationFormPage, paramsCodec as createAnnotationFormPageParamsCodec } from '../html-pages/create-annotation-form-page';
import {
  addArticleToListCommandCodec,
  editListDetailsCommandCodec,
  updateUserDetailsCommandCodec,
  removeArticleFromListCommandCodec,
  recordEvaluationPublicationCommandCodec,
  addGroupCommandCodec,
  eraseEvaluationCommandCodec,
  updateGroupDetailsCommandCodec,
  recordEvaluationRemovalCommandCodec,
  updateEvaluationCommandCodec,
} from '../write-side/commands';
import { generateDocmaps } from '../docmaps/docmap';
import { docmapIndex } from '../docmaps/docmap-index';
import { editListDetailsFormPage, editListDetailsFormPageParamsCodec } from '../html-pages/edit-list-details-form-page';
import { evaluationContent, paramsCodec as evaluationContentParams } from '../evaluation-content';
import { aboutPage } from '../html-pages/about-page';
import { actionFailedPage, actionFailedPageParamsCodec } from '../html-pages/action-failed';
import { articlePage } from '../html-pages/article-page';
import * as GLP from '../html-pages/group-page/group-lists-page';
import * as GAP from '../html-pages/group-page/group-about-page';
import * as GFP from '../html-pages/group-page/group-followers-page';
import * as GFEP from '../html-pages/group-page/group-feed-page';
import { groupsPage } from '../html-pages/groups-page';
import { homePage, homePageLayout } from '../html-pages/home-page';
import { page as listPage, paramsCodec as listPageParams } from '../html-pages/list-page';
import { CollectedPorts } from '../infrastructure';
import { legalPage } from '../html-pages/legal-page';
import { myFeedPage, myFeedParams } from '../html-pages/my-feed-page';

import { scietyFeedCodec, scietyFeedPage } from '../html-pages/sciety-feed-page';
import { searchPage } from '../html-pages/search-page';
import { searchResultsPage, paramsCodec as searchResultsPageParams } from '../html-pages/search-results-page';
import { userPage as userFollowingPage, userPageParams as userFollowingPageParams } from '../html-pages/user-page/user-following-page';
import { userPage as userListsPage, userPageParams as userListsPageParams } from '../html-pages/user-page/user-lists-page';
import * as authentication from './authentication';
import * as formSubmissionHandlers from './form-submission-handlers';
import { createUserAccountCommandCodec } from '../write-side/commands/create-user-account';
import { contentWithoutLayout } from '../shared-components/content-only-layout';
import { createPageFromParams } from './create-page-from-params';
import { Config as AuthenticationRoutesConfig } from './authentication/configure-routes';
import { listsPage } from '../html-pages/lists-page';
import { createApiRouteForResourceAction } from './create-api-route-for-resource-action';
import * as evaluationResource from '../write-side/resources/evaluation';
import * as groupResource from '../write-side/resources/group';
import * as listResource from '../write-side/resources/list';
import * as userResource from '../write-side/resources/user';
import { fullWidthPageLayout } from '../shared-components/full-width-page-layout';
import { applicationStatus } from '../views/status';
import { listFeed } from '../views/list/list-feed';
import { subscribeToListPage } from '../html-pages/subscribe-to-list-page';
import { statusGroups } from '../views/status-groups';
import { referencePage, sharedComponentsPage, indexPage } from '../html-pages/style-guide-page';
import { saveArticleFormPage } from '../html-pages/save-article-form-page';
import { htmlFragmentHandler } from './html-fragment-handler';

type Config = AuthenticationRoutesConfig;

export const createRouter = (adapters: CollectedPorts, config: Config): Router => {
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
    '/about',
    pageHandler(adapters, () => aboutPage({})),
  );

  router.get(
    '/lists/:listId/subscribe',
    pageHandler(adapters, subscribeToListPage(adapters)),
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
      userListsPageParams,
      userListsPage(adapters),
    )),
  );

  router.get(
    '/users/:handle/following',
    pageHandler(adapters, createPageFromParams(
      userFollowingPageParams,
      userFollowingPage(adapters),
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
    pageHandler(adapters, articlePage(adapters), fullWidthPageLayout),
  );

  router.get(
    '/evaluations/:reviewid/content',
    htmlFragmentHandler(
      adapters,
      createPageFromParams(
        evaluationContentParams,
        evaluationContent(adapters),
      ),
      contentWithoutLayout,
    ),
  );

  router.get(
    '/groups',
    pageHandler(adapters, () => groupsPage(adapters)),
  );

  router.get(
    '/groups/:slug',
    async (context, next) => {
      context.status = StatusCodes.TEMPORARY_REDIRECT;
      context.redirect(`/groups/${context.params.slug}/feed`);

      await next();
    },
  );

  router.get(
    '/groups/:slug/lists',
    pageHandler(adapters, createPageFromParams(
      GLP.paramsCodec,
      GLP.constructAndRenderPage(adapters),
    )),
  );

  router.get(
    '/groups/:slug/about',
    pageHandler(adapters, createPageFromParams(
      GAP.paramsCodec,
      GAP.constructAndRenderPage(adapters),
    )),
  );

  router.get(
    '/groups/:slug/followers',
    pageHandler(adapters, createPageFromParams(
      GFP.paramsCodec,
      GFP.constructAndRenderPage(adapters),
    )),
  );

  router.get(
    '/groups/:slug/feed',
    pageHandler(adapters, createPageFromParams(
      GFEP.paramsCodec,
      GFEP.constructAndRenderPage(adapters),
    )),
  );

  router.get(
    '/lists',
    pageHandler(adapters, () => listsPage(adapters)),
  );

  router.get(
    '/lists/:id',
    pageHandler(adapters, createPageFromParams(
      listPageParams,
      listPage(adapters),
    ), fullWidthPageLayout),
  );

  router.get(
    '/lists/:id/feed.atom',
    listFeed(adapters),
  );

  router.get(
    '/lists/:id/edit-details',
    pageHandler(adapters, createPageFromParams(
      editListDetailsFormPageParamsCodec,
      editListDetailsFormPage(adapters),
    )),
  );

  router.get(
    '/save-article',
    requireLoggedInUser(adapters),
    pageHandler(adapters, saveArticleFormPage(adapters)),
  );

  router.get(
    '/annotations/create-annotation-form',
    requireLoggedInUser(adapters),
    pageHandler(adapters, createPageFromParams(
      createAnnotationFormPageParamsCodec,
      createAnnotationFormPage(adapters),
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

  router.get(
    '/style-guide',
    pageHandler(adapters, () => pipe(indexPage, TE.right)),
  );

  router.get(
    '/style-guide/reference',
    pageHandler(adapters, () => pipe(referencePage, TE.right)),
  );

  router.get(
    '/style-guide/shared-components',
    pageHandler(adapters, () => pipe(sharedComponentsPage, TE.right)),
  );

  router.get('/api/lists/owned-by/:ownerId', ownedBy(adapters));

  router.post('/api/add-article-to-list', createApiRouteForResourceAction(adapters, addArticleToListCommandCodec, listResource.addArticle));

  router.post('/api/add-group', createApiRouteForResourceAction(adapters, addGroupCommandCodec, groupResource.create));

  router.post(
    '/api/create-user',
    createApiRouteForResourceAction(adapters, createUserAccountCommandCodec, userResource.create),
  );

  router.post('/api/edit-list-details', createApiRouteForResourceAction(adapters, editListDetailsCommandCodec, listResource.update));

  router.post('/api/erase-evaluation', createApiRouteForResourceAction(adapters, eraseEvaluationCommandCodec, evaluationResource.erase));

  router.post('/api/record-evaluation-publication', createApiRouteForResourceAction(adapters, recordEvaluationPublicationCommandCodec, evaluationResource.recordPublication));

  router.post('/api/record-evaluation-removal', createApiRouteForResourceAction(adapters, recordEvaluationRemovalCommandCodec, evaluationResource.recordRemoval));

  router.post('/api/remove-article-from-list', createApiRouteForResourceAction(adapters, removeArticleFromListCommandCodec, listResource.removeArticle));

  router.post('/api/update-evaluation', createApiRouteForResourceAction(adapters, updateEvaluationCommandCodec, evaluationResource.update));

  router.post('/api/update-group-details', createApiRouteForResourceAction(adapters, updateGroupDetailsCommandCodec, groupResource.update));

  router.post('/api/update-user-details', createApiRouteForResourceAction(adapters, updateUserDetailsCommandCodec, userResource.update));

  formSubmissionHandlers.configureRoutes(router, adapters);

  authentication.configureRoutes(router, adapters, config);

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

  router.get('/docmaps/v1', async (context, next) => {
    const staticFolder = path.resolve(__dirname, '../../static');
    await send(context, 'docmaps-v1-api-docs.html', { root: staticFolder });

    await next();
  });

  // OBSERVABILITY

  router.get('/status', async (context, next) => {
    context.response.body = applicationStatus(adapters);
    context.response.status = StatusCodes.OK;
    await next();
  });

  router.get('/status/groups', async (context, next) => {
    context.response.body = statusGroups(adapters);
    context.response.status = StatusCodes.OK;
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
