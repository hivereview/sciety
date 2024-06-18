import Router from '@koa/router';
import * as RA from 'fp-ts/ReadonlyArray';
import { pipe } from 'fp-ts/function';
import { StatusCodes } from 'http-status-codes';
import { createConfigurePostMiddleware } from './create-configure-post-middleware';
import { DependenciesForViews } from '../../read-side/dependencies-for-views';
import { groups } from '../../read-side/non-html-views/api/groups';
import { applicationStatus } from '../../read-side/non-html-views/api/status';
import { DependenciesForCommands } from '../../write-side';
import {
  addArticleToListCommandCodec,
  addGroupCommandCodec,
  assignUserAsGroupAdminCommandCodec,
  editListDetailsCommandCodec,
  eraseEvaluationCommandCodec,
  promoteListCommandCodec,
  recordEvaluationPublicationCommandCodec,
  recordEvaluationRemovalCommandCodec,
  removeArticleFromListCommandCodec,
  removeListPromotionCommandCodec,
  updateEvaluationCommandCodec,
  updateGroupDetailsCommandCodec,
  updateUserDetailsCommandCodec,
} from '../../write-side/commands';
import { createUserAccountCommandCodec } from '../../write-side/commands/create-user-account';
import * as evaluationResource from '../../write-side/resources/evaluation';
import * as groupResource from '../../write-side/resources/group';
import * as groupAuthorisation from '../../write-side/resources/group-authorisation';
import * as listResource from '../../write-side/resources/list';
import * as listPromotionResource from '../../write-side/resources/list-promotion';
import * as userResource from '../../write-side/resources/user';
import { ownedBy } from '../owned-by-api';

export const configureRoutes = (
  router: Router,
  dependencies: DependenciesForCommands & DependenciesForViews,
  expectedToken: string,
): void => {
  router.get('/api/lists/owned-by/:ownerId', ownedBy(dependencies));

  router.get('/status', async (context, next) => {
    context.response.body = applicationStatus(dependencies);
    context.response.status = StatusCodes.OK;
    await next();
  });

  router.get('/status/groups', async (context, next) => {
    context.response.body = groups(dependencies);
    context.response.status = StatusCodes.OK;
    await next();
  });

  const configurePostMiddleware = createConfigurePostMiddleware(dependencies, expectedToken);

  const config = [{
    endpoint: 'add-article-to-list',
    handler: configurePostMiddleware(addArticleToListCommandCodec, listResource.addArticle),
  },
  {
    endpoint: 'add-group',
    handler: configurePostMiddleware(addGroupCommandCodec, groupResource.create),
  },
  {
    endpoint: 'assign-group-admin',
    handler: configurePostMiddleware(assignUserAsGroupAdminCommandCodec, groupAuthorisation.assign),
  },
  {
    endpoint: 'create-user',
    handler: configurePostMiddleware(createUserAccountCommandCodec, userResource.create),
  },
  {
    endpoint: 'edit-list-details',
    handler: configurePostMiddleware(editListDetailsCommandCodec, listResource.update),
  },
  {
    endpoint: 'erase-evaluation',
    handler: configurePostMiddleware(eraseEvaluationCommandCodec, evaluationResource.erase),
  },
  {
    endpoint: 'promote-list',
    handler: configurePostMiddleware(promoteListCommandCodec, listPromotionResource.create),
  },
  {
    endpoint: 'remove-list-promotion',
    handler: configurePostMiddleware(removeListPromotionCommandCodec, listPromotionResource.remove),
  },
  {
    endpoint: 'record-evaluation-publication',
    handler: configurePostMiddleware(recordEvaluationPublicationCommandCodec, evaluationResource.recordPublication),
  },
  {
    endpoint: 'record-evaluation-removal',
    handler: configurePostMiddleware(recordEvaluationRemovalCommandCodec, evaluationResource.recordRemoval),
  },
  {
    endpoint: 'remove-article-from-list',
    handler: configurePostMiddleware(removeArticleFromListCommandCodec, listResource.removeArticle),
  },
  {
    endpoint: 'update-evaluation',
    handler: configurePostMiddleware(updateEvaluationCommandCodec, evaluationResource.update),
  },
  {
    endpoint: 'update-group-details',
    handler: configurePostMiddleware(updateGroupDetailsCommandCodec, groupResource.update),
  },
  {
    endpoint: 'update-user-details',
    handler: configurePostMiddleware(updateUserDetailsCommandCodec, userResource.update),
  },
  ];
  pipe(
    config,
    RA.map((route) => ({
      ...route,
      endpoint: `/api/${route.endpoint}`,
    })),
    RA.map((route) => router.post(route.endpoint, route.handler)),
  );
};
