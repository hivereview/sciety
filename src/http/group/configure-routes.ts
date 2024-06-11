import Router from '@koa/router';
import { StatusCodes } from 'http-status-codes';
import { CollectedPorts } from '../../infrastructure';
import { createPageFromParams } from '../../read-side/html-pages/create-page-from-params';
import * as GAP from '../../read-side/html-pages/group-page/group-about-page';
import * as GFP from '../../read-side/html-pages/group-page/group-followers-page';
import * as GHP from '../../read-side/html-pages/group-page/group-home-page';
import * as GLP from '../../read-side/html-pages/group-page/group-lists-page';
import * as GMP from '../../read-side/html-pages/group-page/group-management-page';
import { groupPagePathSpecification, groupSubPagePathSpecification } from '../../read-side/paths';
import { constructGroupPagePath } from '../../read-side/paths/construct-group-page-href';
import { pageHandler, pageHandlerWithLoggedInUser } from '../page-handler';

export const configureRoutes = (router: Router, adapters: CollectedPorts): void => {
  router.get(
    groupPagePathSpecification,
    pageHandler(adapters, createPageFromParams(
      GHP.paramsCodec,
      GHP.constructAndRenderPage(adapters),
    )),
  );

  router.get(
    constructGroupPagePath.lists.spec,
    pageHandler(adapters, createPageFromParams(
      GLP.paramsCodec,
      GLP.constructAndRenderPage(adapters),
    )),
  );

  router.get(
    constructGroupPagePath.about.spec,
    pageHandler(adapters, createPageFromParams(
      GAP.paramsCodec,
      GAP.constructAndRenderPage(adapters),
    )),
  );

  router.get(
    constructGroupPagePath.followers.spec,
    pageHandler(adapters, createPageFromParams(
      GFP.paramsCodec,
      GFP.constructAndRenderPage(adapters),
    )),
  );

  router.get(
    groupSubPagePathSpecification('/feed'),
    async (context, next) => {
      context.status = StatusCodes.TEMPORARY_REDIRECT;
      context.redirect(constructGroupPagePath.home.href({ slug: context.params.slug }));

      await next();
    },
  );

  router.get(
    constructGroupPagePath.management.spec,
    pageHandlerWithLoggedInUser(adapters, GMP.page(adapters)),
  );
};
