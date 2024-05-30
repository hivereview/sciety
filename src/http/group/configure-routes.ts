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
    groupSubPagePathSpecification('lists'),
    pageHandler(adapters, createPageFromParams(
      GLP.paramsCodec,
      GLP.constructAndRenderPage(adapters),
    )),
  );

  router.get(
    groupSubPagePathSpecification('about'),
    pageHandler(adapters, createPageFromParams(
      GAP.paramsCodec,
      GAP.constructAndRenderPage(adapters),
    )),
  );

  router.get(
    groupSubPagePathSpecification('followers'),
    pageHandler(adapters, createPageFromParams(
      GFP.paramsCodec,
      GFP.constructAndRenderPage(adapters),
    )),
  );

  router.get(
    groupSubPagePathSpecification('feed'),
    async (context, next) => {
      context.status = StatusCodes.TEMPORARY_REDIRECT;
      context.redirect(`/groups/${context.params.slug}`);

      await next();
    },
  );

  router.get(
    groupSubPagePathSpecification('management'),
    pageHandlerWithLoggedInUser(adapters, GMP.page(adapters)),
  );
};
