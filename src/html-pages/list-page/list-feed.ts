import { Middleware } from '@koa/router';
import { HttpStatusCode } from 'axios';
import { pipe } from 'fp-ts/function';
import * as O from 'fp-ts/Option';
import { renderAsAtom } from './render-as-atom';
import { ListId } from '../../types/list-id';

export const listFeed: Middleware = async (context, next) => {
  context.response.status = HttpStatusCode.Ok;
  context.response.type = 'application/atom+xml';
  context.response.body = pipe(
    {
      name: '',
      description: '',
      ownerName: '',
      ownerHref: '',
      ownerAvatarPath: '',
      articleCount: 0,
      updatedAt: new Date(),
      editCapability: false,
      listId: '' as ListId,
      basePath: '',
      content: 'no-articles',
      relatedArticlesLink: O.none,
    },
    renderAsAtom,
  );

  await next();
};
