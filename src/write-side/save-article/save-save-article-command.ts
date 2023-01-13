import { Middleware } from 'koa';

export const articleIdFieldName = 'articleid';

export const saveSaveArticleCommand: Middleware = async (context, next) => {
  context.session.command = 'save-article';
  context.session.articleId = context.request.body[articleIdFieldName];
  await next();
};
