/* eslint-disable @typescript-eslint/no-unused-vars */
import * as O from 'fp-ts/Option';
import { pipe } from 'fp-ts/function';
import * as R from 'fp-ts/Record';
import { RawUserInput, ReadModel } from './handle-event.js';
import { ListId } from '../../types/list-id.js';
import { ArticleId } from '../../types/article-id.js';

type GetAnnotationContent = (listId: ListId, articleId: ArticleId) => O.Option<RawUserInput>;

export const getAnnotationContent = (readModel: ReadModel): GetAnnotationContent => (listId, articleId) => pipe(
  readModel,
  R.lookup(listId),
  O.chain(R.lookup(articleId.value)),
);
