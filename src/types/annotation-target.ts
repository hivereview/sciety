import * as Eq from 'fp-ts/Eq';
import { pipe } from 'fp-ts/function';
import * as t from 'io-ts';
import { DoiFromString } from './codecs/DoiFromString';
import { ListIdFromString } from './codecs/ListIdFromString';
import { eqDoi } from './doi';
import { eqListId } from './list-id';

export const annotationTargetCodec = t.type({
  articleId: DoiFromString,
  listId: ListIdFromString,
});

export type AnnotationTarget = t.TypeOf<typeof annotationTargetCodec>;

export const eqAnnotationTarget: Eq.Eq<AnnotationTarget> = pipe(
  {
    articleId: eqDoi,
    listId: eqListId,
  },
  Eq.struct,
);
