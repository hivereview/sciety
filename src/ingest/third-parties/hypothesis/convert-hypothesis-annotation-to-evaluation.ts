import * as E from 'fp-ts/Either';
import { pipe } from 'fp-ts/function';
import * as RA from 'fp-ts/ReadonlyArray';
import * as R from 'fp-ts/Record';
import { Evaluation } from '../../types/evaluations';
import { supportedArticleIdFromLink } from '../../supported-article-id-from-link';
import { Annotation } from '../../../third-parties/hypothesis';
import { SkippedItem } from '../../types/skipped-item';

const annotationContainsText = (annotation: Annotation) => annotation.text.length > 0;

const mapTagToType = (
  tags: ReadonlyArray<string>,
  tagToEvaluationTypeMap: Record<string, ReadonlyArray<string>>,
): string => pipe(
  tagToEvaluationTypeMap,
  R.filter((t) => pipe(
    tags,
    RA.some((tag) => t.includes(tag)),
  )),
  R.keys,
  (keys) => (keys.length === 1 ? keys[0] : 'not-provided'),
);

export const convertHypothesisAnnotationToEvaluation = (
  tagToEvaluationTypeMap: Record<string, ReadonlyArray<string>>,
) => (
  annotation: Annotation,
): E.Either<SkippedItem, Evaluation> => pipe(
  annotation.uri,
  supportedArticleIdFromLink,
  E.filterOrElse(
    () => annotationContainsText(annotation),
    () => 'annotation text field is empty',
  ),
  E.bimap(
    (reason) => ({ item: annotation.uri, reason }),
    (articleDoi) => ({
      date: new Date(annotation.created),
      articleDoi,
      evaluationLocator: `hypothesis:${annotation.id}`,
      authors: [],
      evaluationType: mapTagToType(
        annotation.tags,
        tagToEvaluationTypeMap,
      ),
    }),
  ),
);
