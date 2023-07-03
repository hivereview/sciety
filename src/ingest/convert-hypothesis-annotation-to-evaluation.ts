import * as E from 'fp-ts/Either';
import { pipe } from 'fp-ts/function';
import * as RA from 'fp-ts/ReadonlyArray';
import { Evaluation } from './evaluations';
import { supportedArticleIdFromLink } from './supported-article-id-from-link';
import { SkippedItem } from './update-all';
import * as Hyp from '../third-parties/hypothesis';

const annotationContainsText = (annotation: Hyp.Annotation) => annotation.text.length > 0;

const provideEvaluationTypeValueFromSupportedAnnotationTag = (tags: ReadonlyArray<string>) => pipe(
  tags,
  RA.some((tag) => tag === 'Summary ' || tag === 'Summary'),
  (isCurationStatement) => (isCurationStatement ? 'curation-statement' : undefined),
);

export const convertHypothesisAnnotationToEvaluation = (
  annotation: Hyp.Annotation,
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
      evaluationType: provideEvaluationTypeValueFromSupportedAnnotationTag(annotation.tags),
    }),
  ),
);
