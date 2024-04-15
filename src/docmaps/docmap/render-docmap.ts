import * as RA from 'fp-ts/ReadonlyArray';
import * as RNEA from 'fp-ts/ReadonlyNonEmptyArray';
import { pipe } from 'fp-ts/function';
import { Docmap } from './docmap-type';
import { Evaluation } from './evaluation';
import { anonymous, peerReviewer } from './peer-reviewer';
import { publisherAccountId } from './publisher-account-id';
import * as EL from '../../types/evaluation-locator';
import * as EDOI from '../../types/expression-doi';
import { DocmapViewModel } from './view-model';
import { constructPaperActivityPageHref } from '../../read-side/paths';

const renderInputs = (expressionDoi: EDOI.ExpressionDoi) => [{
  doi: expressionDoi,
  url: `https://doi.org/${expressionDoi}`,
}];

const createAction = (expressionDoi: EDOI.ExpressionDoi) => (evaluation: Evaluation) => ({
  participants: pipe(
    evaluation.authors,
    RA.match(
      () => [peerReviewer(anonymous)],
      RA.map(peerReviewer),
    ),
  ),
  inputs: renderInputs(expressionDoi),
  outputs: [
    {
      type: 'review-article' as const,
      published: evaluation.publishedAt.toISOString(),
      content: [
        {
          type: 'web-page',
          url: evaluation.sourceUrl.toString(),
        },
        {
          type: 'web-page',
          url: `https://sciety.org${constructPaperActivityPageHref(expressionDoi)}#${EL.serialize(evaluation.evaluationLocator)}`,
        },
        {
          type: 'web-content',
          url: `https://sciety.org/evaluations/${EL.serialize(evaluation.evaluationLocator)}/content`,
        },
      ],
    },
  ],
});

export const renderDocmap = (viewModel: DocmapViewModel): Docmap => ({
  '@context': 'https://w3id.org/docmaps/context.jsonld',
  id: `https://sciety.org/docmaps/v1/articles/${viewModel.expressionDoi}/${viewModel.group.slug}.docmap.json`,
  type: 'docmap',
  created: RNEA.head(viewModel.evaluations).recordedAt.toISOString(),
  updated: viewModel.updatedAt.toISOString(),
  publisher: {
    id: viewModel.group.homepage,
    name: viewModel.group.name,
    logo: `https://sciety.org${viewModel.group.avatarPath}`,
    homepage: viewModel.group.homepage,
    account: {
      id: publisherAccountId(viewModel.group),
      service: 'https://sciety.org',
    },
  },
  'first-step': '_:b0',
  steps: {
    '_:b0': {
      assertions: [],
      inputs: renderInputs(viewModel.expressionDoi),
      actions: pipe(
        viewModel.evaluations,
        RA.map(createAction(viewModel.expressionDoi)),
      ),
    },
  },
});
