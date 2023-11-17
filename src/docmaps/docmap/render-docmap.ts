import * as RA from 'fp-ts/ReadonlyArray';
import * as RNEA from 'fp-ts/ReadonlyNonEmptyArray';
import { pipe } from 'fp-ts/function';
import { Docmap } from './docmap-type.js';
import { Evaluation } from './evaluation.js';
import { DocmapViewModel } from './construct-docmap-view-model.js';
import { anonymous, peerReviewer } from './peer-reviewer.js';
import { publisherAccountId } from './publisher-account-id.js';
import { ArticleId } from '../../types/article-id.js';
import * as EL from '../../types/evaluation-locator.js';

const createAction = (articleId: ArticleId) => (evaluation: Evaluation) => ({
  participants: pipe(
    evaluation.authors,
    RA.match(
      () => [peerReviewer(anonymous)],
      RA.map(peerReviewer),
    ),
  ),
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
          url: `https://sciety.org/articles/activity/${articleId.value}#${EL.serialize(evaluation.evaluationLocator)}`,
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
  id: `https://sciety.org/docmaps/v1/articles/${viewModel.articleId.value}/${viewModel.group.slug}.docmap.json`,
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
      inputs: [{
        doi: viewModel.articleId.value,
        url: `https://doi.org/${viewModel.articleId.value}`,
      }],
      actions: pipe(
        viewModel.evaluations,
        RA.map(createAction(viewModel.articleId)),
      ),
    },
  },
});
