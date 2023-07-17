import { htmlEscape } from 'escape-goat';
import * as O from 'fp-ts/Option';
import { flow, pipe } from 'fp-ts/function';
import { HtmlFragment, toHtmlFragment } from '../../types/html-fragment';
import { templateDate } from '../date';
import { renderCountWithDescriptor } from '../render-count-with-descriptor';
import { GroupCardViewModel } from './view-model';

const wrapInSpan = (text: string) => toHtmlFragment(`<span>${text}</span>`);

const renderEvaluationCount = (evaluationCount: number): HtmlFragment => pipe(
  renderCountWithDescriptor(evaluationCount, 'evaluation', 'evaluations'),
  wrapInSpan,
);

const renderCuratedArticlesCount = (articleCount: number) => (
  (articleCount === 0)
    ? ''
    : pipe(
      renderCountWithDescriptor(articleCount, 'curated article', 'curated articles'),
      wrapInSpan,
    )
);

const renderListCount = (listCount: number) => pipe(
  renderCountWithDescriptor(listCount, 'list', 'lists'),
  wrapInSpan,
);

const renderFollowerCount = (followerCount: number): HtmlFragment => pipe(
  renderCountWithDescriptor(followerCount, 'follower', 'followers'),
  wrapInSpan,
);

const renderLatestActivity = (latestActivity: O.Option<Date>): HtmlFragment => pipe(
  latestActivity,
  O.fold(
    () => toHtmlFragment(''),
    flow(
      templateDate,
      (text) => `Latest activity ${text}`,
      wrapInSpan,
    ),
  ),
);

export const renderGroupCard = flow(
  (viewModel: GroupCardViewModel) => `
    <article class="group-card">
        <div class="group-card__body">
          <h3 class="group-card__title">
            <a class="group-card__link" href="/groups/${viewModel.slug}">${htmlEscape(viewModel.name)}</a>
          </h3>
          <div class="group-card__description">
            ${viewModel.description}
          </div>
          <span class="group-card__meta">
            <span class="visually-hidden">This group has </span>${renderEvaluationCount(viewModel.evaluationCount)}${renderCuratedArticlesCount(viewModel.curatedArticlesCount)}${renderListCount(viewModel.listCount)}${renderFollowerCount(viewModel.followerCount)}${renderLatestActivity(viewModel.latestActivityAt)}
          </span>
        </div>
        <img class="group-card__avatar" src="${viewModel.avatarPath}" alt="" />
    </article>
  `,
  toHtmlFragment,
);
