import { htmlEscape } from 'escape-goat';
import * as O from 'fp-ts/Option';
import { flow, pipe } from 'fp-ts/function';
import { GroupId } from '../../types/group-id';
import { HtmlFragment, toHtmlFragment } from '../../types/html-fragment';
import { SanitisedHtmlFragment } from '../../types/sanitised-html-fragment';
import { templateDate } from '../date';

export type GroupViewModel = {
  id: GroupId,
  name: string,
  description: SanitisedHtmlFragment,
  avatarPath: string,
  slug: string,
  listCount: number,
  followerCount: number,
  reviewCount: number,
  latestActivity: O.Option<Date>,
};

const wrapInSpan = (text: string) => toHtmlFragment(`<span>${text}</span>`);

const renderFollowerCount = (followerCount: number): HtmlFragment => pipe(
  followerCount === 1,
  (singular) => `${followerCount} ${singular ? 'follower' : 'followers'}`,
  wrapInSpan,
);

const renderEvaluationCount = (evaluationCount: number): HtmlFragment => pipe(
  evaluationCount === 1,
  (singular) => `${evaluationCount} ${singular ? 'evaluation' : 'evaluations'}`,
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

const renderListCount = flow(
  (listCount: number) => (listCount === 1 ? `${listCount} list` : `${listCount} lists`),
  wrapInSpan,
);

export const renderGroupCard = flow(
  (viewModel: GroupViewModel) => `
    <article>
      <a class="group-card" href="/groups/${viewModel.slug}">
        <div class="group-card__body">
          <h3 class="group-card__title">
            ${htmlEscape(viewModel.name)}
          </h3>
          <div class="group-card__description">
            ${viewModel.description}
          </div>
          <span class="group-card__meta">
            <span class="visually-hidden">This group has </span>${renderEvaluationCount(viewModel.reviewCount)}${renderListCount(viewModel.listCount)}${renderFollowerCount(viewModel.followerCount)}${renderLatestActivity(viewModel.latestActivity)}
          </span>
        </div>
        <img class="group-card__avatar" src="${viewModel.avatarPath}" alt="" />
      </a>
    </article>
  `,
  toHtmlFragment,
);
