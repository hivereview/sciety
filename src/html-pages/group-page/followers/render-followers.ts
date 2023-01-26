import * as RA from 'fp-ts/ReadonlyArray';
import { flow, pipe } from 'fp-ts/function';
import { HtmlFragment, toHtmlFragment } from '../../../types/html-fragment';
import { UserHandle } from '../../../types/user-handle';

export type UserCardViewModel = {
  link: string,
  title: string,
  handle: UserHandle,
  listCount: number,
  followedGroupCount: number,
  avatarUrl: string,
};

type FollowerListViewModel = {
  followerCount: number,
  followers: ReadonlyArray<UserCardViewModel>,
  nextLink: HtmlFragment,
};

type RenderFollowers = (followerListViewModel: FollowerListViewModel) => HtmlFragment;

const renderUserCard = (userCard: UserCardViewModel): HtmlFragment => toHtmlFragment(`
  <article class="user-card">
    <div class="user-card__body">
      <h3 class="user-card__title"><a href="${userCard.link}" class="user-card__link">${userCard.title}</a></h3>
      <div class="user-card__handle">@${userCard.handle}</div>
      <span class="user-card__meta"><span class="visually-hidden">This user has </span><span>${userCard.listCount} list</span><span>${userCard.followedGroupCount} groups followed</span></span>
    </div>
    <img class="user-card__avatar" src="${userCard.avatarUrl}" alt="">
  </article>
`);

const renderFollowersList = (userCards: ReadonlyArray<UserCardViewModel>) => pipe(
  userCards,
  RA.map(flow(
    renderUserCard,
    (userCard) => `<li class="group-page-followers-list__item">${userCard}</li>`,
  )),
  (items) => (items.length === 0 ? '' : `
    <ul class="group-page-followers-list">
      ${items.join('')}
    </ul>
  `),
);

export const renderFollowers: RenderFollowers = ({ followerCount, followers, nextLink }) => toHtmlFragment(`
  <p>
    ${followerCount} ${followerCount === 1 ? 'user is' : 'users are'} following this group.
  </p>
  ${renderFollowersList(followers)}
  ${nextLink}
`);
