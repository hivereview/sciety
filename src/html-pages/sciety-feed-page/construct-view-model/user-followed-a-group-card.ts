import { sequenceS } from 'fp-ts/Apply';
import * as O from 'fp-ts/Option';
import { pipe } from 'fp-ts/function';
import { EventOfType } from '../../../domain-events';
import { toHtmlFragment } from '../../../types/html-fragment';
import { ScietyFeedCard } from '../view-model';
import { Dependencies } from './dependencies';

type UserFollowedAGroupCard = (
  dependencies: Dependencies
) => (event: EventOfType<'UserFollowedEditorialCommunity'>) => O.Option<ScietyFeedCard>;

export const userFollowedAGroupCard: UserFollowedAGroupCard = (dependencies) => (event) => pipe(
  {
    group: pipe(
      dependencies.getGroup(event.editorialCommunityId),
    ),
    userDetails: pipe(
      event.userId,
      dependencies.lookupUser,
      O.getOrElseW(
        () => ({
          handle: 'A user',
          avatarUrl: '/static/images/sciety-logo.jpg',
        }),
      ),
      O.some,
    ),
  },
  sequenceS(O.Apply),
  O.map(({ group, userDetails }) => ({
    linkUrl: `/groups/${group.slug}/about`,
    avatarUrl: userDetails.avatarUrl,
    titleText: `${userDetails.handle} followed a group`,
    date: event.date,
    details: {
      title: toHtmlFragment(group.name),
      content: toHtmlFragment(`<p>${group.shortDescription}</p>`),
    },
  })),
);
