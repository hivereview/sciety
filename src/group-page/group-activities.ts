import * as I from 'fp-ts/Identity';
import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import * as RM from 'fp-ts/ReadonlyMap';
import { constant, flow, pipe } from 'fp-ts/function';
import { Doi, eqDoi } from '../types/doi';
import {
  DomainEvent,
  isEditorialCommunityReviewedArticleEvent,
} from '../types/domain-events';
import { eqGroupId, GroupId } from '../types/group-id';

type ArticleActivity = { doi: Doi, latestActivityDate: Date, evaluationCount: number };

type GroupActivities = (events: ReadonlyArray<DomainEvent>) => (groupId: GroupId) => ReadonlyArray<ArticleActivity>;

type AllGroupActivities = (events: ReadonlyArray<DomainEvent>) => ReadonlyMap<Doi, {
  latestActivityDate: Date,
  evaluationCount: number,
}>;

const allGroupActivities: AllGroupActivities = flow(
  RA.filter(isEditorialCommunityReviewedArticleEvent),
  RA.reduce(
    RM.empty,
    (activities: ReadonlyMap<Doi, { latestActivityDate: Date, evaluationCount: number }>, event) => pipe(
      activities,
      RM.lookup(eqDoi)(event.articleId),
      O.fold(
        () => ({ latestActivityDate: event.date, evaluationCount: 1 }),
        (oldActivity) => ({
          latestActivityDate: event.date,
          evaluationCount: oldActivity.evaluationCount + 1,
        }),
      ),
      (newActivity) => RM.upsertAt(eqDoi)(event.articleId, newActivity)(activities),
    ),
  ),
);

type Activity = { groupId: GroupId, articleId: Doi };

const doisEvaluatedByGroup = (events: ReadonlyArray<DomainEvent>, groupId: GroupId) => pipe(
  events,
  RA.reduce(RA.empty, (state: ReadonlyArray<Activity>, event) => pipe(
    event,
    O.fromPredicate(isEditorialCommunityReviewedArticleEvent),
    O.fold(
      constant(state),
      ({ editorialCommunityId, articleId }) => RA.fromArray([...state, { groupId: editorialCommunityId, articleId }]),
    ),
  )),
  RA.filter((activity) => eqGroupId.equals(activity.groupId, groupId)),
  RA.map((activity) => activity.articleId),
  RA.reverse,
  RA.uniq(eqDoi),
);

type ActivityDetails = { evaluationCount: number, latestActivityDate: Date };
const addActivitiesDetailsToDois = (dois: ReadonlyArray<Doi>, activities: ReadonlyMap<Doi, ActivityDetails>) => pipe(
  dois,
  RA.map((doi) => pipe(
    activities,
    RM.lookup(eqDoi)(doi),
    O.map((activityDetails) => ({ ...activityDetails, doi })),
  )),
);

export const groupActivities: GroupActivities = (events) => (groupId) => pipe(
  I.Do,
  I.apS('dois', doisEvaluatedByGroup(events, groupId)),
  I.apS('activities', pipe(events, allGroupActivities)),
  ({ activities, dois }) => addActivitiesDetailsToDois(dois, activities),
  O.sequenceArray,
  O.map(RA.takeLeft(10)),
  O.getOrElseW(() => []),
);
