import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import { pipe } from 'fp-ts/function';
import { DomainEvent } from '../../domain-events';
import { isAnnotationCreatedEvent } from '../../domain-events/annotation-created-event';
import { eqAnnotationTarget } from '../../types/annotation-target';
import { Doi } from '../../types/doi';
import { HtmlFragment, toHtmlFragment } from '../../types/html-fragment';
import * as LID from '../../types/list-id';
import { UserId, userIdCodec } from '../../types/user-id';

type GetAnnotationContentByUserListTarget = (articleId: Doi, listOwnerId: UserId)
=> (events: ReadonlyArray<DomainEvent>)
=> O.Option<HtmlFragment>;

const listIdForAvasthiReadingUser = LID.fromValidatedString('1af5b971-162e-4cf3-abdf-57e3bbfcd0d7');

const queryTarget = (articleId: Doi) => ({ listId: listIdForAvasthiReadingUser, articleId });

export const getAnnotationContentByUserListTarget: GetAnnotationContentByUserListTarget = (
  articleId,
  listOwnerId,
) => (events) => {
  const isAuthorised = pipe(
    '1412019815619911685',
    userIdCodec.decode,
    E.filterOrElseW((uid) => uid === listOwnerId, () => 'not-authorised'),
    E.isRight,
  );
  if (!isAuthorised) {
    return O.none;
  }

  return pipe(
    events,
    RA.filter(isAnnotationCreatedEvent),
    RA.filter((event) => eqAnnotationTarget.equals(event.target, queryTarget(articleId))),
    RA.head,
    O.map((event) => toHtmlFragment(event.content)),
  );
};
