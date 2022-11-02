import * as O from 'fp-ts/Option';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { elifeGroupId, getCorrespondingListId } from '../add-article-to-elife-subject-area-list/read-model';
import { DomainEvent, isEvaluationRecordedEvent } from '../domain-events';
import {
  AddArticleToList, GetArticleSubjectArea, Logger,
} from '../shared-ports';

export type Ports = {
  logger: Logger,
  getArticleSubjectArea: GetArticleSubjectArea,
  addArticleToList: AddArticleToList,
};

type AddArticleToElifeSubjectAreaLists = (ports: Ports) => (event: DomainEvent) => T.Task<void>;

export const addArticleToElifeSubjectAreaLists: AddArticleToElifeSubjectAreaLists = (ports) => (event) => {
  if (!isEvaluationRecordedEvent(event)) {
    return T.of(undefined);
  }
  if (event.groupId !== elifeGroupId) {
    return T.of(undefined);
  }

  return pipe(
    event.articleId,
    ports.getArticleSubjectArea,
    TE.bimap(
      () => 'Subject Area available from neither bioRxiv nor medRxiv',
      ({ value }) => value,
    ),
    TE.chain((subjectArea) => pipe(
      subjectArea,
      getCorrespondingListId,
      O.foldW(
        () => {
          ports.logger('info', 'addArticleToElifeSubjectAreaLists policy: unsupported subject area', { event, subjectArea });
          return TE.right(undefined);
        },
        (listId) => ports.addArticleToList({ articleId: event.articleId, listId }),
      ),
    )),
    TE.match(
      (errorMessage) => { ports.logger('error', 'addArticleToElifeSubjectAreaLists policy failed', { articleId: event.articleId, errorMessage }); },
      () => {},
    ),
  );
};
