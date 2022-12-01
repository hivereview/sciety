import * as RA from 'fp-ts/ReadonlyArray';
import * as B from 'fp-ts/boolean';
import { pipe } from 'fp-ts/function';
import { RecordEvaluationCommand } from '../commands';
import {
  DomainEvent, evaluationRecorded, isEvaluationRecordedEvent,
} from '../domain-events';
import { ReviewId } from '../types/review-id';

const hasEvaluationAlreadyBeenRecorded = (evaluationLocator: ReviewId) => (events: ReadonlyArray<DomainEvent>) => pipe(
  events,
  RA.filter(isEvaluationRecordedEvent),
  RA.some((event) => event.evaluationLocator === evaluationLocator),
);

const createEvaluationRecordedEvent = (command: RecordEvaluationCommand) => evaluationRecorded(
  command.groupId,
  command.articleId,
  command.evaluationLocator,
  command.authors,
  command.publishedAt,
  new Date(),
);

type ExecuteCommand = (command: RecordEvaluationCommand)
=> (events: ReadonlyArray<DomainEvent>)
=> ReadonlyArray<DomainEvent>;

export const executeCommand: ExecuteCommand = (command) => (events) => pipe(
  events,
  hasEvaluationAlreadyBeenRecorded(command.evaluationLocator),
  B.fold(
    () => [createEvaluationRecordedEvent(command)],
    () => [],
  ),
);
