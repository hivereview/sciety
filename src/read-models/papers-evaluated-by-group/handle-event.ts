/* eslint-disable no-param-reassign */
/* eslint-disable no-loops/no-loops */
import { DomainEvent, EventOfType, isEventOfType } from '../../domain-events';
import { ExpressionDoi } from '../../types/expression-doi';
import { GroupId } from '../../types/group-id';

export type ReadModel = {
  expressionsEvaluatedByGroupId: Record<GroupId, Array<ExpressionDoi>>,
  evaluatedExpressionsWithoutPaperSnapshot: Record<GroupId, Set<ExpressionDoi>>,
  paperSnapshots: Record<ExpressionDoi, ReadonlyArray<ExpressionDoi>>,
};

export const initialState = (): ReadModel => ({
  expressionsEvaluatedByGroupId: {},
  evaluatedExpressionsWithoutPaperSnapshot: {},
  paperSnapshots: {},
});

const ensureGroupIdExists = (readmodel: ReadModel, groupId: GroupId) => {
  if (!(groupId in readmodel.evaluatedExpressionsWithoutPaperSnapshot)) {
    readmodel.evaluatedExpressionsWithoutPaperSnapshot[groupId] = new Set<ExpressionDoi>();
  }
  if (!(groupId in readmodel.expressionsEvaluatedByGroupId)) {
    readmodel.expressionsEvaluatedByGroupId[groupId as GroupId] = [];
  }
};

const hasIntersection = (
  paperExpressionDois: EventOfType<'PaperSnapshotRecorded'>['expressionDois'],
  knownEvaluatedPapers: ReadModel['expressionsEvaluatedByGroupId'][GroupId],
) => {
  const intersection = Array.from(paperExpressionDois).filter(
    (expression) => knownEvaluatedPapers.includes(expression),
  );
  return intersection.length > 0;
};

const handleEvaluationPublicationRecorded = (event: EventOfType<'EvaluationPublicationRecorded'>, readmodel: ReadModel) => {
  ensureGroupIdExists(readmodel, event.groupId);
  const isPartOfKnownSnapshot = Object.keys(readmodel.paperSnapshots).includes(event.articleId);
  if (!isPartOfKnownSnapshot) {
    readmodel.evaluatedExpressionsWithoutPaperSnapshot[event.groupId].add(event.articleId);
    return;
  }
  const latestSnapshotForEvaluatedExpression = new Set(readmodel.paperSnapshots[event.articleId]);
  const noExpressionOfThePaperIsInEvaluatedPapersForThatGroup = !hasIntersection(
    latestSnapshotForEvaluatedExpression,
    readmodel.expressionsEvaluatedByGroupId[event.groupId],
  );
  if (noExpressionOfThePaperIsInEvaluatedPapersForThatGroup) {
    readmodel.expressionsEvaluatedByGroupId[event.groupId].push(event.articleId);
  }
};

const updateKnownEvaluatedPapers = (
  knownEvaluatedPapers: ReadModel['expressionsEvaluatedByGroupId'][GroupId],
  paperExpressionDois: EventOfType<'PaperSnapshotRecorded'>['expressionDois'],
  queueOfExpressionsWithoutPaperSnapshot: ReadModel['evaluatedExpressionsWithoutPaperSnapshot'][GroupId],
) => {
  paperExpressionDois.forEach((paperExpressionDoi) => {
    const paperExpressionWasInQueue = queueOfExpressionsWithoutPaperSnapshot.delete(paperExpressionDoi);
    const noExpressionOfThePaperIsInEvaluatedPapers = !hasIntersection(paperExpressionDois, knownEvaluatedPapers);
    if (paperExpressionWasInQueue && noExpressionOfThePaperIsInEvaluatedPapers) {
      knownEvaluatedPapers.push(paperExpressionDoi);
    }
  });
};

const handlePaperSnapshotRecorded = (event: EventOfType<'PaperSnapshotRecorded'>, readmodel: ReadModel) => {
  event.expressionDois.forEach((expression) => {
    readmodel.paperSnapshots[expression] = Array.from(event.expressionDois);
  });
  for (
    const [groupId, expressionsWithoutPaperSnapshot]
    of Object.entries(readmodel.evaluatedExpressionsWithoutPaperSnapshot)
  ) {
    ensureGroupIdExists(readmodel, groupId as GroupId);
    const knownEvaluatedPapers = readmodel.expressionsEvaluatedByGroupId[groupId as GroupId];
    updateKnownEvaluatedPapers(knownEvaluatedPapers, event.expressionDois, expressionsWithoutPaperSnapshot);
  }
};

export const handleEvent = (
  consideredGroupIds: ReadonlyArray<GroupId>,
) => (readmodel: ReadModel, event: DomainEvent): ReadModel => {
  if (isEventOfType('EvaluationPublicationRecorded')(event)) {
    if (consideredGroupIds.includes(event.groupId)) {
      handleEvaluationPublicationRecorded(event, readmodel);
    }
  }
  if (isEventOfType('PaperSnapshotRecorded')(event)) {
    handlePaperSnapshotRecorded(event, readmodel);
  }
  return readmodel;
};
