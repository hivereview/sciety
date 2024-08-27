import { URL } from 'url';
import { v4 } from 'uuid';
import { PendingEvaluation } from '../../read-models/evaluations-for-notifications';
import * as paths from '../../standards/paths';
import { CoarNotificationModel } from '../../types/coar-notification-model';
import { toDoiUrl } from '../../types/expression-doi';

export const constructCoarNotificationModel = (
  scietyUiOrigin: URL,
) => (
  pendingEvaluation: PendingEvaluation,
): CoarNotificationModel => ({
  id: `urn:uuid:${v4()}`,
  objectId: new URL(`${scietyUiOrigin.origin}${paths.constructPaperActivityPageFocusedOnEvaluationHref(pendingEvaluation.expressionDoi, pendingEvaluation.evaluationLocator)}`),
  contextId: new URL(`${scietyUiOrigin.origin}${paths.constructPaperActivityPageHref(pendingEvaluation.expressionDoi)}`),
  contextCiteAs: new URL(toDoiUrl(pendingEvaluation.expressionDoi)),
  targetId: pendingEvaluation.targetId,
  targetInbox: pendingEvaluation.targetInbox,
});
