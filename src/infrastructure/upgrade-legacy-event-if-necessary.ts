import {
  DomainEvent, CurrentOrLegacyDomainEvent, EventOfType,
  LegacyEventOfType,
} from '../domain-events';

export const upgradeLegacyEventIfNecessary = (event: CurrentOrLegacyDomainEvent): DomainEvent => {
  const upgradeFunctions = {
    EvaluationRecorded: (legacyEvent: LegacyEventOfType<'EvaluationRecorded'>) => ({
      ...legacyEvent,
      type: 'EvaluationPublicationRecorded' as const,
    } satisfies EventOfType<'EvaluationPublicationRecorded'>),
    CurationStatementRecorded: (legacyEvent: LegacyEventOfType<'CurationStatementRecorded'>) => ({
      ...legacyEvent,
      authors: undefined,
      evaluationType: 'curation-statement',
      type: 'EvaluationUpdated' as const,
    } satisfies EventOfType<'EvaluationUpdated'>),
    AnnotationCreated: (legacyEvent: LegacyEventOfType<'AnnotationCreated'>) => ({
      id: legacyEvent.id,
      type: 'ArticleInListAnnotated',
      date: legacyEvent.date,
      content: legacyEvent.content,
      articleId: legacyEvent.target.articleId,
      listId: legacyEvent.target.listId,
    } satisfies EventOfType<'ArticleInListAnnotated'>),
  };
  if (event.type === 'EvaluationRecorded') {
    return upgradeFunctions[event.type](event);
  }
  if (event.type === 'CurationStatementRecorded') {
    return upgradeFunctions[event.type](event);
  }
  if (event.type === 'AnnotationCreated') {
    return upgradeFunctions[event.type](event);
  }
  return event;
};
