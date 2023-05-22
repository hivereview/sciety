/* eslint-disable quote-props */
/* eslint-disable no-param-reassign */
import { elifeGroupId, elifeSubjectAreaLists } from './data';
import {
  DomainEvent,
  isEvaluationRecordedEvent, isEventOfType,
  isSubjectAreaRecordedEvent,
} from '../../domain-events';
import { SubjectArea } from '../../types/subject-area';

type ArticleStateWithSubjectArea =
 | { name: 'subject-area-known', subjectArea: SubjectArea }
 | { name: 'evaluated-and-subject-area-known', subjectArea: SubjectArea };

// ts-unused-exports:disable-next-line
export type ArticleState =
 | ArticleStateWithSubjectArea
 | { name: 'evaluated' }
 | { name: 'listed' };

export type ArticleStateName = ArticleState['name'];

export type ReadModel = Record<string, ArticleState>;

export const initialState = (): ReadModel => ({});

export const isStateWithSubjectArea = (state: ArticleState):
  state is ArticleStateWithSubjectArea => {
  if (state === undefined) {
    return false;
  }
  return state.name === 'subject-area-known' || state.name === 'evaluated-and-subject-area-known';
};

export const handleEvent = (readmodel: ReadModel, event: DomainEvent): ReadModel => {
  if (isEvaluationRecordedEvent(event)) {
    if (event.groupId === elifeGroupId) {
      const key = event.articleId.value;
      const transitions = {
        'initial': 'evaluated' as const,
        'evaluated': 'evaluated' as const,
        'listed': 'listed' as const,
      };
      const currentState = readmodel[key];
      if (isStateWithSubjectArea(currentState)) {
        readmodel[key] = { name: 'evaluated-and-subject-area-known', subjectArea: currentState.subjectArea };
      } else {
        readmodel[key] = { name: transitions[currentState ? currentState.name : 'initial'] };
      }
    }
  } else if (isEventOfType('ArticleAddedToList')(event)) {
    if (elifeSubjectAreaLists.includes(event.listId)) {
      readmodel[event.articleId.value] = { name: 'listed' as const };
    }
  } else if (isSubjectAreaRecordedEvent(event)) {
    const key = event.articleId.value;
    const transitions = {
      'initial': 'subject-area-known' as const,
      'subject-area-known': 'subject-area-known' as const,
      'evaluated': 'evaluated-and-subject-area-known' as const,
      'evaluated-and-subject-area-known': 'evaluated-and-subject-area-known' as const,
      'listed': 'listed' as const,
    };
    const currentStateName = readmodel[key] ? readmodel[key].name : 'initial';
    readmodel[key] = { name: transitions[currentStateName], subjectArea: event.subjectArea };
  }
  return readmodel;
};
