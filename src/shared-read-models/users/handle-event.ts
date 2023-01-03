/* eslint-disable no-param-reassign */
import { DomainEvent, isUserCreatedAccountEvent } from '../../domain-events';
import { UserId } from '../../types/user-id';

type UserDetails = {
  avatarUrl: string,
  displayName: string,
  handle: string,
  userId: UserId,
};

export type ReadModel = Record<UserId, UserDetails>;

// ts-unused-exports:disable-next-line
export const initialState = (): ReadModel => ({});

// ts-unused-exports:disable-next-line
export const handleEvent = (readModel: ReadModel, event: DomainEvent): ReadModel => {
  if (isUserCreatedAccountEvent(event)) {
    readModel[event.userId] = {
      avatarUrl: event.avatarUrl,
      displayName: event.displayName,
      handle: event.handle,
      userId: event.userId,
    };
  }
  return readModel;
};
