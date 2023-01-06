import { userCreatedAccount } from '../../src/domain-events';
import { updateSetOfUsersWithoutCreatedAccountEvents } from '../../src/policies/ensure-all-users-have-accounts';
import { arbitraryString, arbitraryUri, arbitraryWord } from '../helpers';
import { arbitraryUserId } from '../types/user-id.helper';

describe('updateSetOfUsersWithoutCreatedAccountEvents', () => {
  const userId = arbitraryUserId();

  describe('when the userId has not been seen before', () => {
    const currentState = {};

    describe('when the next event is UserCreatedAccount', () => {
      const readmodel = updateSetOfUsersWithoutCreatedAccountEvents(
        currentState,
        userCreatedAccount(userId, arbitraryWord(), arbitraryUri(), arbitraryString()),
      );

      it.failing('the userId is marked as having an account', () => {
        expect(readmodel[userId]).toBe(true);
      });
    });

    describe('when the next event is not UserCreatedAccount', () => {
      it.todo('the userId is marked as not having an account');
    });
  });

  describe('when the userId has been seen before', () => {
    describe('when the next event is UserCreatedAccount', () => {
      it.todo('the userId is marked as having an account');
    });

    describe('when the next event is not UserCreatedAccount', () => {
      describe('and the user is already marked as having an account', () => {
        it.todo('the userId is still marked as having an account');
      });

      describe('and the user is already marked as not having an account', () => {
        it.todo('the userId is still marked as not having an account');
      });
    });
  });
});
