import * as E from 'fp-ts/Either';
import { pipe } from 'fp-ts/function';
import { inMemoryGroupRepository } from '../../src/infrastructure/in-memory-groups';
import * as DE from '../../src/types/data-error';
import { Group } from '../../src/types/group';
import { GroupRepository } from '../../src/types/group-repository';
import { arbitraryUri, arbitraryWord } from '../helpers';
import { arbitraryGroupId, groupIdFromString } from '../types/group-id.helper';

const id = '530812a5-838a-4fb2-95b6-eb4828f0d37c';
const groupId = groupIdFromString(id);
const groupSlug = arbitraryWord();

describe('in-memory-editorial-communities', () => {
  let repository: GroupRepository;
  const group = {
    id: groupId,
    name: 'My pals',
    avatarPath: '',
    shortDescription: '',
    descriptionPath: '/static/desc.md',
    homepage: arbitraryUri(),
    slug: groupSlug,
  };

  beforeEach(async () => {
    repository = inMemoryGroupRepository([group]);
  });

  describe('lookup', () => {
    let result: E.Either<DE.DataError, Group>;

    describe('when the group exists', () => {
      beforeEach(async () => {
        result = await pipe(
          id,
          groupIdFromString,
          repository.lookup,
        )();
      });

      it('returns the group', () => {
        expect(result).toStrictEqual(E.right(group));
      });
    });

    describe('when the group does not exist', () => {
      beforeEach(async () => {
        result = await pipe(
          arbitraryGroupId(),
          repository.lookup,
        )();
      });

      it('returns nothing', () => {
        expect(result).toStrictEqual(E.left(DE.notFound));
      });
    });
  });

  describe('lookup by slug', () => {
    let result: E.Either<DE.DataError, Group>;

    describe('when the group exists', () => {
      beforeEach(async () => {
        result = await pipe(
          groupSlug,
          repository.lookupBySlug,
        )();
      });

      it('returns the group', () => {
        expect(result).toStrictEqual(E.right(group));
      });
    });

    describe('when the group does not exist', () => {
      beforeEach(async () => {
        result = await pipe(
          arbitraryWord(),
          repository.lookupBySlug,
        )();
      });

      it('returns nothing', () => {
        expect(result).toStrictEqual(E.left(DE.notFound));
      });
    });
  });
});
