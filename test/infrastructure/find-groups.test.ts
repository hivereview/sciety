import * as TE from 'fp-ts/TaskEither';
import { findGroups } from '../../src/infrastructure/find-groups';
import { GroupId } from '../../src/types/group-id';

type Group = {
  id: GroupId,
  avatarPath?: string,
  descriptionPath?: string,
  name?: string,
  shortDescription?: string,
};

const constructGroup = ({
  id,
  avatarPath = '',
  descriptionPath = '',
  name = '',
  shortDescription = '',
}: Group) => ({
  id,
  avatarPath,
  descriptionPath,
  name,
  shortDescription,
});

describe('find-groups', () => {
  describe('provided a valid group name', () => {
    it('returns an array containing the groupId', async () => {
      const result = await findGroups(() => TE.right(''), [constructGroup({ id: new GroupId('12345'), name: 'My Group' })])('My Group')();

      expect(result).toStrictEqual([new GroupId('12345')]);
    });
  });

  describe('provided an invalid group name', () => {
    it('returns an empty array', async () => {
      const result = await findGroups(() => TE.right(''), [constructGroup({ id: new GroupId('12345'), name: 'My Group' })])('Other Group')();

      expect(result).toStrictEqual([]);
    });
  });
});
