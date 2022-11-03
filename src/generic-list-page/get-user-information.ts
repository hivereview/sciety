import * as E from 'fp-ts/Either';
import * as TE from 'fp-ts/TaskEither';
import * as DE from '../types/data-error';
import { UserId } from '../types/user-id';

type OwnerInfo = {
  ownerName: string,
  ownerHref: string,
  ownerAvatarPath: string,
};

type UserDetails = {
  avatarUrl: string,
  displayName: string,
  handle: string,
};

export type Ports = {
  getUserDetails: (userId: UserId) => TE.TaskEither<DE.DataError, UserDetails>,
};

type GetUserOwnerInformation = (ports: Ports) => (userId: UserId) => TE.TaskEither<DE.DataError, OwnerInfo>;

export const getUserOwnerInformation: GetUserOwnerInformation = () => (userId) => {
  switch (userId) {
    case '931653361':
      return TE.right({
        ownerName: 'David Ashbrook',
        ownerHref: '/users/DavidAshbrook',
        ownerAvatarPath: 'https://pbs.twimg.com/profile_images/1503119472353239040/eJgS9Y1y_normal.jpg',
      });
    case '1238289812307632129':
      return TE.right({
        ownerName: 'Ruchika Bajaj',
        ownerHref: '/users/RuchikaBajaj9',
        ownerAvatarPath: 'https://pbs.twimg.com/profile_images/1426490209990975489/tkYaltji_normal.jpg',
      });
    case '1338873008283377664':
      return TE.right({
        ownerName: 'accountfortesting',
        ownerHref: '/users/account27775998',
        ownerAvatarPath: 'https://abs.twimg.com/sticky/default_profile_images/default_profile_normal.png',
      });
    default:
      return TE.right({
        ownerName: 'Getting owner info is not implemented',
        ownerHref: '/users/not-a-valid-user-id',
        ownerAvatarPath: 'https://abs.twimg.com/sticky/default_profile_images/default_profile_normal.png',
      });
  }
};
