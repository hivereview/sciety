import { Strategy as TwitterStrategy } from 'passport-twitter';
import { toUserId } from '../../types/user-id';
import { createAccountIfNecessary, Ports } from '../../user-account/create-account-if-necessary';
import { writeUserToState } from './setup-auth0-strategy';

export const setupTwitterStrategy = (ports: Ports) => new TwitterStrategy(
  {
    consumerKey: process.env.TWITTER_API_KEY ?? '',
    consumerSecret: process.env.TWITTER_API_SECRET_KEY ?? '',
    callbackURL: `${process.env.APP_ORIGIN ?? ''}/twitter/callback`,
  },
  (_token, _tokenSecret, profile, cb) => {
    // photos can never be undefined:
    // https://github.com/jaredhanson/passport-twitter/blob/cfe7807b0e89e9ff130592c28622e134749e757b/lib/profile.js#L21
    const photos = profile.photos ?? [{ value: '' }];
    const userAccount = {
      id: toUserId(profile.id),
      handle: profile.username,
      avatarUrl: photos[0].value,
      displayName: profile.displayName,
    };
    void createAccountIfNecessary(ports)(userAccount)()
      .then(() => writeUserToState(cb)(userAccount));
  },
);
