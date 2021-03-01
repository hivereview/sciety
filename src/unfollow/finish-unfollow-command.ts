import { Middleware } from 'koa';
import { CommitEvents, GetFollowList, unfollowCommand } from './unfollow-command';
import { EditorialCommunityId } from '../types/editorial-community-id';

type Ports = {
  commitEvents: CommitEvents,
  getFollowList: GetFollowList,
};

export const finishUnfollowCommand = (ports: Ports): Middleware => {
  const command = unfollowCommand(
    ports.getFollowList,
    ports.commitEvents,
  );
  return async (context, next) => {
    if (context.session.command === 'unfollow' && context.session.editorialCommunityId) {
      const editorialCommunityId = new EditorialCommunityId(context.session.editorialCommunityId);
      const { user } = context.state;
      await command(user, editorialCommunityId);
    }

    await next();
  };
};
