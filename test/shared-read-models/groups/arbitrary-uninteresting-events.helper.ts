import {
  constructEvent,
  evaluationRecorded, userFollowedEditorialCommunity,
} from '../../../src/domain-events';
import { arbitraryDate } from '../../helpers';
import { arbitraryArticleId } from '../../types/article-id.helper';
import { arbitraryGroupId } from '../../types/group-id.helper';
import { arbitraryGroup } from '../../types/group.helper';
import { arbitraryEvaluationLocator } from '../../types/evaluation-locator.helper';
import { arbitraryUserId } from '../../types/user-id.helper';

const group = arbitraryGroup();

export const arbitraryUninterestingEvents = [
  constructEvent('GroupJoined')({
    groupId: group.id,
    name: group.name,
    avatarPath: group.avatarPath,
    descriptionPath: group.descriptionPath,
    shortDescription: group.shortDescription,
    homepage: group.homepage,
    slug: group.slug,
  }),
  userFollowedEditorialCommunity(arbitraryUserId(), arbitraryGroupId()),
  evaluationRecorded(group.id, arbitraryArticleId(), arbitraryEvaluationLocator(), [], arbitraryDate()),
];
