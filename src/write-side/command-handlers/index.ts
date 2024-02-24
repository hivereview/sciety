import {
  AddArticleToListCommand,
  CreateListCommand,
  EditListDetailsCommand,
  RecordSubjectAreaCommand,
  RemoveArticleFromListCommand,
} from '../commands/index.js';
import { CommandHandler } from './command-handler.js';

export { createUserAccountCommandHandler } from './create-user-account-command-handler.js';
export { updateUserDetailsCommandHandler } from './update-user-details-command-handler.js';
export { editListDetailsCommandHandler } from './edit-list-details-command-handler.js';
export { recordEvaluationPublicationCommandHandler } from './record-evaluation-publication-command-handler.js';
export { createListCommandHandler } from './create-list-command-handler.js';
export { recordSubjectAreaCommandHandler } from './record-subject-area-command-handler.js';
export { removeArticleFromListCommandHandler } from './remove-article-from-list-command-handler.js';
export { followCommandHandler } from './follow-command-handler.js';
export { unfollowCommandHandler } from './unfollow-command-handler.js';
export { addArticleToListCommandHandler } from './add-article-to-list-command-handler.js';

export type CommandHandlers = {
  addArticleToList: CommandHandler<AddArticleToListCommand>,
  createList: CommandHandler<CreateListCommand>,
  editListDetails: CommandHandler<EditListDetailsCommand>,
  recordSubjectArea: CommandHandler<RecordSubjectAreaCommand>,
  removeArticleFromList: CommandHandler<RemoveArticleFromListCommand>,
};
