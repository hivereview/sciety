import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { FetchStaticFile, renderDescription } from './render-description';
import { renderLists } from './render-lists';
import { List } from '../../shared-read-models/lists';
import * as DE from '../../types/data-error';
import { Group } from '../../types/group';
import { HtmlFragment, toHtmlFragment } from '../../types/html-fragment';
import * as LOID from '../../types/list-owner-id';
import { ListOwnerId } from '../../types/list-owner-id';

export type Ports = {
  fetchStaticFile: FetchStaticFile,
  getListsOwnedBy: (ownerId: ListOwnerId) => TE.TaskEither<DE.DataError, ReadonlyArray<List>>,
};

type RenderAbout = (
  about: { lists: HtmlFragment, description: HtmlFragment }
) => HtmlFragment;

const renderAbout: RenderAbout = (about) => pipe(
  `
  <div class="group-page-about">
    ${about.lists}
    ${about.description}
  </div>
`,
  toHtmlFragment,
);

const getRenderedDescription = (ports: Ports) => (group: Group): TE.TaskEither<DE.DataError, HtmlFragment> => pipe(
  `groups/${group.descriptionPath}`,
  ports.fetchStaticFile,
  TE.map(renderDescription),
);

const getRenderedLists = (ports: Ports) => (group: Group): HtmlFragment => pipe(
  group.id,
  LOID.fromGroupId,
  ports.getListsOwnedBy,
  renderLists,
);

export const about = (ports: Ports) => (group: Group): TE.TaskEither<DE.DataError, HtmlFragment> => pipe(
  group,
  getRenderedDescription(ports),
  TE.map((renderedDescription) => ({
    description: renderedDescription,
    lists: getRenderedLists(ports)(group),
  })),
  TE.map(renderAbout),
);
