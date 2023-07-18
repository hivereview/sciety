import { pipe } from 'fp-ts/function';
import * as RA from 'fp-ts/ReadonlyArray';
import { ViewModel } from './view-model';

const renderEntries = (entries: ViewModel['content']['articles']) => pipe(
  entries,
  RA.map((entry) => `
  <entry>
    <title>${entry.articleCard.title}</title>
    <link rel="alternate" type="text/html" href="https://sciety.org${entry.articleCard.articleLink}"/>
    <id>https://sciety.org${entry.articleCard.articleLink}</id>
    <published>{{ENTRY.FIRST_POST_TIME in RFC3339 format}}</published>
    <updated>{{ENTRY.LAST_UPDATE_TIME in RFC3339 format}}</updated>
    <content type="html">{{ENTRY.HTML}}</content>
  </entry>
  `),
  (items) => items.join('\n'),
);

export const renderAsAtom = (viewModel: ViewModel): string => `<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>${viewModel.name}</title>
  <link rel="alternate" href="${viewModel.listPageAbsoluteUrl.toString()}"/>
  <link rel="self" href="${viewModel.listPageAbsoluteUrl.toString()}/feed.atom"/>
  <updated>${viewModel.updatedAt.toISOString()}</updated>
  <author>
    <name>${viewModel.ownerName}</name>
  </author>
  <id>urn:uuid:${viewModel.listId}</id>

  ${renderEntries(viewModel.content.articles)}

</feed>
`;
