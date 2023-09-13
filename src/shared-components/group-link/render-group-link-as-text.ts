import { HtmlFragment, toHtmlFragment } from '../../types/html-fragment';
import { GroupLinkAsTextViewModel } from './group-link-as-text-view-model';

export const renderGroupLinkAsText = (group: GroupLinkAsTextViewModel): HtmlFragment => toHtmlFragment(`<a href="${group.groupPageHref}">${group.groupName}</a>`);
