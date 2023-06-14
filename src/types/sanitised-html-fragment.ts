import sanitiseHtml from 'sanitize-html';
import { HtmlFragment } from './html-fragment';

export type SanitisedHtmlFragment = HtmlFragment & { readonly SanitisedHtmlFragment: unique symbol };

export const sanitise = (value: HtmlFragment): SanitisedHtmlFragment => (
  sanitiseHtml(value, {
    allowedTags: sanitiseHtml.defaults.allowedTags.concat(['img']),
  }) as SanitisedHtmlFragment
);
