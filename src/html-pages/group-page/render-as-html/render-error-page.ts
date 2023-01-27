import { pipe } from 'fp-ts/function';
import * as DE from '../../../types/data-error';
import { toHtmlFragment } from '../../../types/html-fragment';
import { RenderPageError } from '../../../types/render-page-error';

export const renderErrorPage = (e: DE.DataError): RenderPageError => pipe(
  e,
  DE.match({
    notFound: () => 'No such group. Please check and try again.',
    unavailable: () => 'We couldn\'t retrieve this information. Please try again.',
  }),
  toHtmlFragment,
  (message) => ({
    type: e,
    message,
  }),
);
