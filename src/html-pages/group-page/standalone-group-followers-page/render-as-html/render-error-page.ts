import { pipe } from 'fp-ts/function';
import * as DE from '../../../../types/data-error';
import { toHtmlFragment } from '../../../../types/html-fragment';
import { ErrorPageBodyViewModel, toErrorPageBodyViewModel } from '../../../../types/error-page-body-view-model';

export const renderErrorPage = (e: DE.DataError): ErrorPageBodyViewModel => pipe(
  e,
  DE.match({
    notFound: () => 'No such group. Please check and try again.',
    unavailable: () => 'We couldn\'t retrieve this information. Please try again.',
  }),
  toHtmlFragment,
  (message) => toErrorPageBodyViewModel({
    type: e,
    message,
  }),
);
