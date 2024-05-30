import { pipe } from 'fp-ts/function';
import * as DE from '../../../../types/data-error';
import { ErrorPageBodyViewModel, toErrorPageBodyViewModel } from '../../../../types/error-page-body-view-model';
import { toHtmlFragment } from '../../../../types/html-fragment';

export const renderErrorPage = (e: DE.DataError): ErrorPageBodyViewModel => pipe(
  e,
  DE.match({
    notFound: () => 'We couldn\'t find this information.',
    unavailable: () => 'We couldn\'t retrieve this information. Please try again.',
    notAuthorised: () => 'You aren\'t permitted to do that.',
  }),
  toHtmlFragment,
  (message) => toErrorPageBodyViewModel({
    type: e,
    message,
  }),
);
