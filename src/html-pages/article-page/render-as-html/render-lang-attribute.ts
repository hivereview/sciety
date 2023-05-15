import * as O from 'fp-ts/Option';
import { pipe } from 'fp-ts/function';
import { LanguageCode } from '../construct-view-model/detect-language';

export const renderLangAttribute = (code: O.Option<LanguageCode>) => pipe(
  code,
  O.match(
    () => '',
    (lc) => ` lang="${lc}"`,
  ),
);
