import { createReadAndWriteSides, ReadAndWriteSides } from './create-read-and-write-sides';
import { CommandHelpers, createCommandHelpers } from './create-command-helpers';
import { createHappyPathThirdPartyAdapters, HappyPathThirdPartyAdapters } from './happy-path-third-party-adapters';
import { AbortTest, abortTest } from './abort-test';
import { Logger } from '../../src/shared-ports';
import { Queries } from '../../src/read-models';
import { dummyLogger } from '../dummy-logger';

export type TestFramework = ReadAndWriteSides & {
  abortTest: AbortTest,
  commandHelpers: CommandHelpers,
  happyPathThirdParties: HappyPathThirdPartyAdapters,
  dependenciesForViews: Queries & HappyPathThirdPartyAdapters & { logger: Logger },
};

export const createTestFramework = (): TestFramework => {
  const framework = createReadAndWriteSides();
  const happyPathThirdParties = createHappyPathThirdPartyAdapters();
  return {
    ...framework,
    abortTest,
    commandHelpers: createCommandHelpers(
      framework.commandHandlers,
      {
        commitEvents: framework.commitEvents,
        getAllEvents: framework.getAllEvents,
      },
    ),
    happyPathThirdParties,
    dependenciesForViews: {
      ...framework.queries,
      ...happyPathThirdParties,
      logger: dummyLogger,
    },
  };
};
