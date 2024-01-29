module.exports = {
  env: {
    es2020: true,
    node: true,
  },
  extends: [
    'airbnb-base',
    'airbnb-typescript/base',
    'plugin:@typescript-eslint/recommended',
    'plugin:import/recommended',
    'plugin:import/typescript',
    'plugin:fp-ts/all',
  ],
  parser: '@typescript-eslint/parser',
  parserOptions: {
    project: './tsconfig.json',
  },
  plugins: [
    '@typescript-eslint',
    'node',
    'no-loops',
    'unused-imports',
  ],
  root: true,
  rules: {
    '@typescript-eslint/array-type': ['error', {
      default: 'generic',
    }],
    '@typescript-eslint/brace-style': 'error',
    '@typescript-eslint/consistent-type-definitions': ['error', 'type'],
    '@typescript-eslint/explicit-module-boundary-types': 'error',
    '@typescript-eslint/member-delimiter-style': ['error', {
      multiline: {
        delimiter: 'comma',
        requireLast: true,
      },
      singleline: {
        delimiter: 'comma',
        requireLast: false,
      },
    }],
    '@typescript-eslint/no-floating-promises': 'error',
    '@typescript-eslint/no-misused-promises': ['error', {
      checksVoidReturn: false,
    }],
    '@typescript-eslint/no-unsafe-return': 'error',
    '@typescript-eslint/no-unused-vars': 'error',
    '@typescript-eslint/prefer-nullish-coalescing': 'error',
    '@typescript-eslint/promise-function-async': 'error',
    '@typescript-eslint/restrict-template-expressions': 'error',
    '@typescript-eslint/switch-exhaustiveness-check': 'error',
    '@typescript-eslint/type-annotation-spacing': 'error',
    'consistent-return': 'off',
    'default-case': 'off',
    'fp-ts/no-module-imports': 'off',
    'fp-ts/prefer-bimap': 'off',
    'fp-ts/prefer-chain': 'off',
    'function-paren-newline': ['error', 'consistent'],
    'import/namespace': 'off',
    'import/no-cycle': 'off',
    'import/no-relative-packages': 'off',
    'import/prefer-default-export': 'off',
    'max-len': ['error', 120, 2, {
      ignoreComments: false,
      ignoreRegExpLiterals: true,
      ignoreStrings: true,
      ignoreTemplateLiterals: true,
      ignoreUrls: true,
    }],
    'no-await-in-loop': 'off',
    'no-loops/no-loops': 2,
    'no-restricted-syntax': ['error', ...[
      'ForInStatement',
      'LabeledStatement',
      'WithStatement',
    ]],
    'no-underscore-dangle': 'off',
    'no-unreachable': 'error',
    'no-void': ['error', {
      allowAsStatement: true,
    }],
    'node/prefer-global/buffer': ['error', 'never'],
    'node/prefer-global/text-decoder': ['error', 'never'],
    'node/prefer-global/text-encoder': ['error', 'never'],
    'node/prefer-global/url': ['error', 'never'],
    'node/prefer-global/url-search-params': ['error', 'never'],
    'prefer-destructuring': 'off',
    'unused-imports/no-unused-imports-ts': 'error',
    'import/no-extraneous-dependencies': ['error', { devDependencies: ['feature-test/**/*.ts', 'test/**/*.ts'] }],
  },
  overrides: [
    {
      files: ['**/*.test.ts'],
      extends: [
        'plugin:jest/all',
        'plugin:jest-formatting/strict',
      ],
      plugins: [
        'jest',
        'jest-formatting',
      ],
      rules: {
        'jest/no-conditional-in-test': 'off',
        'jest/no-disabled-tests': 'off',
        'jest/no-hooks': 'off',
        'jest/prefer-expect-assertions': 'off',
        'jest/prefer-to-be': 'error',
        'jest/prefer-expect-resolves': 'off',
        'jest/unbound-method': 'off',
      },
    },
  ],
  settings: {
    'import/resolver': {
      typescript: {
        alwaysTryTypes: true,
      },
    },
  },
};
