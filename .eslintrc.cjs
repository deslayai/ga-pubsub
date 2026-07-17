/** @type {import('eslint').Linter.Config} */
module.exports = {
  root: true,
  parser: '@typescript-eslint/parser',
  parserOptions: {
    project: ['./packages/*/tsconfig.json'],
    tsconfigRootDir: __dirname,
    ecmaVersion: 2022,
    sourceType: 'module',
  },
  plugins: ['@typescript-eslint'],
  extends: [
    'eslint:recommended',
    'plugin:@typescript-eslint/recommended',
    'plugin:@typescript-eslint/recommended-requiring-type-checking',
  ],
  rules: {
    // ── Security-critical rules ────────────────────────────────────────────
    'no-eval':                              'error',
    'no-implied-eval':                      'error',
    'no-new-func':                          'error',
    '@typescript-eslint/no-explicit-any':   'error',
    '@typescript-eslint/no-unsafe-assignment':   'warn',
    '@typescript-eslint/no-unsafe-call':         'warn',
    '@typescript-eslint/no-unsafe-member-access': 'warn',
    '@typescript-eslint/no-unsafe-return':       'warn',

    // ── Code quality ──────────────────────────────────────────────────────
    '@typescript-eslint/explicit-function-return-type': ['warn', {
      allowExpressions: true,
      allowTypedFunctionExpressions: true,
    }],
    '@typescript-eslint/no-unused-vars': ['error', {
      argsIgnorePattern: '^_',
      varsIgnorePattern: '^_',
    }],
    '@typescript-eslint/prefer-nullish-coalescing':  'warn',
    '@typescript-eslint/prefer-optional-chain':      'warn',
    '@typescript-eslint/no-non-null-assertion':      'warn',
    '@typescript-eslint/consistent-type-imports':    ['error', { prefer: 'type-imports' }],
    '@typescript-eslint/no-floating-promises':       'error',
    '@typescript-eslint/await-thenable':             'error',
    '@typescript-eslint/require-await':              'off', // async functions without await are fine

    // ── Style ──────────────────────────────────────────────────────────────
    'no-console': ['warn', { allow: ['warn', 'error', 'debug'] }],
    'eqeqeq':     ['error', 'always'],
    'curly':      ['error', 'all'],
  },
  overrides: [
    {
      // Test files — relax some rules
      files: ['tests/**/*.ts', '**/*.test.ts'],
      rules: {
        '@typescript-eslint/no-explicit-any': 'off',
        '@typescript-eslint/no-unsafe-assignment': 'off',
        '@typescript-eslint/no-unsafe-call': 'off',
        '@typescript-eslint/no-unsafe-member-access': 'off',
        'no-console': 'off',
      },
    },
    {
      // Example files — even more relaxed
      files: ['docs/**/*.ts'],
      rules: {
        '@typescript-eslint/no-explicit-any':   'off',
        '@typescript-eslint/require-await':     'off',
        'no-console':                           'off',
      },
    },
  ],
  ignorePatterns: ['dist/', 'node_modules/', 'coverage/', '*.js', '*.cjs', '*.mjs'],
};
