"use strict";
module.exports = {
    extends: '@coinify/eslint-config-coinify',
    parser: '@typescript-eslint/parser',
    parserOptions: {
        project: './tsconfig.json',
        tsconfigRootDir: __dirname,
        sourceType: 'module'
    },
    rules: {
        'mocha/no-exclusive-tests': 'error',
        'mocha/no-pending-tests': 'warn',
        'no-console': 'warn',
        'no-else-return': 'error',
        'no-implicit-coercion': ['error', { allow: ['!!'] }],
        'quote-props': ['error', 'as-needed'],
        'require-await': 'error',
        '@typescript-eslint/await-thenable': 'error',
        '@typescript-eslint/camelcase': 'off',
        '@typescript-eslint/explicit-function-return-type': 'off',
        '@typescript-eslint/explicit-module-boundary-types': 'off',
        '@typescript-eslint/indent': ['error', 2],
        '@typescript-eslint/no-empty-function': 'off',
        '@typescript-eslint/no-explicit-any': 'off',
        '@typescript-eslint/no-floating-promises': 'error',
        '@typescript-eslint/no-use-before-define': 'off',
        '@typescript-eslint/no-unnecessary-type-assertion': 'warn',
        '@typescript-eslint/no-unused-vars': ['warn', { argsIgnorePattern: '^_' }],
        '@typescript-eslint/quotes': ['error', 'single']
    }
};
