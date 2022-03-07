module.exports = {
    preset: 'ts-jest',
    collectCoverageFrom: [
        "src/**/*.{ts,tsx}",
        "!src/requests/**/*"
    ],
    // testMatch: [ "src/*/__tests__/**/*.[jt]s?(x)"],
    testPathIgnorePatterns: ['/node_modules', 'dist'],
    coverageThreshold: {
        global: {
            statements: 100,
            branches: 100,
            functions: 100,
            lines: 100
        }
    }
};
