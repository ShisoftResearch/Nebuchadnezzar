#!/bin/bash

# Script to run corruption detection tests
# These tests are designed to detect the "Cannot decode entry header" panic

set -e

echo "================================================"
echo "Transaction Corruption Detection Test Suite"
echo "================================================"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Parse arguments
ITERATIONS=1
VERBOSE=false
TEST_NAME=""
BACKTRACE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -i|--iterations)
            ITERATIONS="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -b|--backtrace)
            BACKTRACE=true
            shift
            ;;
        -t|--test)
            TEST_NAME="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -i, --iterations N    Run tests N times (default: 1)"
            echo "  -v, --verbose         Enable verbose output with logging"
            echo "  -b, --backtrace       Enable full backtraces"
            echo "  -t, --test NAME       Run specific test by name"
            echo "  -h, --help            Show this help message"
            echo ""
            echo "Available tests:"
            echo "  - test_rapid_concurrent_updates_same_cell"
            echo "  - test_varying_size_concurrent_updates"
            echo "  - test_multi_cell_concurrent_transactions"
            echo "  - test_rapid_commit_sequence"
            echo "  - test_interleaved_prepare_commit"
            echo "  - test_maximum_concurrency_stress"
            echo ""
            echo "Examples:"
            echo "  $0                                    # Run all tests once"
            echo "  $0 -i 10                             # Run all tests 10 times"
            echo "  $0 -v -b                             # Run with verbose output and backtraces"
            echo "  $0 -t test_rapid_concurrent_updates_same_cell -i 5  # Run specific test 5 times"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use -h or --help for usage information"
            exit 1
            ;;
    esac
done

# Set environment variables
export RUST_TEST_THREADS=1
if [ "$VERBOSE" = true ]; then
    export RUST_LOG=debug
fi
if [ "$BACKTRACE" = true ]; then
    export RUST_BACKTRACE=full
fi

# Determine test filter
if [ -n "$TEST_NAME" ]; then
    TEST_FILTER="corruption_tests::$TEST_NAME"
    echo "Running specific test: $TEST_NAME"
else
    TEST_FILTER="corruption_tests::"
    echo "Running all corruption tests"
fi

echo "Iterations: $ITERATIONS"
echo "Verbose: $VERBOSE"
echo "Backtrace: $BACKTRACE"
echo ""

# Run tests
FAILED=0
PASSED=0

for i in $(seq 1 $ITERATIONS); do
    echo "----------------------------------------"
    echo -e "${YELLOW}Iteration $i of $ITERATIONS${NC}"
    echo "----------------------------------------"
    
    if cargo test --package nebuchadnezzar "$TEST_FILTER" -- --nocapture; then
        PASSED=$((PASSED + 1))
        echo -e "${GREEN}✓ Iteration $i: PASSED${NC}"
    else
        FAILED=$((FAILED + 1))
        echo -e "${RED}✗ Iteration $i: FAILED${NC}"
        
        if [ $ITERATIONS -gt 1 ]; then
            echo ""
            echo -e "${RED}Test failed on iteration $i${NC}"
            echo "You can re-run the failing test with:"
            if [ -n "$TEST_NAME" ]; then
                echo "  cargo test --package nebuchadnezzar corruption_tests::$TEST_NAME -- --nocapture"
            else
                echo "  cargo test --package nebuchadnezzar corruption_tests -- --nocapture"
            fi
            break
        fi
    fi
    echo ""
done

# Summary
echo "================================================"
echo "Test Summary"
echo "================================================"
echo -e "Total iterations: $ITERATIONS"
echo -e "${GREEN}Passed: $PASSED${NC}"
echo -e "${RED}Failed: $FAILED${NC}"
echo ""

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}Some tests failed. Check the output above for details.${NC}"
    echo ""
    echo "Tips for debugging:"
    echo "1. Run with --verbose to see detailed logs"
    echo "2. Run with --backtrace to see full stack traces"
    echo "3. Run specific failing tests with -t <test_name>"
    echo "4. Check CORRUPTION_TEST_README.md for more information"
    exit 1
fi

