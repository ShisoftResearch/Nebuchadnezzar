#!/bin/bash
# Script to run tests individually in separate processes to avoid resource exhaustion
# Usage: ./run_tests_isolated.sh [filter_pattern]
# Example: ./run_tests_isolated.sh hnsw
# Example: ./run_tests_isolated.sh apps::hnsw::recovery

# Handle Ctrl+C and other signals
CURRENT_TEST_PID=""
cleanup() {
    echo ""
    echo "=========================================="
    echo "Interrupted! Cleaning up..."
    echo "=========================================="
    
    # Kill the current test if running
    if [ -n "$CURRENT_TEST_PID" ]; then
        echo "Stopping current test (PID: $CURRENT_TEST_PID)..."
        kill -TERM "$CURRENT_TEST_PID" 2>/dev/null || true
        wait "$CURRENT_TEST_PID" 2>/dev/null || true
    fi
    
    # Kill any other background processes
    jobs -p | xargs -r kill -TERM 2>/dev/null || true
    
    # Clean up temp file
    rm -f /tmp/test_output.log
    
    echo "Cleanup complete"
    exit 130
}

trap cleanup SIGINT SIGTERM

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Parse arguments
FILTER="${1:-}"
TEST_THREADS="${TEST_THREADS:-1}"
TIMEOUT="${TIMEOUT:-180}"  # Default 3 minute timeout per test

echo "=========================================="
echo "Running tests in isolated processes"
echo "Filter: ${FILTER:-<none>}"
echo "Test threads: ${TEST_THREADS}"
echo "Timeout per test: ${TIMEOUT}s"
echo "=========================================="
echo ""

# Build tests once
echo "Building tests..."
if [ -z "$FILTER" ]; then
    cargo test --lib --no-run 2>&1 | grep -E "Compiling|Finished"
else
    cargo test --lib "$FILTER" --no-run 2>&1 | grep -E "Compiling|Finished"
fi

# Find the test binary
# Try to use jq if available, otherwise fall back to find
if command -v jq >/dev/null 2>&1; then
    TEST_BINARY=$(cargo test --lib --no-run --message-format=json 2>/dev/null | \
        jq -r 'select(.profile.test == true) | select(.target.kind | contains(["lib"])) | .executable' | \
        grep -v "^null$" | head -1)
fi

if [ -z "$TEST_BINARY" ] || [ ! -f "$TEST_BINARY" ]; then
    # Fallback: find the most recently modified test binary
    TEST_BINARY=$(find target/debug/deps -name 'morpheus-*' -type f -executable -not -name '*.d' | \
        xargs ls -t 2>/dev/null | head -1)
    
    if [ -z "$TEST_BINARY" ] || [ ! -f "$TEST_BINARY" ]; then
        echo "Error: Could not find test binary in target/debug/deps"
        echo "Please run 'cargo test --lib --no-run' first"
        exit 1
    fi
fi

echo "Using test binary: $TEST_BINARY"
echo ""

# Get list of tests
echo "Discovering tests..."
if [ -z "$FILTER" ]; then
    TEST_LIST=$("$TEST_BINARY" --list 2>/dev/null | grep ': test$' | sed 's/: test$//')
else
    TEST_LIST=$("$TEST_BINARY" --list 2>/dev/null | grep "$FILTER" | grep ': test$' | sed 's/: test$//')
fi

# Count total tests
TOTAL_TESTS=$(echo "$TEST_LIST" | wc -l)
echo "Found $TOTAL_TESTS tests to run"
echo ""

# Track results
PASSED=0
FAILED=0
IGNORED=0
TIMED_OUT=0
FAILED_TESTS=()
TIMED_OUT_TESTS=()

# Run each test individually
CURRENT=0
while IFS= read -r test_name; do
    CURRENT=$((CURRENT + 1))
    
    # Skip empty lines
    [ -z "$test_name" ] && continue
    
    echo "[$CURRENT/$TOTAL_TESTS] Running: $test_name"
    
    # Run test with timeout (directly using test binary)
    # Run in background so we can track PID and handle signals
    # Use PIPESTATUS to capture the test exit code, not tee's exit code
    (timeout "$TIMEOUT" "$TEST_BINARY" "$test_name" --test-threads="$TEST_THREADS" --nocapture 2>&1; echo $? > /tmp/test_exit_code.txt) | tee /tmp/test_output.log &
    CURRENT_TEST_PID=$!
    
     # Wait for background process to complete
    wait $CURRENT_TEST_PID
    CURRENT_TEST_PID=""
    
    # Read the actual test exit code (captured by the subprocess)
    TEST_EXIT_CODE=$(cat /tmp/test_exit_code.txt 2>/dev/null || echo "1")
    
    if [ "$TEST_EXIT_CODE" -eq 0 ]; then
        # Check if test was actually ignored (look for "test result: ok. 0 passed" with ignored > 0)
        if grep -q "test result: ok. 0 passed; 0 failed; 1 ignored" /tmp/test_output.log || \
           grep -q "... ignored$" /tmp/test_output.log; then
             echo -e "${YELLOW}IGNORED${NC}"
             IGNORED=$((IGNORED + 1))
         else
             echo -e "${GREEN}PASSED${NC}"
             PASSED=$((PASSED + 1))
         fi
    else
        if [ "$TEST_EXIT_CODE" -eq 124 ]; then
             echo -e "${RED}TIMED OUT after ${TIMEOUT}s${NC}"
             TIMED_OUT=$((TIMED_OUT + 1))
             TIMED_OUT_TESTS+=("$test_name")
         elif [ "$TEST_EXIT_CODE" -eq 130 ]; then
             echo -e "${RED}INTERRUPTED${NC}"
             cleanup
         else
             echo -e "${RED}FAILED${NC}"
             FAILED=$((FAILED + 1))
             FAILED_TESTS+=("$test_name")
         fi
     fi
    
    echo ""
    
     # Small delay between tests to allow resource cleanup
    sleep 0.5
    
    # Clean up temp files
    rm -f /tmp/test_output.log /tmp/test_exit_code.txt
done <<< "$TEST_LIST"

# Print summary
echo ""
echo "=========================================="
echo "Test Summary"
echo "=========================================="
echo "Total:     $TOTAL_TESTS"
echo -e "${GREEN}Passed:    $PASSED${NC}"
echo -e "${YELLOW}Ignored:   $IGNORED${NC}"
echo -e "${RED}Failed:    $FAILED${NC}"
echo -e "${RED}Timed out: $TIMED_OUT${NC}"

if [ $FAILED -gt 0 ]; then
    echo ""
    echo "Failed tests:"
    for test in "${FAILED_TESTS[@]}"; do
        echo "  - $test"
    done
fi

if [ $TIMED_OUT -gt 0 ]; then
    echo ""
    echo "Timed out tests:"
    for test in "${TIMED_OUT_TESTS[@]}"; do
        echo "  - $test"
    done
fi

echo ""

# Clean up temp files
rm -f /tmp/test_output.log /tmp/test_exit_code.txt

# Exit with error if any tests failed
if [ $FAILED -gt 0 ] || [ $TIMED_OUT -gt 0 ]; then
    exit 1
fi

exit 0
