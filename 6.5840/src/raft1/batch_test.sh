#!/bin/bash
# filepath: /home/ryuzaki/Desktop/Distributed Systems/6.5840/src/raft1/batch_test.sh

if [ -z "$1" ]; then
    echo "Usage: ./batch_test.sh <number_of_runs>"
    exit 1
fi

runs=$1

echo "Running Raft tests $runs times..."
echo "=============================="

failed=0

for i in $(seq 1 $runs); do
    echo ""
    echo "========== Run $i of $runs =========="
    echo ""
    
    echo "--- Test 3A ---"
    if go test -run 3A; then
        echo "3A: PASSED"
    else
        echo "3A: FAILED"
        ((failed++))
    fi
    sleep 2
    
    echo ""
    echo "--- Test 3B ---"
    if go test -run 3B; then
        echo "3B: PASSED"
    else
        echo "3B: FAILED"
        ((failed++))
    fi
    sleep 2
    
    echo ""
    echo "--- Test 3C ---"
    if go test -run 3C; then
        echo "3C: PASSED"
    else
        echo "3C: FAILED"
        ((failed++))
    fi
    sleep 2

    echo ""
    echo "--- Test 3D ---"
    if go test -run 3D; then
        echo "3D: PASSED"
    else
        echo "3D: FAILED"
        ((failed++))
    fi
    
    if [ $i -lt $runs ]; then
        echo ""
        echo "Sleeping 5 seconds before next run..."
        sleep 5
    fi
done

echo ""
echo "=============================="
echo "All runs completed!"
echo "Failed test suites: $failed"