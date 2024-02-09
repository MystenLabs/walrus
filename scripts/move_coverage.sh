#!/bin/bash

shopt -s nullglob

MIN_COVERAGE=${1:-70}
RED='\033[0;31m'
GREEN='\033[0;32m'
BOLD='\033[1m'
NORMAL='\033[0m'


# 1) Run tests and record coverage
error=0
for dir in contracts/*/; do
    echo -e "\nTesting $dir..."
    cd $dir
    sui move test --coverage
    [ $? -ne 0 ] && error=1
    cd ../..
done

if [ $error -ne 0 ]; then
    echo -e ${RED}ERROR${NORMAL}: Some Move tests failed.
    exit $error
fi


# 2) Check coverage and print summaries
error=0
for dir in contracts/*; do
    cd $dir
    coverage_summary=$(sui move coverage summary)
    echo -e "\n${BOLD}Coverage summary for $dir:${NORMAL}\n$coverage_summary"
    coverage_percentage=$(echo "$coverage_summary" | awk '/Move Coverage:/ {print $5}')
    if [[ ${coverage_percentage%.*} -lt $MIN_COVERAGE ]]; then
        echo -e ${RED}ERROR${NORMAL}: \
            Contract $dir has a coverage of ${RED}$coverage_percentage%${NORMAL}, \
            which is below the minimal acceptable coverage of $MIN_COVERAGE%.
        echo '       Run "sui move coverage source --module $module"' to find uncovered lines.
        error=2
    else
        echo -e ${GREEN}SUCCESS${NORMAL}: \
            Contract $dir has a coverage of ${GREEN}$coverage_percentage%${NORMAL}, \
            which is above the minimal acceptable coverage of $MIN_COVERAGE%.
    fi
    cd ../..
done

if [ $error -ne 0 ]; then
    echo -e ${RED}ERROR${NORMAL}: Some Move contracts had insufficient test coverage.
    exit $error
fi

exit 0
