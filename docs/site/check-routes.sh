#!/bin/bash

# Check if all route targets in ws-resources.json exist in the build directory
# Usage: ./check-routes.sh [build_dir]
#   build_dir defaults to "./build"

BUILD_DIR="${1:-./build}"
RESOURCES_FILE="ws-resources.json"

if [ ! -f "$RESOURCES_FILE" ]; then
    echo "Error: $RESOURCES_FILE not found"
    exit 1
fi

if [ ! -d "$BUILD_DIR" ]; then
    echo "Error: Build directory '$BUILD_DIR' not found"
    exit 1
fi

# Extract unique route values (target paths) from the routes object
values=$(jq -r '[.routes[]] | unique | .[]' "$RESOURCES_FILE")

missing_count=0
checked_count=0

echo "Checking route values against build directory: $BUILD_DIR"
echo "================================================"

while IFS= read -r value; do
    # Remove leading slash and construct full path
    relative_path="${value#/}"
    full_path="$BUILD_DIR/$relative_path"

    ((checked_count++))

    if [ ! -f "$full_path" ]; then
        echo "MISSING: $value"
        ((missing_count++))
    fi
done <<< "$values"

echo "================================================"
echo "Checked: $checked_count files"
echo "Missing: $missing_count files"

if [ "$missing_count" -gt 0 ]; then
    exit 1
else
    echo "All route values exist as files!"
    exit 0
fi