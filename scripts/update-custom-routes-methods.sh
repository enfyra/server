#!/usr/bin/env bash
# Update all custom (user-created) routes to have all methods
# Usage: ./scripts/update-custom-routes-methods.sh [BASE_URL]
# Example: ./scripts/update-custom-routes-methods.sh http://localhost:1105

BASE_URL="${1:-http://localhost:1105}"
ALL_METHOD_IDS="[1,2,3,4,5,6]"  # GQL_QUERY, GQL_MUTATION, GET, POST, PATCH, DELETE

echo "Fetching routes from $BASE_URL/route_definition?limit=0 ..."
ROUTES=$(curl -s "$BASE_URL/route_definition?limit=0")

CUSTOM_IDS=$(echo "$ROUTES" | jq -r '.data[] | select(.isSystem == false) | .id')

if [ -z "$CUSTOM_IDS" ]; then
  echo "No custom routes found (isSystem=false). Nothing to update."
  exit 0
fi

COUNT=0
for ID in $CUSTOM_IDS; do
  PATH_=$(echo "$ROUTES" | jq -r ".data[] | select(.id == $ID) | .path")
  echo "Updating route $ID ($PATH_) ..."
  RESP=$(curl -s -X PATCH "$BASE_URL/route_definition/$ID" \
    -H "Content-Type: application/json" \
    -d "{\"availableMethods\": $ALL_METHOD_IDS}")
  if echo "$RESP" | jq -e '.data' >/dev/null 2>&1; then
    echo "  OK"
    COUNT=$((COUNT + 1))
  else
    echo "  FAILED: $RESP"
  fi
done

echo "Done. Updated $COUNT custom route(s)."
echo "Run POST $BASE_URL/admin/reload/routes to refresh route cache."
