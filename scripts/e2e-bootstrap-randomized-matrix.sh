#!/usr/bin/env bash
set -euo pipefail

SERVER_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$SERVER_ROOT"

PG_USER="${MATRIX_POSTGRES_USER:-root}"
PG_PASS="${MATRIX_POSTGRES_PASSWORD:-1234}"
PG_DB="${MATRIX_POSTGRES_DATABASE:-enfyra}"
MYSQL_USER="${MATRIX_MYSQL_USER:-root}"
MYSQL_PASS="${MATRIX_MYSQL_PASSWORD:-1234}"
MYSQL_DB="${MATRIX_MYSQL_DATABASE:-enfyra_matrix}"
MONGO_USER="${MATRIX_MONGO_USER:-}"
MONGO_PASS="${MATRIX_MONGO_PASSWORD:-}"
MONGO_AUTH_DB="${MATRIX_MONGO_AUTH_DATABASE:-admin}"
MONGO_CONTAINER="${MATRIX_MONGO_CONTAINER:-enfyra-mongodb}"

if [[ -z "$MONGO_USER" || -z "$MONGO_PASS" ]] && command -v docker >/dev/null 2>&1 && docker inspect "$MONGO_CONTAINER" >/dev/null 2>&1; then
  docker_mongo_user="$(docker exec "$MONGO_CONTAINER" printenv MONGO_INITDB_ROOT_USERNAME 2>/dev/null || true)"
  docker_mongo_pass="$(docker exec "$MONGO_CONTAINER" printenv MONGO_INITDB_ROOT_PASSWORD 2>/dev/null || true)"
  MONGO_USER="${MONGO_USER:-$docker_mongo_user}"
  MONGO_PASS="${MONGO_PASS:-$docker_mongo_pass}"
fi

MONGO_USER="${MONGO_USER:-enfyra_admin}"
MONGO_PASS="${MONGO_PASS:-enfyra_password_123}"

MATRIX_POSTGRES_USER="$PG_USER" \
MATRIX_POSTGRES_PASSWORD="$PG_PASS" \
MATRIX_POSTGRES_DATABASE="$PG_DB" \
MATRIX_MYSQL_USER="$MYSQL_USER" \
MATRIX_MYSQL_PASSWORD="$MYSQL_PASS" \
MATRIX_MYSQL_DATABASE="$MYSQL_DB" \
MATRIX_MONGO_USER="$MONGO_USER" \
MATRIX_MONGO_PASSWORD="$MONGO_PASS" \
MATRIX_MONGO_AUTH_DATABASE="$MONGO_AUTH_DB" \
NODE_OPTIONS="--no-node-snapshot${NODE_OPTIONS:+ $NODE_OPTIONS}" \
yarn tsx test/e2e/bootstrap-randomized-matrix.e2e.ts
