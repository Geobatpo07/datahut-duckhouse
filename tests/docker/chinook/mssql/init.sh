#!/usr/bin/env bash
# Loads Chinook_SqlServer.sql into a just-started mssql-server container.
#
# The official mcr.microsoft.com/mssql/server image has no
# docker-entrypoint-initdb.d mechanism (unlike postgres/mysql), so this
# script runs as the container's entrypoint: start the real server in the
# background, wait for it to accept connections, run the Chinook script
# once, then exec into the real server process so the container's lifecycle
# (signals, `docker compose down`, etc.) behaves normally.
set -euo pipefail

/opt/mssql/bin/sqlservr &
SQLSERVR_PID=$!

echo "Waiting for SQL Server to accept connections..."
for _ in $(seq 1 60); do
  if /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -Q "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

if ! /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -Q "SELECT 1" >/dev/null 2>&1; then
  echo "SQL Server did not become ready in time" >&2
  exit 1
fi

echo "Loading Chinook_SqlServer.sql..."
/opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -i /chinook/Chinook_SqlServer.sql

echo "Chinook loaded. Handing off to the SQL Server process."
wait "$SQLSERVR_PID"
