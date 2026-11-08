# Not run or verified in this pass -- see tests/docker/chinook/README.md and
# README.md's "Banc d'essai multi-moteurs" section. Behind the `mssql` compose
# profile, disabled by default: the base image is x86_64-only (no ARM64
# build), ~1.5 GB, and noticeably heavier to start than Postgres/MySQL.
FROM mcr.microsoft.com/mssql/server:2022-latest

COPY Chinook_SqlServer.sql /chinook/Chinook_SqlServer.sql
COPY init.sh /chinook/init.sh

USER root
RUN chmod +x /chinook/init.sh
USER mssql

ENTRYPOINT ["/chinook/init.sh"]
