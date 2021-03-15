#ifndef STARDUST_DB_H
#define STARDUST_DB_H

/* Warning, this file is generated automatically. Do not modify. */

#define STARDUST_DB_OK 0

#define STARDUST_DB_INVALID_PATH_UTF_8 1

#define STARDUST_DB_INVALID_PATH_LOCATION 2

typedef struct Database Database;

typedef struct Db {
  struct Database *database;
} Db;

int open_database(const char *path, struct Db *db);

#endif /* STARDUST_DB_H */
