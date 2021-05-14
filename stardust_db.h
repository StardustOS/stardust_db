#ifndef STARDUST_DB_H
#define STARDUST_DB_H

/* Warning, this file is generated automatically. Do not modify. */

#include <stdint.h>

#define STARDUST_DB_OK 0

#define STARDUST_DB_INVALID_PATH_UTF_8 1

#define STARDUST_DB_INVALID_PATH_LOCATION 2

#define STARDUST_DB_NULL_ROW_SET 3

#define STARDUST_DB_NULL_DB 4

#define STARDUST_DB_INVALID_QUERY_UTF_8 5

#define STARDUST_DB_NO_RESULT 6

#define STARDUST_DB_EXECUTION_ERROR 7

#define STARDUST_DB_END 8

#define STARDUST_DB_NO_COLUMN 9

#define STARDUST_DB_BUFFER_TOO_SMALL 10

#define STARDUST_DB_VALUE_WRONG_TYPE 11

#define STARDUST_DB_VALUE_NULL 12

#define STARDUST_DB_TEMP_DB_ERROR 13

typedef struct Database Database;

typedef struct Relation Relation;

typedef struct TemporaryDatabase TemporaryDatabase;

typedef enum Db_Tag {
  Ordinary,
  Temporary,
} Db_Tag;

typedef struct Db {
  Db_Tag tag;
  union {
    struct {
      struct Database *ordinary;
    };
    struct {
      struct TemporaryDatabase *temporary;
    };
  };
} Db;

typedef struct RowSet {
  struct Relation *relation;
  uintptr_t current_row;
} RowSet;

typedef int64_t IntegerStorage;

#define ROW_SET_INIT (RowSet){ .relation = (Relation*)0, .current_row = 0 }

/**
 * Opens the database at the specified path. Returns `STARDUST_DB_OK` on success.
 */
int open_database(const char *path, struct Db *db);

/**
 * Opens a temporary database. Returns `STARDUST_DB_OK` on success.
 */
int temp_db(struct Db *db);

/**
 * Closes the database. This function should always succeed.
 */
void close_db(struct Db *db);

/**
 * Frees the memory from the `RowSet`.
 */
void close_row_set(struct RowSet *row_set);

/**
 * Executes the query in `query` and places the result in `row_set`.
 * Errors will be placed in the buffer at `err_buf`, which must be no smaller than `err_buff_len`.
 */
int execute_query(struct Db *db,
                  const char *query,
                  struct RowSet *row_set,
                  char *err_buff,
                  uintptr_t err_buff_len);

/**
 * Move to the next row in the `RowSet`. Returns `STARDUST_DB_END` if the row is past the end of the `RowSet`.
 */
int next_row(struct RowSet *row_set);

/**
 * Set the current row of the `RowSet` to the specified value. Returns `STARDUST_DB_END` if the row is past the end of the `RowSet`.
 */
int set_row(struct RowSet *row_set,
            uintptr_t row);

/**
 * Sets the value in `is_end` to 1 if the current row is past the end of the `RowSet`, otherwise the value is set to 0.
 */
int is_end(const struct RowSet *row_set,
           int *is_end);

/**
 * Sets the value in `num_columns` to be the number of columns in the `RowSet`.
 */
int num_columns(const struct RowSet *row_set, uintptr_t *num_columns);

/**
 * Sets the value in `num_rows` to be the number of rows in the `RowSet`.
 */
int num_rows(const struct RowSet *row_set, uintptr_t *num_rows);

/**
 * Sets the value in `is_null` to 1 if the value at the specified column is Null, otherwise 0.
 */
int is_null_index(const struct RowSet *row_set, uintptr_t column, int *is_null);

/**
 * Sets the value in `is_string` to 1 if the value at the specified column is a string, otherwise 0.
 */
int is_string_index(const struct RowSet *row_set, uintptr_t column, int *is_string);

/**
 * Sets the value in `is_int` to 1 if the value at the specified column is an integer, otherwise 0.
 */
int is_int_index(const struct RowSet *row_set, uintptr_t column, int *is_int);

/**
 * If the value at the specified column is a string, copy the value to the buffer, otherwise a type error is returned.
 * `STARDUST_DB_BUFFER_TOO_SMALL` is returned if the string buffer is too small.
 */
int get_string_index(const struct RowSet *row_set,
                     uintptr_t column,
                     char *string_buffer,
                     uintptr_t buffer_len);

/**
 * If the value at the specified column is an integer, copy the value to the buffer, otherwise a type error is returned.
 */
int get_int_index(const struct RowSet *row_set,
                  uintptr_t column,
                  IntegerStorage *int_buffer);

/**
 * Cast the value to a string and copy the value to the buffer. An error will be returned if the value is null.
 * `STARDUST_DB_BUFFER_TOO_SMALL` is returned if the string buffer is too small.
 */
int get_string_index_cast(const struct RowSet *row_set,
                          uintptr_t column,
                          char *string_buffer,
                          uintptr_t buffer_len);

/**
 * Cast the value to an integer and copy the value to the buffer. An error will be returned if the value is null.
 */
int get_int_index_cast(const struct RowSet *row_set,
                       uintptr_t column,
                       IntegerStorage *int_buffer);

/**
 * Sets the value in `is_null` to 1 if the value at the specified column is null, otherwise 0.
 */
int is_null_named(const struct RowSet *row_set, const char *column, int *is_null);

/**
 * Sets the value in `is_string` to 1 if the value at the specified column is a string, otherwise 0.
 */
int is_string_named(const struct RowSet *row_set, const char *column, int *is_string);

/**
 * Sets the value in `is_int` to 1 if the value at the specified column is an integer, otherwise 0.
 */
int is_int_named(const struct RowSet *row_set, const char *column, int *is_int);

/**
 * If the value at the specified column is a string, copy the value to the buffer, otherwise a type error is returned.
 * `STARDUST_DB_BUFFER_TOO_SMALL` is returned if the string buffer is too small.
 */
int get_string_named(const struct RowSet *row_set,
                     const char *column,
                     char *string_buffer,
                     uintptr_t buffer_len);

/**
 * If the value at the specified column is an integer, copy the value to the buffer, otherwise a type error is returned.
 */
int get_int_named(const struct RowSet *row_set,
                  const char *column,
                  IntegerStorage *int_buffer);

/**
 * Cast the value to a string and copy the value to the buffer. An error will be returned if the value is null.
 * `STARDUST_DB_BUFFER_TOO_SMALL` is returned if the string buffer is too small.
 */
int get_string_named_cast(const struct RowSet *row_set,
                          const char *column,
                          char *string_buffer,
                          uintptr_t buffer_len);

/**
 * Cast the value to an integer and copy the value to the buffer. An error will be returned if the value is null.
 */
int get_int_named_cast(const struct RowSet *row_set,
                       const char *column,
                       IntegerStorage *int_buffer);

#endif /* STARDUST_DB_H */
