use std::{
    ffi::CStr,
    ops::{Deref, DerefMut},
    os::raw::{c_char, c_int},
    ptr::null_mut,
};

use crate::{
    data_types::{IntegerStorage, TypeContents, Value},
    relation::Relation,
    temporary_database::TemporaryDatabase,
    Database,
};

pub const STARDUST_DB_OK: c_int = 0;
pub const STARDUST_DB_INVALID_PATH_UTF_8: c_int = 1;
pub const STARDUST_DB_INVALID_PATH_LOCATION: c_int = 2;
pub const STARDUST_DB_NULL_ROW_SET: c_int = 3;
pub const STARDUST_DB_NULL_DB: c_int = 4;
pub const STARDUST_DB_INVALID_QUERY_UTF_8: c_int = 5;
pub const STARDUST_DB_NO_RESULT: c_int = 6;
pub const STARDUST_DB_EXECUTION_ERROR: c_int = 7;
pub const STARDUST_DB_END: c_int = 8;
pub const STARDUST_DB_NO_COLUMN: c_int = 9;
pub const STARDUST_DB_BUFFER_TOO_SMALL: c_int = 10;
pub const STARDUST_DB_VALUE_WRONG_TYPE: c_int = 11;
pub const STARDUST_DB_VALUE_NULL: c_int = 12;
pub const STARDUST_DB_TEMP_DB_ERROR: c_int = 13;

pub const ROW_SET_INIT: RowSet = RowSet {
    relation: 0 as *mut Relation,
    current_row: 0,
};

enum DatabaseRef {
    Ordinary(&'static mut Database),
    Temporary(&'static mut TemporaryDatabase),
}

impl Deref for DatabaseRef {
    type Target = Database;

    fn deref(&self) -> &Self::Target {
        match self {
            DatabaseRef::Ordinary(database) => database,
            DatabaseRef::Temporary(database) => database,
        }
    }
}

impl DerefMut for DatabaseRef {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            DatabaseRef::Ordinary(database) => database,
            DatabaseRef::Temporary(database) => database,
        }
    }
}

#[repr(C)]
pub enum Db {
    Ordinary(*mut Database),
    Temporary(*mut TemporaryDatabase),
}

#[repr(C)]
pub struct RowSet {
    relation: *mut Relation,
    current_row: usize,
}

/// Opens the database at the specified path. Returns `STARDUST_DB_OK` on success.
#[no_mangle]
pub unsafe extern "C" fn open_database(path: *const c_char, db: *mut Db) -> c_int {
    let path = CStr::from_ptr(path);
    let path = result_to_error!(path.to_str(), STARDUST_DB_INVALID_PATH_UTF_8);
    let database = Box::new(result_to_error!(
        Database::open(path),
        STARDUST_DB_INVALID_PATH_LOCATION
    ));
    let database_ptr = Box::into_raw(database);
    *db = Db::Ordinary(database_ptr);
    STARDUST_DB_OK
}

/// Opens a temporary database. Returns `STARDUST_DB_OK` on success.
#[no_mangle]
pub unsafe extern "C" fn temp_db(db: *mut Db) -> c_int {
    let database = Box::new(result_to_error!(
        TemporaryDatabase::new(),
        STARDUST_DB_TEMP_DB_ERROR
    ));
    let database_ptr = Box::into_raw(database);
    *db = Db::Temporary(database_ptr);
    STARDUST_DB_OK
}

/// Closes the database. This function should always succeed.
#[no_mangle]
pub unsafe extern "C" fn close_db(db: *mut Db) {
    let database = option_to_error!(db.as_mut());
    match database {
        Db::Ordinary(database) => {
            let _ = Box::<Database>::from_raw(option_to_error!(database.as_mut()));
        }
        Db::Temporary(database) => {
            let _ = Box::<TemporaryDatabase>::from_raw(option_to_error!(database.as_mut()));
        }
    };
    *db = Db::Ordinary(null_mut())
}

/// Frees the memory from the `RowSet`.
#[no_mangle]
pub unsafe extern "C" fn close_row_set(row_set: *mut RowSet) {
    let row_set = option_to_error!(row_set.as_mut());
    if !row_set.relation.is_null() {
        let _ = Box::<Relation>::from_raw(row_set.relation);
    }
    *row_set = RowSet {
        current_row: 0,
        relation: null_mut(),
    }
}

unsafe fn set_row_set(row_set: *mut RowSet, relation: Relation) -> core::result::Result<(), c_int> {
    let row_set = row_set.as_mut().ok_or(STARDUST_DB_NULL_ROW_SET)?;
    row_set.current_row = 0;
    if row_set.relation.is_null() {
        row_set.relation = Box::into_raw(Box::new(relation));
    } else {
        let mut existing = Box::from_raw(row_set.relation);
        let _ = std::mem::replace(existing.as_mut(), relation);
    }
    Ok(())
}

unsafe fn get_database(db: *mut Db) -> core::result::Result<DatabaseRef, c_int> {
    let database = db.as_mut().ok_or(STARDUST_DB_NULL_DB)?;
    let database = match database {
        Db::Ordinary(database) => {
            DatabaseRef::Ordinary(database.as_mut().ok_or(STARDUST_DB_NULL_DB)?)
        }
        Db::Temporary(database) => {
            DatabaseRef::Temporary(database.as_mut().ok_or(STARDUST_DB_NULL_DB)?)
        }
    };
    Ok(database)
}

unsafe fn get_relation_and_verify_row(
    row_set: *const RowSet,
) -> core::result::Result<(&'static Relation, usize), c_int> {
    let row_set = row_set.as_ref().ok_or(STARDUST_DB_NULL_ROW_SET)?;
    let relation = row_set.relation.as_ref().ok_or(STARDUST_DB_NULL_ROW_SET)?;
    if row_set.current_row >= relation.num_rows() {
        return Err(STARDUST_DB_END);
    }
    Ok((relation, row_set.current_row))
}

/// Executes the query in `query` and places the result in `row_set`.
/// Errors will be placed in the buffer at `err_buf`, which must be no smaller than `err_buff_len`.
#[no_mangle]
pub unsafe extern "C" fn execute_query(
    db: *mut Db,
    query: *const c_char,
    row_set: *mut RowSet,
    err_buff: *mut c_char,
    err_buff_len: usize,
) -> c_int {
    let database = result_to_error!(get_database(db));
    let query = CStr::from_ptr(query);
    let query = result_to_error!(query.to_str(), STARDUST_DB_INVALID_QUERY_UTF_8);
    let result = database.execute_query(query);
    match result {
        Ok(mut relations) => match relations.pop() {
            Some(result) => result_to_error!(set_row_set(row_set, result)),
            None => return STARDUST_DB_NO_RESULT,
        },
        Err(e) => {
            let err_str = e.to_string();
            return result_to_error!(fill_buffer(&err_str, err_buff, err_buff_len, true, STARDUST_DB_EXECUTION_ERROR))
        }
    }
    STARDUST_DB_OK
}

/// Move to the next row in the `RowSet`. Returns `STARDUST_DB_END` if the row is past the end of the `RowSet`.
#[no_mangle]
pub unsafe extern "C" fn next_row(row_set: *mut RowSet) -> c_int {
    let row_set = option_to_error!(row_set.as_mut(), STARDUST_DB_NULL_ROW_SET);
    let relation = option_to_error!(row_set.relation.as_ref(), STARDUST_DB_NULL_ROW_SET);
    (*row_set).current_row += 1;
    if (*row_set).current_row > relation.num_rows() {
        return STARDUST_DB_END;
    }
    STARDUST_DB_OK
}

/// Set the current row of the `RowSet` to the specified value. Returns `STARDUST_DB_END` if the row is past the end of the `RowSet`.
#[no_mangle]
pub unsafe extern "C" fn set_row(row_set: *mut RowSet, row: usize) -> c_int {
    let row_set = option_to_error!(row_set.as_mut(), STARDUST_DB_NULL_ROW_SET);
    let relation = option_to_error!(row_set.relation.as_ref(), STARDUST_DB_NULL_ROW_SET);
    if row > relation.num_rows() {
        return STARDUST_DB_END;
    }
    (*row_set).current_row = row;
    STARDUST_DB_OK
}

/// Sets the value in `is_end` to 1 if the current row is past the end of the `RowSet`, otherwise the value is set to 0.
#[no_mangle]
pub unsafe extern "C" fn is_end(row_set: *const RowSet, is_end: *mut c_int) -> c_int {
    let row_set = option_to_error!(row_set.as_ref(), STARDUST_DB_NULL_ROW_SET);
    let current_row = row_set.current_row;
    let relation = option_to_error!(row_set.relation.as_ref(), STARDUST_DB_NULL_ROW_SET);
    *is_end = (current_row > relation.num_rows()) as c_int;
    STARDUST_DB_OK
}

/// Sets the value in `num_columns` to be the number of columns in the `RowSet`.
#[no_mangle]
pub unsafe extern "C" fn num_columns(row_set: *const RowSet, num_columns: *mut usize) -> c_int {
    let row_set = option_to_error!(row_set.as_ref(), STARDUST_DB_NULL_ROW_SET);
    let relation = option_to_error!(row_set.relation.as_ref(), STARDUST_DB_NULL_ROW_SET);
    *num_columns = relation.num_columns() as usize;
    STARDUST_DB_OK
}

/// Sets the value in `num_rows` to be the number of rows in the `RowSet`.
#[no_mangle]
pub unsafe extern "C" fn num_rows(row_set: *const RowSet, num_rows: *mut usize) -> c_int {
    let row_set = option_to_error!(row_set.as_ref(), STARDUST_DB_NULL_ROW_SET);
    let relation = option_to_error!(row_set.relation.as_ref(), STARDUST_DB_NULL_ROW_SET);
    *num_rows = relation.num_rows() as usize;
    STARDUST_DB_OK
}

unsafe fn get_value_index(
    row_set: *const RowSet,
    column: usize,
) -> core::result::Result<&'static Value, c_int> {
    let (relation, row) = get_relation_and_verify_row(row_set)?;
    if column > relation.num_columns() {
        return Err(STARDUST_DB_NO_COLUMN);
    }
    Ok(relation.get_value(column, row))
}

/// Sets the value in `is_null` to 1 if the value at the specified column is Null, otherwise 0.
#[no_mangle]
pub unsafe extern "C" fn is_null_index(
    row_set: *const RowSet,
    column: usize,
    is_null: *mut c_int,
) -> c_int {
    let value = result_to_error!(get_value_index(row_set, column));
    *is_null = matches!(value, Value::Null) as c_int;
    STARDUST_DB_OK
}

/// Sets the value in `is_string` to 1 if the value at the specified column is a string, otherwise 0.
#[no_mangle]
pub unsafe extern "C" fn is_string_index(
    row_set: *const RowSet,
    column: usize,
    is_string: *mut c_int,
) -> c_int {
    let value = result_to_error!(get_value_index(row_set, column));
    *is_string = matches!(value, Value::TypedValue(TypeContents::String(_))) as c_int;
    STARDUST_DB_OK
}

/// Sets the value in `is_int` to 1 if the value at the specified column is an integer, otherwise 0.
#[no_mangle]
pub unsafe extern "C" fn is_int_index(
    row_set: *const RowSet,
    column: usize,
    is_int: *mut c_int,
) -> c_int {
    let value = result_to_error!(get_value_index(row_set, column));
    *is_int = matches!(value, Value::TypedValue(TypeContents::Integer(_))) as c_int;
    STARDUST_DB_OK
}

unsafe fn get_string(value: &Value, string_buffer: *mut c_char, buffer_len: usize) -> c_int {
    match value {
        Value::TypedValue(TypeContents::String(string)) => {
            result_to_error!(fill_buffer(string, string_buffer, buffer_len, false, STARDUST_DB_OK))
        }
        Value::Null => STARDUST_DB_VALUE_NULL,
        _ => STARDUST_DB_VALUE_WRONG_TYPE,
    }
}

unsafe fn fill_buffer<T>(
    string: &str,
    mut string_buffer: *mut c_char,
    buffer_len: usize,
    truncate: bool,
    ok_value: T
) -> Result<T, c_int> {
    for byte in string.bytes().take(buffer_len - 1) {
        *string_buffer = byte as c_char;
        string_buffer = string_buffer.add(1);
    }
    *string_buffer = 0;
    if !truncate && string.len() >= buffer_len {
        Err(STARDUST_DB_BUFFER_TOO_SMALL)
    } else {
        Ok(ok_value)
    }
}

unsafe fn as_int(value: &Value, int_buffer: *mut IntegerStorage) -> c_int {
    match value {
        Value::TypedValue(TypeContents::Integer(i)) => {
            *int_buffer = *i;
            STARDUST_DB_OK
        }
        Value::Null => STARDUST_DB_VALUE_NULL,
        _ => STARDUST_DB_VALUE_WRONG_TYPE,
    }
}

/// If the value at the specified column is a string, copy the value to the buffer, otherwise a type error is returned.
/// `STARDUST_DB_BUFFER_TOO_SMALL` is returned if the string buffer is too small.
#[no_mangle]
pub unsafe extern "C" fn get_string_index(
    row_set: *const RowSet,
    column: usize,
    string_buffer: *mut c_char,
    buffer_len: usize,
) -> c_int {
    let value = result_to_error!(get_value_index(row_set, column));
    get_string(value, string_buffer, buffer_len);
    STARDUST_DB_OK
}

/// If the value at the specified column is an integer, copy the value to the buffer, otherwise a type error is returned.
#[no_mangle]
pub unsafe extern "C" fn get_int_index(
    row_set: *const RowSet,
    column: usize,
    int_buffer: *mut IntegerStorage,
) -> c_int {
    let value = result_to_error!(get_value_index(row_set, column));
    as_int(value, int_buffer)
}

/// Cast the value to a string and copy the value to the buffer. An error will be returned if the value is null.
/// `STARDUST_DB_BUFFER_TOO_SMALL` is returned if the string buffer is too small.
#[no_mangle]
pub unsafe extern "C" fn get_string_index_cast(
    row_set: *const RowSet,
    column: usize,
    string_buffer: *mut c_char,
    buffer_len: usize,
) -> c_int {
    let value = result_to_error!(get_value_index(row_set, column));
    if let Some(string) = value.cast_string() {
        result_to_error!(fill_buffer(&string, string_buffer, buffer_len, false, STARDUST_DB_OK))
    } else {
        STARDUST_DB_VALUE_NULL
    }
}

/// Cast the value to an integer and copy the value to the buffer. An error will be returned if the value is null.
#[no_mangle]
pub unsafe extern "C" fn get_int_index_cast(
    row_set: *const RowSet,
    column: usize,
    int_buffer: *mut IntegerStorage,
) -> c_int {
    let value = result_to_error!(get_value_index(row_set, column));
    if let Some(integer) = value.cast_int() {
        *int_buffer = integer;
        STARDUST_DB_OK
    } else {
        STARDUST_DB_VALUE_NULL
    }
}

unsafe fn get_value_named(
    row_set: *const RowSet,
    column: *const c_char,
) -> core::result::Result<&'static Value, c_int> {
    let (relation, row) = get_relation_and_verify_row(row_set)?;
    let column = CStr::from_ptr(column);
    let column = column.to_str().map_err(|_| STARDUST_DB_NO_COLUMN)?;
    relation
        .get_value_named(column, row)
        .ok_or(STARDUST_DB_NO_COLUMN)
}

/// Sets the value in `is_null` to 1 if the value at the specified column is null, otherwise 0.
#[no_mangle]
pub unsafe extern "C" fn is_null_named(
    row_set: *const RowSet,
    column: *const c_char,
    is_null: *mut c_int,
) -> c_int {
    let value = result_to_error!(get_value_named(row_set, column));
    *is_null = matches!(value, Value::Null) as c_int;
    STARDUST_DB_OK
}

/// Sets the value in `is_string` to 1 if the value at the specified column is a string, otherwise 0.
#[no_mangle]
pub unsafe extern "C" fn is_string_named(
    row_set: *const RowSet,
    column: *const c_char,
    is_string: *mut c_int,
) -> c_int {
    let value = result_to_error!(get_value_named(row_set, column));
    *is_string = matches!(value, Value::TypedValue(TypeContents::String(_))) as c_int;
    STARDUST_DB_OK
}

/// Sets the value in `is_int` to 1 if the value at the specified column is an integer, otherwise 0.
#[no_mangle]
pub unsafe extern "C" fn is_int_named(
    row_set: *const RowSet,
    column: *const c_char,
    is_int: *mut c_int,
) -> c_int {
    let value = result_to_error!(get_value_named(row_set, column));
    *is_int = matches!(value, Value::TypedValue(TypeContents::Integer(_))) as c_int;
    STARDUST_DB_OK
}

/// If the value at the specified column is a string, copy the value to the buffer, otherwise a type error is returned.
/// `STARDUST_DB_BUFFER_TOO_SMALL` is returned if the string buffer is too small.
#[no_mangle]
pub unsafe extern "C" fn get_string_named(
    row_set: *const RowSet,
    column: *const c_char,
    string_buffer: *mut c_char,
    buffer_len: usize,
) -> c_int {
    let value = result_to_error!(get_value_named(row_set, column));
    get_string(value, string_buffer, buffer_len);
    STARDUST_DB_OK
}

/// If the value at the specified column is an integer, copy the value to the buffer, otherwise a type error is returned.
#[no_mangle]
pub unsafe extern "C" fn get_int_named(
    row_set: *const RowSet,
    column: *const c_char,
    int_buffer: *mut IntegerStorage,
) -> c_int {
    let value = result_to_error!(get_value_named(row_set, column));
    as_int(value, int_buffer)
}

/// Cast the value to a string and copy the value to the buffer. An error will be returned if the value is null.
/// `STARDUST_DB_BUFFER_TOO_SMALL` is returned if the string buffer is too small.
#[no_mangle]
pub unsafe extern "C" fn get_string_named_cast(
    row_set: *const RowSet,
    column: *const c_char,
    string_buffer: *mut c_char,
    buffer_len: usize,
) -> c_int {
    let value = result_to_error!(get_value_named(row_set, column));
    if let Some(string) = value.cast_string() {
        result_to_error!(fill_buffer(&string, string_buffer, buffer_len, false, STARDUST_DB_OK))
    } else {
        STARDUST_DB_VALUE_NULL
    }
}
/// Cast the value to an integer and copy the value to the buffer. An error will be returned if the value is null.
#[no_mangle]
pub unsafe extern "C" fn get_int_named_cast(
    row_set: *const RowSet,
    column: *const c_char,
    int_buffer: *mut IntegerStorage,
) -> c_int {
    let value = result_to_error!(get_value_named(row_set, column));
    if let Some(integer) = value.cast_int() {
        *int_buffer = integer;
        STARDUST_DB_OK
    } else {
        STARDUST_DB_VALUE_NULL
    }
}
