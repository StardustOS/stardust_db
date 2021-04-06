use crate::data_types::Value;
use crate::error::{Error, ExecutionError};
use crate::temporary_database::temp_db;
use std::collections::HashSet;

#[test]
fn create_table() {
    let db = temp_db();
    let result = db
        .execute_query("CREATE TABLE test (name string);")
        .unwrap();
    assert_eq!(result.len(), 1);
    assert!(result[0].is_empty())
}

#[test]
fn select_empty_table() {
    let db = temp_db();
    let _ = db
        .execute_query("CREATE TABLE test (name string);")
        .unwrap();
    let result = db.execute_query("SELECT * FROM test;").unwrap();
    assert_eq!(result.len(), 1);
    result[0].assert_equals(set![], vec!["name"])
}

#[test]
fn insert_simple_string() {
    let db = temp_db();
    let _ = db
        .execute_query("CREATE TABLE test (name string);")
        .unwrap();
    let result = db
        .execute_query("INSERT INTO test VALUES ('User');")
        .unwrap();
    assert_eq!(result.len(), 1);
    assert!(result[0].is_empty())
}

#[test]
fn select_simple_string() {
    let db = temp_db();
    let _ = db
        .execute_query("CREATE TABLE test (name string);")
        .unwrap();
    let _ = db
        .execute_query("INSERT INTO test VALUES ('User');")
        .unwrap();
    let result = db.execute_query("SELECT * FROM test;").unwrap();
    assert_eq!(result.len(), 1);
    result[0].assert_equals(set![vec![Value::from("User")]], vec!["name"])
}

#[test]
fn select_column_string() {
    let db = temp_db();
    let _ = db
        .execute_query("CREATE TABLE test (name string, age int);")
        .unwrap();
    let _ = db
        .execute_query("INSERT INTO test VALUES ('User', 25);")
        .unwrap();
    let result = db.execute_query("SELECT name FROM test;").unwrap();
    assert_eq!(result.len(), 1);
    result[0].assert_equals(set![vec![Value::from("User")]], vec!["name"])
}

#[test]
fn insert_simple_int() {
    let db = temp_db();
    let _ = db.execute_query("CREATE TABLE test (age int);").unwrap();
    let result = db.execute_query("INSERT INTO test VALUES (25);").unwrap();
    assert_eq!(result.len(), 1);
    assert!(result[0].is_empty())
}

#[test]
fn select_simple_int() {
    let db = temp_db();
    let _ = db.execute_query("CREATE TABLE test (age int);").unwrap();
    let _ = db.execute_query("INSERT INTO test VALUES (25);").unwrap();
    let result = db.execute_query("SELECT * FROM test;").unwrap();
    assert_eq!(result.len(), 1);
    result[0].assert_equals(set![vec![Value::from(25)]], vec!["age"])
}

#[test]
fn select_column_int() {
    let db = temp_db();
    let _ = db
        .execute_query("CREATE TABLE test (name string, age int);")
        .unwrap();
    let _ = db
        .execute_query("INSERT INTO test VALUES ('User', 25);")
        .unwrap();
    let result = db.execute_query("SELECT age FROM test;").unwrap();
    assert_eq!(result.len(), 1);
    result[0].assert_equals(set![vec![Value::from(25)]], vec!["age"])
}

#[test]
fn select_multiple_columns() {
    let db = temp_db();
    let _ = db
        .execute_query("CREATE TABLE test (name string, age int);")
        .unwrap();
    let _ = db
        .execute_query("INSERT INTO test VALUES ('User', 25);")
        .unwrap();
    let result = db.execute_query("SELECT name, age FROM test;").unwrap();
    assert_eq!(result.len(), 1);
    result[0].assert_equals(
        set![vec![Value::from("User"), Value::from(25)]],
        vec!["name", "age"],
    )
}

#[test]
fn select_multiple_columns_wildcard() {
    let db = temp_db();
    let _ = db
        .execute_query("CREATE TABLE test (name string, age int);")
        .unwrap();
    let _ = db
        .execute_query("INSERT INTO test VALUES ('User', 25);")
        .unwrap();
    let result = db.execute_query("SELECT * FROM test;").unwrap();
    assert_eq!(result.len(), 1);
    result[0].assert_equals(
        set![vec![Value::from("User"), Value::from(25)]],
        vec!["name", "age"],
    )
}

#[test]
fn select_column_and_wildcard() {
    let db = temp_db();
    let _ = db
        .execute_query("CREATE TABLE test (name string, age int);")
        .unwrap();
    let _ = db
        .execute_query("INSERT INTO test VALUES ('User', 25);")
        .unwrap();
    let result = db.execute_query("SELECT name, * FROM test;").unwrap();
    assert_eq!(result.len(), 1);
    result[0].assert_equals(
        set![vec![
            Value::from("User"),
            Value::from("User"),
            Value::from(25)
        ]],
        vec!["name", "name", "age"],
    )
}

#[test]
fn insert_multiple_values_same_statement() {
    let db = temp_db();
    let _ = db
        .execute_query("CREATE TABLE test (name string, age int);")
        .unwrap();
    let result = db
        .execute_query("INSERT INTO test VALUES ('User', 25), ('User 2', 17);")
        .unwrap();
    assert_eq!(result.len(), 1);
    assert!(result[0].is_empty())
}

#[test]
fn insert_multiple_values_different_statements() {
    let db = temp_db();
    let _ = db
        .execute_query("CREATE TABLE test (name string, age int);")
        .unwrap();
    let result = db
        .execute_query("INSERT INTO test VALUES ('User', 25);")
        .unwrap();
    assert_eq!(result.len(), 1);
    assert!(result[0].is_empty());
    let result = db
        .execute_query("INSERT INTO test VALUES ('User 2', 17);")
        .unwrap();
    assert_eq!(result.len(), 1);
    assert!(result[0].is_empty())
}

#[test]
fn select_multiple_values_same_statement() {
    let db = temp_db();
    let _ = db
        .execute_query("CREATE TABLE test (name string, age int);")
        .unwrap();
    let _ = db
        .execute_query("INSERT INTO test VALUES ('User', 25), ('User 2', 17);")
        .unwrap();
    let result = db.execute_query("SELECT * FROM test;").unwrap();
    assert_eq!(result.len(), 1);
    result[0].assert_equals(
        set![
            vec![Value::from("User"), Value::from(25)],
            vec![Value::from("User 2"), Value::from(17)]
        ],
        vec!["name", "age"],
    )
}

#[test]
fn select_multiple_values_different_statements() {
    let db = temp_db();
    let _ = db
        .execute_query("CREATE TABLE test (name string, age int);")
        .unwrap();
    let _ = db
        .execute_query("INSERT INTO test VALUES ('User', 25);")
        .unwrap();
    let _ = db
        .execute_query("INSERT INTO test VALUES ('User 2', 17);")
        .unwrap();
    let result = db.execute_query("SELECT * FROM test;").unwrap();
    assert_eq!(result.len(), 1);
    result[0].assert_equals(
        set![
            vec![Value::from("User"), Value::from(25)],
            vec![Value::from("User 2"), Value::from(17)]
        ],
        vec!["name", "age"],
    )
}

#[test]
fn select_columns_alias() {
    let db = temp_db();
    let _ = db
        .execute_query("CREATE TABLE test (name string, age int);")
        .unwrap();
    let _ = db
        .execute_query("INSERT INTO test VALUES ('User', 25);")
        .unwrap();
    let result = db
        .execute_query("SELECT name AS eman, age AS ega FROM test;")
        .unwrap();
    assert_eq!(result.len(), 1);
    result[0].assert_equals(
        set![vec![Value::from("User"), Value::from(25)]],
        vec!["eman", "ega"],
    )
}

#[test]
fn select_expression() {
    let db = temp_db();
    let result = db.execute_query("SELECT 1 < 2;").unwrap();
    assert_eq!(result.len(), 1);
    result[0].assert_equals(set![vec![Value::from(1)]], vec!["1 < 2"])
}

#[test]
fn select_expression_alias() {
    let db = temp_db();
    let result = db.execute_query("SELECT 1 < 2 AS sum;").unwrap();
    assert_eq!(result.len(), 1);
    result[0].assert_equals(set![vec![Value::from(1)]], vec!["sum"])
}

#[test]
fn select_expression_from_table() {
    let db = temp_db();
    let _ = db
        .execute_query("CREATE TABLE test (name string, age int);")
        .unwrap();
    let _ = db
        .execute_query("INSERT INTO test VALUES ('User', 25);")
        .unwrap();
    let result = db.execute_query("SELECT 1 < 2 FROM test;").unwrap();
    assert_eq!(result.len(), 1);
    result[0].assert_equals(set![vec![Value::from(1)]], vec!["1 < 2"])
}

#[test]
fn select_expression_from_empty_table() {
    let db = temp_db();
    let _ = db
        .execute_query("CREATE TABLE test (name string, age int);")
        .unwrap();
    let result = db.execute_query("SELECT 1 < 2 FROM test;").unwrap();
    assert_eq!(result.len(), 1);
    result[0].assert_equals(set![], vec!["1 < 2"])
}

#[test]
fn insert_specific_values() {
    let db = temp_db();
    let _ = db
        .execute_query("CREATE TABLE test (name string, age int);")
        .unwrap();
    let _ = db
        .execute_query("INSERT INTO test (name, age) VALUES ('User', 25);")
        .unwrap();
    let result = db.execute_query("SELECT * FROM test;").unwrap();
    assert_eq!(result.len(), 1);
    result[0].assert_equals(
        set![vec![Value::from("User"), Value::from(25)]],
        vec!["name", "age"],
    )
}

#[test]
fn insert_default_values() {
    let db = temp_db();
    let _ = db
        .execute_query("CREATE TABLE test (name string DEFAULT 'User', age int);")
        .unwrap();
    let _ = db
        .execute_query("INSERT INTO test (age) VALUES (25);")
        .unwrap();
    let result = db.execute_query("SELECT * FROM test;").unwrap();
    assert_eq!(result.len(), 1);
    result[0].assert_equals(
        set![vec![Value::from("User"), Value::from(25)]],
        vec!["name", "age"],
    )
}

#[test]
fn select_multiple_tables() {
    let db = temp_db();
    let _ = db
        .execute_query(
            "CREATE TABLE people (name string, age int);
        INSERT INTO people VALUES ('Josh', 23), ('Rupert', 25);
        CREATE TABLE hobbies (name string, hobby string);
        INSERT INTO hobbies VALUES ('Josh', 'Music'), ('Hugh', 'Swimming');",
        )
        .unwrap();
    let result = db.execute_query("SELECT * FROM people, hobbies;").unwrap();
    assert_eq!(result.len(), 1);
    result[0].assert_equals(
        set![
            vec![
                Value::from("Josh"),
                Value::from(23),
                Value::from("Josh"),
                Value::from("Music")
            ],
            vec![
                Value::from("Josh"),
                Value::from(23),
                Value::from("Hugh"),
                Value::from("Swimming")
            ],
            vec![
                Value::from("Rupert"),
                Value::from(25),
                Value::from("Josh"),
                Value::from("Music")
            ],
            vec![
                Value::from("Rupert"),
                Value::from(25),
                Value::from("Hugh"),
                Value::from("Swimming")
            ],
        ],
        vec!["name", "age", "name", "hobby"],
    )
}

#[test]
fn select_inner_join() {
    let db = temp_db();
    let _ = db
        .execute_query(
            "CREATE TABLE people (name string, age int);
        INSERT INTO people VALUES ('Josh', 23), ('Rupert', 25);
        CREATE TABLE hobbies (name string, hobby string);
        INSERT INTO hobbies VALUES ('Josh', 'Music'), ('Hugh', 'Swimming');",
        )
        .unwrap();
    let result = db
        .execute_query("SELECT * FROM people INNER JOIN hobbies;")
        .unwrap();
    assert_eq!(result.len(), 1);
    result[0].assert_equals(
        set![
            vec![
                Value::from("Josh"),
                Value::from(23),
                Value::from("Josh"),
                Value::from("Music")
            ],
            vec![
                Value::from("Josh"),
                Value::from(23),
                Value::from("Hugh"),
                Value::from("Swimming")
            ],
            vec![
                Value::from("Rupert"),
                Value::from(25),
                Value::from("Josh"),
                Value::from("Music")
            ],
            vec![
                Value::from("Rupert"),
                Value::from(25),
                Value::from("Hugh"),
                Value::from("Swimming")
            ],
        ],
        vec!["name", "age", "name", "hobby"],
    )
}

#[test]
fn select_inner_join_on() {
    let db = temp_db();
    let _ = db
        .execute_query(
            "CREATE TABLE people (name string, age int);
        INSERT INTO people VALUES ('Josh', 23), ('Rupert', 25), ('Hugh', 43);
        CREATE TABLE hobbies (name string, hobby string);
        INSERT INTO hobbies VALUES ('Josh', 'Music'), ('Hugh', 'Swimming'), ('Mike', 'Painting');",
        )
        .unwrap();
    let result = db
        .execute_query("SELECT * FROM people INNER JOIN hobbies ON people.name = hobbies.name;")
        .unwrap();
    assert_eq!(result.len(), 1);
    result[0].assert_equals(
        set![
            vec![
                Value::from("Josh"),
                Value::from(23),
                Value::from("Josh"),
                Value::from("Music")
            ],
            vec![
                Value::from("Hugh"),
                Value::from(43),
                Value::from("Hugh"),
                Value::from("Swimming")
            ],
        ],
        vec!["name", "age", "name", "hobby"],
    )
}

#[test]
fn select_left_join_on() {
    let db = temp_db();
    let _ = db
        .execute_query(
            "CREATE TABLE people (name string, age int);
        INSERT INTO people VALUES ('Josh', 23), ('Rupert', 25), ('Hugh', 43);
        CREATE TABLE hobbies (name string, hobby string);
        INSERT INTO hobbies VALUES ('Josh', 'Music'), ('Hugh', 'Swimming'), ('Mike', 'Painting');",
        )
        .unwrap();
    let result = db
        .execute_query("SELECT * FROM people LEFT JOIN hobbies ON people.name = hobbies.name;")
        .unwrap();
    assert_eq!(result.len(), 1);
    result[0].assert_equals(
        set![
            vec![
                Value::from("Josh"),
                Value::from(23),
                Value::from("Josh"),
                Value::from("Music")
            ],
            vec![
                Value::from("Hugh"),
                Value::from(43),
                Value::from("Hugh"),
                Value::from("Swimming")
            ],
            vec![
                Value::from("Rupert"),
                Value::from(25),
                Value::Null,
                Value::Null
            ],
        ],
        vec!["name", "age", "name", "hobby"],
    )
}

#[test]
fn select_right_join_on() {
    let db = temp_db();
    let _ = db
        .execute_query(
            "CREATE TABLE people (name string, age int);
        INSERT INTO people VALUES ('Josh', 23), ('Rupert', 25), ('Hugh', 43);
        CREATE TABLE hobbies (name string, hobby string);
        INSERT INTO hobbies VALUES ('Josh', 'Music'), ('Hugh', 'Swimming'), ('Mike', 'Painting');",
        )
        .unwrap();
    let result = db
        .execute_query("SELECT * FROM people RIGHT JOIN hobbies ON people.name = hobbies.name;")
        .unwrap();
    assert_eq!(result.len(), 1);
    result[0].assert_equals(
        set![
            vec![
                Value::from("Josh"),
                Value::from(23),
                Value::from("Josh"),
                Value::from("Music")
            ],
            vec![
                Value::from("Hugh"),
                Value::from(43),
                Value::from("Hugh"),
                Value::from("Swimming")
            ],
            vec![
                Value::Null,
                Value::Null,
                Value::from("Mike"),
                Value::from("Painting"),
            ],
        ],
        vec!["name", "age", "name", "hobby"],
    )
}

#[test]
fn create_duplicate_table() {
    let db = temp_db();
    let _ = db
        .execute_query("CREATE TABLE test (name string, age int);")
        .unwrap();
    let result = db.execute_query("CREATE TABLE test (name string, age int);");
    assert!(
        matches!(result, Err(Error::Execution(ExecutionError::TableExists(err))) if err == "test")
    )
}

#[test]
fn create_duplicate_column() {
    let db = temp_db();
    let result = db.execute_query("CREATE TABLE test (name string, name int);");
    assert!(
        matches!(result, Err(Error::Execution(ExecutionError::ColumnExists(err))) if err == "name")
    )
}

#[test]
fn select_nonexistant_table() {
    let db = temp_db();
    let result = db.execute_query("SELECT * FROM test;");
    assert!(matches!(result, Err(Error::Execution(ExecutionError::NoTable(err))) if err == "test"))
}

#[test]
fn insert_too_few_columns() {
    let db = temp_db();
    let _ = db
        .execute_query("CREATE TABLE test (name string, age int);")
        .unwrap();
    let result = db.execute_query("INSERT INTO test VALUES ('User');");
    assert!(
        matches!(result, Err(Error::Execution(ExecutionError::WrongNumColumns{ expected, actual })) if expected == 2 && actual == 1)
    )
}

#[test]
fn insert_too_many_columns() {
    let db = temp_db();
    let _ = db
        .execute_query("CREATE TABLE test (name string, age int);")
        .unwrap();
    let result = db.execute_query("INSERT INTO test VALUES ('User', 25, 'Swimming');");
    assert!(
        matches!(result, Err(Error::Execution(ExecutionError::WrongNumColumns{ expected, actual })) if expected == 2 && actual == 3)
    )
}

#[test]
fn select_nonexistant_column() {
    let db = temp_db();
    let _ = db
        .execute_query("CREATE TABLE test (name string, age int);")
        .unwrap();
    let result = db.execute_query("SELECT invalid FROM test;");
    assert!(
        matches!(result, Err(Error::Execution(ExecutionError::NoColumn(err))) if err == "invalid")
    )
}

#[test]
fn select_ambiguous_column() {
    let db = temp_db();
    let _ = db
        .execute_query("CREATE TABLE test (name string, age int);")
        .unwrap();
    let _ = db
        .execute_query("INSERT INTO test VALUES ('User', 25);")
        .unwrap();
    let result = db.execute_query("SELECT name FROM test, test as test2;");
    assert!(
        matches!(result, Err(Error::Execution(ExecutionError::AmbiguousName(err))) if err == "name")
    )
}

#[test]
fn insert_not_null() {
    let db = temp_db();
    let _ = db
        .execute_query("CREATE TABLE test (name string NOT NULL);")
        .unwrap();
    let _ = db
        .execute_query("INSERT INTO test VALUES ('User');")
        .unwrap();
    let result = db.execute_query("SELECT * FROM test;").unwrap();
    assert_eq!(result.len(), 1);
    result[0].assert_equals(set![vec![Value::from("User")]], vec!["name"])
}

#[test]
fn insert_null_to_not_null() {
    let db = temp_db();
    let _ = db
        .execute_query("CREATE TABLE test (name string NOT NULL);")
        .unwrap();
    let result = db.execute_query("INSERT INTO test VALUES (NULL);");
    assert!(
        matches!(result, Err(Error::Execution(ExecutionError::NullConstraintFailed(err))) if err == "name")
    )
}

#[test]
fn insert_unique() {
    let db = temp_db();
    let _ = db
        .execute_query("CREATE TABLE test (name string UNIQUE);")
        .unwrap();
    let _ = db
        .execute_query("INSERT INTO test VALUES ('User');")
        .unwrap();
    let result = db.execute_query("SELECT * FROM test;").unwrap();
    assert_eq!(result.len(), 1);
    result[0].assert_equals(set![vec![Value::from("User")]], vec!["name"])
}

#[test]
fn insert_duplicate_to_unique() {
    let db = temp_db();
    let _ = db
        .execute_query("CREATE TABLE test (name string UNIQUE);")
        .unwrap();
    let _ = db
        .execute_query("INSERT INTO test VALUES ('User');")
        .unwrap();
    let result = db.execute_query("INSERT INTO test VALUES ('User');");
    assert!(
        matches!(result, Err(Error::Execution(ExecutionError::UniqueConstraintFailed(err))) if err == "name")
    )
}

#[test]
fn insert_duplicate_to_unique_separate_constraint() {
    let db = temp_db();
    let _ = db
        .execute_query("CREATE TABLE test (name string, UNIQUE(name));")
        .unwrap();
    let _ = db
        .execute_query("INSERT INTO test VALUES ('User');")
        .unwrap();
    let result = db.execute_query("INSERT INTO test VALUES ('User');");
    assert!(
        matches!(result, Err(Error::Execution(ExecutionError::UniqueConstraintFailed(err))) if err == "name")
    )
}

#[test]
fn insert_duplicate_to_unique_separate_constraint_multiple_values() {
    let db = temp_db();
    let _ = db
        .execute_query("CREATE TABLE test (name string, age int, UNIQUE(name, age));")
        .unwrap();
    let _ = db
        .execute_query("INSERT INTO test VALUES ('User', 25);")
        .unwrap();
    let _ = db
        .execute_query("INSERT INTO test VALUES ('User', 23);")
        .unwrap();
    let _ = db
        .execute_query("INSERT INTO test VALUES ('User2', 25);")
        .unwrap();
    let result = db.execute_query("INSERT INTO test VALUES ('User', 25);");
    assert!(
        matches!(result, Err(Error::Execution(ExecutionError::UniqueConstraintFailed(err))) if err == "name, age")
    )
}

#[test]
fn insert_duplicate_to_unique_named_constraint_multiple_values() {
    let db = temp_db();
    let _ = db
        .execute_query(
            "CREATE TABLE test (name string, age int, CONSTRAINT constraint UNIQUE(name, age));",
        )
        .unwrap();
    let _ = db
        .execute_query("INSERT INTO test VALUES ('User', 25);")
        .unwrap();
    let _ = db
        .execute_query("INSERT INTO test VALUES ('User', 23);")
        .unwrap();
    let _ = db
        .execute_query("INSERT INTO test VALUES ('User2', 25);")
        .unwrap();
    let result = db.execute_query("INSERT INTO test VALUES ('User', 25);");
    assert!(
        matches!(result, Err(Error::Execution(ExecutionError::UniqueConstraintFailed(err))) if err == "constraint")
    )
}