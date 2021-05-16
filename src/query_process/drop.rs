use sqlparser::ast::{ObjectName, ObjectType};

use crate::ast::DropTable;

pub fn parse_drop(
    object_type: ObjectType,
    if_exists: bool,
    names: Vec<ObjectName>,
    _cascade: bool,
    _purge: bool,
) -> DropTable {
    match object_type {
        ObjectType::Table => {
            let names = names.into_iter().map(|name| name.to_string()).collect();
            DropTable::new(if_exists, names)
        }
        _ => unimplemented!("{:?}", object_type),
    }
}
