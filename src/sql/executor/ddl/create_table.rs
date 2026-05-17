use crate::catalog::schema::{Column, Schema};
use crate::sql::executor::{ExecutionResult, Executor};
use std::io;

pub fn execute_create(
    executor: &mut Executor,
    table_name: String,
    columns: Vec<Column>,
) -> io::Result<ExecutionResult> {
    let schema = Schema::new(&table_name, columns)?;
    executor.database.create_table(schema)?;

    Ok(ExecutionResult::Success {
        message: format!("Table '{}' created.", table_name),
    })
}

#[cfg(test)]
mod test {
    use crate::{
        catalog::schema::DataType,
        sql::{executor::test_helpers::*, parser::Statement},
    };

    use super::*;

    #[test]
    fn test_execute_create_table() {
        cleanup("test_exec_create");

        let mut executor = create_test_executor("test_exec_create");

        let columns = vec![
            Column::new("id", DataType::Integer, true),
            Column::new("name", DataType::Text, false),
        ];

        let statement = Statement::CreateTable {
            name: "users".to_string(),
            columns,
        };

        let result = executor.execute(statement, &mut None).unwrap();

        match result {
            ExecutionResult::Success { message } => {
                assert!(message.contains("users"));
                assert!(message.contains("created"));
            }
            _ => panic!("Expected Success result"),
        }

        cleanup("test_exec_create");
    }

    #[test]
    fn test_create_table_with_primary_key_creates_index() {
        cleanup("test_create_table_pk_index");
        let mut executor = create_test_executor("test_create_table_pk_index");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("name", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        // index should exist in catalog
        let indexes = executor.database.get_indexes_for_table("users");
        assert!(indexes.is_some());
        let indexes = indexes.unwrap();
        assert_eq!(indexes.len(), 1);
        assert!(indexes[0].is_primary());
        assert_eq!(indexes[0].column_name(), "id");

        // index tree should be loaded in memory
        assert!(!executor.database.indexes().is_empty());

        cleanup("test_create_table_pk_index");
    }

    #[test]
    fn test_create_table_without_primary_key_no_index() {
        cleanup("test_create_table_no_pk");
        let mut executor = create_test_executor("test_create_table_no_pk");

        executor
            .execute(
                Statement::CreateTable {
                    name: "logs".to_string(),
                    columns: vec![
                        Column::new("message", DataType::Text, false),
                        Column::new("level", DataType::Integer, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        // no index should be created
        let indexes = executor.database.get_indexes_for_table("logs");
        assert!(indexes.is_none());

        cleanup("test_create_table_no_pk");
    }
}
