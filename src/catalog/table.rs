use crate::catalog::schema::Schema;
use crate::storage::page::PageManager;
use std::collections::HashMap;
use std::io::{self, Error, ErrorKind};
pub struct TableMetadata {
    schema: Schema,
    first_page: u32,
}

pub struct TableCatalog {
    tables: HashMap<String, TableMetadata>,
    page_manager: PageManager,
}

impl TableCatalog {
    pub fn new(page_manager: PageManager) -> io::Result<Self> {
        // try reading existing catalog
        let catalog_data = page_manager.read_page(1u32)?;

        if catalog_data.iter().all(|&b| b == 0) {
            // empty catalog
            return Ok(TableCatalog {
                tables: HashMap::new(),
                page_manager,
            });
        }

        let mut offset = 0;

        if catalog_data.len() < 4 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Not enough bytes for number of tables".to_string(),
            ));
        }

        let num_tables = u32::from_le_bytes([
            catalog_data[offset],
            catalog_data[offset + 1],
            catalog_data[offset + 2],
            catalog_data[offset + 3],
        ]) as usize;
        offset += 4;

        let mut tables = HashMap::new();

        for _ in 0..num_tables {
            let (schema, bytes_consumed) = Schema::from_bytes(&catalog_data[offset..])?;
            offset += bytes_consumed;

            if catalog_data.len() < offset + 4 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "Not enough bytes for first page".to_string(),
                ));
            }

            let first_page = u32::from_le_bytes([
                catalog_data[offset],
                catalog_data[offset + 1],
                catalog_data[offset + 2],
                catalog_data[offset + 3],
            ]);
            offset += 4;

            let table_metadata = TableMetadata { schema, first_page };

            tables.insert(
                table_metadata.schema.table_name().to_string(),
                table_metadata,
            );
        }

        Ok(TableCatalog {
            tables,
            page_manager,
        })
    }

    fn create_table(&mut self, schema: Schema) -> io::Result<()> {
        // allocate first page for table data
        let first_page = self.page_manager.allocate_page()?;

        let table_name = schema.table_name().to_string();
        let table_metadata = TableMetadata { schema, first_page };

        self.tables.insert(table_name, table_metadata);

        // save to disk
        self.save()?;

        Ok(())
    }

    pub fn save(&mut self) -> io::Result<()> {
        let bytes = self.to_bytes();
        self.page_manager.write_page(1u32, &bytes)?;
        Ok(())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // number of tables
        bytes.extend_from_slice(&(self.tables.len() as u32).to_le_bytes());

        for (_, metadata) in self.tables.iter() {
            let schema_bytes = metadata.schema.to_bytes();
            bytes.extend_from_slice(&schema_bytes);

            // first page
            bytes.extend_from_slice(&metadata.first_page.to_le_bytes());
        }

        bytes
    }
}
