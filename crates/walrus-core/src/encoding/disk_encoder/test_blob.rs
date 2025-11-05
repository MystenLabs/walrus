use std::{boxed::Box, collections::HashMap, vec::Vec};

use rand::{RngCore, SeedableRng, rngs::SmallRng};

use super::*;

#[derive(Debug, Default)]
pub struct ExpandedTestBlob {
    pub rows: HashMap<Region, Vec<Vec<u8>>>,
    pub columns: HashMap<Region, Vec<Vec<u8>>>,
}

impl ExpandedTestBlob {
    pub fn iter_row_data(&self, region: Region) -> Box<dyn Iterator<Item = &Vec<u8>> + '_> {
        match region {
            Region::Source | Region::SourceColumnExpansion | Region::SourceRowExpansion => {
                Box::new(self.rows[&region].iter())
            }
            Region::Primary => Box::new(
                self.iter_row_data(Region::Source)
                    .chain(self.iter_row_data(Region::SourceColumnExpansion)),
            ),
            Region::Secondary => Box::new(
                self.iter_row_data(Region::Source)
                    .zip(self.iter_row_data(Region::SourceRowExpansion))
                    .flat_map(|(first, second)| [first, second]),
            ),
        }
    }

    pub fn iter_column_data(&self, region: Region) -> Box<dyn Iterator<Item = &Vec<u8>> + '_> {
        match region {
            Region::Source | Region::SourceColumnExpansion | Region::SourceRowExpansion => {
                Box::new(self.columns[&region].iter())
            }
            Region::Primary => Box::new(
                self.iter_column_data(Region::Source)
                    .zip(self.iter_column_data(Region::SourceColumnExpansion))
                    .flat_map(|(first, second)| [first, second]),
            ),
            Region::Secondary => Box::new(
                self.iter_column_data(Region::Source)
                    .chain(self.iter_column_data(Region::SourceRowExpansion)),
            ),
        }
    }

    pub fn new(params: BlobExpansionParameters) -> ExpandedTestBlob {
        let mut rng = SmallRng::seed_from_u64(73);
        let disjoint_regions = [
            Region::Source,
            Region::SourceRowExpansion,
            Region::SourceColumnExpansion,
        ];

        let mut rows: HashMap<Region, Vec<Vec<u8>>> = HashMap::new();

        for region in disjoint_regions {
            let region_rows = rows.entry(region).or_default();

            for _ in 0..params.row_count(region) {
                let mut row_data = std::vec![0u8; params.row_bytes(region)];
                rng.fill_bytes(&mut row_data);
                region_rows.push(row_data);
            }
        }

        let mut columns: HashMap<Region, Vec<Vec<u8>>> = HashMap::new();

        for region in disjoint_regions {
            let region_rows = &rows[&region];
            let region_columns = columns.entry(region).or_default();

            region_columns.resize_with(params.column_count(region), Default::default);

            for row in region_rows {
                let symbols = row.chunks_exact(params.symbol_size);

                for (symbol, column) in symbols.zip(region_columns.iter_mut()) {
                    column.extend(symbol);
                }
            }
        }

        Self { rows, columns }
    }
}
