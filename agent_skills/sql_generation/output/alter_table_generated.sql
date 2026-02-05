-- Alter Table Operations
ALTER TABLE example_table
ADD COLUMNS (
  new_col_1 string COMMENT 'description'
);
CHANGE COLUMN old_col new_col int;