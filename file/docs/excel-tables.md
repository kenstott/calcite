# Reading Excel workbooks

The file adapter reads each `.xlsx` or `.xls` workbook in its directory as a set of tables. A sheet can hold one table or several. Each table you get is named `<file>__<sheet>`; when a title row stands above it, `_<title>` follows. Tables on one sheet that would share a name are told apart as `_t1`, `_t2` and so on, in the order they stand on the sheet.

## How the reader finds tables

The reader looks first at what the workbook declares about itself:

1. A range you give a table (see [`range`](#range)).
2. Every Excel table on the sheet, the blocks made with **Format as Table**. Its own header row count is used.
3. Every named range that covers a single block of the sheet. Print areas, filter ranges, hidden names and names that hold a formula or a constant are skipped.

Each of these is read as one table, whatever its cells hold. Once a block is taken, the reader leaves it alone in the later steps.

The rest of each sheet is searched for blocks of cells. Two blank rows in a row end a block, and two blank columns in a row separate blocks that sit side by side. A single blank row or column inside a table stays part of it.

Within each block, the reader finds the table's parts from the top:

- **Title.** A row with one cell, above a row with more, is the table's title. It names the table and is not read as data.
- **Header rows.** The first header row has at least two cells and reads as labels. A row fails that test when any of its text runs past 50 characters or over several lines, when any text looks like an id, a hex string, an email address or a URL, or when it holds a number over 1000. A row that mixes text and numbers needs more than twice as many text cells as numbers. Below a header row, a wider row is a further header row. A group header row is sparse, since each label spans the columns it groups, so the detail header row under it is wider. A row no wider than the header row above it is data. The reader takes at most three header rows.
- **Columns.** Each column with a cell in the header rows becomes a column of the table. Its name joins the group labels above it to its own label, separated by `_`. Cells under a column with no header cell are not read.
- **Data rows.** These run from the last header row to the end of the block. A blank row inside the table is skipped. A table with no data rows is not read.

A row that starts no table, such as a note above a table, is passed over.

A cell holding an empty string counts as blank, the same as an empty cell. A formula is judged by the value it evaluates to. Hidden rows and columns are read like visible ones. A merged cell holds its value in its top-left cell, and the rest of it is blank.

## Hints

When the reader gets a workbook wrong, give it hints in the schema operand `excelHints`. Every hint you leave unset keeps the reader's own rule.

| Hint | What it sets | Auto |
|------|--------------|------|
| `headerRows` | The exact number of header rows. With it set, header rows are not detected. | detected |
| `maxHeaderRows` | The most rows detection takes as header rows. `1` means each table has a single header row. | 3 |
| `tableGapRows` | How many blank rows in a row end a table. | 2 |
| `columnGap` | How many blank columns in a row separate tables side by side. | 2 |
| `minColumns` | The fewest header cells a table has. `1` lets the reader find one-column tables. | 2 |
| `minDataRows` | The fewest data rows a table has. A table with fewer is not read. | 1 |
| `range` | The cells a table is, such as `Sheet1!A3:F200`. Quote a sheet name that has spaces: `'Q1 results'!A1:D9`. | none |

You can give a hint for every workbook in the schema, for one workbook by its file name under `workbooks`, or for one table by the name the reader gives it under `tables`. The narrowest one that sets a hint wins. The reader finds the gaps between tables before it knows which table is which, so `tableGapRows` and `columnGap` go on every workbook or on one workbook. `range` goes on one table, and that table takes the name you give it.

```json
"excelHints": {
  "maxHeaderRows": 1,
  "workbooks": {
    "watch_list.xlsx": {"columnGap": 1}
  },
  "tables": {
    "report__summary": {"headerRows": 2},
    "report__totals": {"range": "summary!H3:K20"}
  }
}
```

A hint that names a key the reader does not know, or gives a count below 1, refuses the schema and names the hint. So does a range with no sheet.

### When to reach for which

- The first data row was taken as a header row, or a header row was taken as data. Set `headerRows`. Years such as 2023 in the header row are numbers over 1000, so they read as data until you do.
- A table has one header row, but the row below it is wider. Set `maxHeaderRows: 1`.
- Tables are separated by a single blank row. Set `tableGapRows: 1`.
- Two tables sit side by side with one blank column between them. Set `columnGap: 1`. A table that keeps two blank columns inside itself needs `columnGap: 3`, or make it an Excel table.
- A table has a single column. Set `minColumns: 1`.
- Stray two-row fragments show up as tables. Raise `minDataRows`.
- Nothing else fits. Give the table a `range`, or make it an Excel table in the workbook.

### range

A range reads exactly its cells as one table, named by its key under `tables`. Its header rows are detected within the range unless you also set `headerRows`. The key must start with the workbook's name followed by `__`, as every table name of that workbook does. The reader uses that prefix to know which workbook the range belongs to.

## Changing hints

The adapter converts a workbook again when you change its hints. It also converts the workbook again when the workbook itself changes. Tables found under the earlier hints, or in the earlier version of the workbook, are removed first, so the schema lists only the tables the workbook holds now.
