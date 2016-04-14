import itertools
from csvkit import CSVKitReader
from csvkit.cli import CSVKitUtility, parse_column_identifiers
from csvkit.headers import make_default_headers

class CSV2Sql(CSVKitUtility):
    description = 'Convert the csv to ldif format.'

    def add_arguments(self):
        self.argparser.add_argument('tablename',
                                    help='Set the table to be populated.')
        self.argparser.add_argument('-n', '--names', dest='names_only', action='store_true',
                                    help='Display column names and indices from the input CSV and exit.')
        self.argparser.add_argument('-c', '--columns', dest='columns',
                                    help='A comma separated list of column indices or names to be extracted. Defaults to all columns.')
        self.argparser.add_argument('--columns-types', dest='columns_types',
                                    help='A comma separated list of column types s=string,n=number,b=boolean. Defaults to all columns as string.')
        self.argparser.add_argument('-C', '--not-columns', dest='not_columns',
                                    help='A comma separated list of column indices or names to be excluded. Defaults to no columns.')

    def mapcolumntype(self, type):
        if type == 's' or type == 'S':
            return lambda x: '\'' + str(x) + '\''
        if type == 'n' or type == 'N':
            return lambda x: str(x)
        if type == 'b' or type == 'B':
            return lambda x: 'TRUE' if str(x).lower() == 'true' or str(x).lower() == 't' or str(x).lower() == '1' else 'FALSE'
        raise "Unknown type " + type

    def parse_column_types(self, types, ids):
        if not types:
            return dict(zip(ids,[self.mapcolumntype('s')] * len(ids)))
        col_types = types.split(',')

        if len(col_types) == 1:
            return dict(zip(ids,[self.mapcolumntype(col_types[0].strip())] * len(ids)))

        if len(col_types) != len(ids):
            raise "the size of column types array must be 1 or equal to the number of columns"

        types_collection = []
        for c in col_types:
            types_collection.append(self.mapcolumntype(c.strip()))

        return dict(zip(ids,types_collection))

    def main(self):
        if self.args.names_only:
            self.print_column_names()
            return

        rows = CSVKitReader(self.input_file, **self.reader_kwargs)

        if self.args.no_header_row:
            row = next(rows)

            column_names = make_default_headers(len(row))

            # Put the row back on top
            rows = itertools.chain([row], rows)
        else:
            column_names = next(rows)

        column_ids = parse_column_identifiers(self.args.columns, column_names, self.args.zero_based,
                                              self.args.not_columns)
        columns_type = self.parse_column_types(self.args.columns_types, column_ids)
        column_names = map(lambda x: column_names[x],column_ids)


        output = self.output_file

        for row in rows:
            out_row = [row[c] if c < len(row) else None for c in column_ids]

            if ''.join(out_row) == '':
                continue

            insert_stat = "INSERT INTO " + self.args.tablename + "("
            insert_stat += ",".join(column_names)
            insert_stat += ") VALUES ("
            insert_stat += ",".join(
                map(lambda colid: columns_type[colid](row[colid]) ,
                    column_ids))
            insert_stat += ");"
            output.write('%s\n' % insert_stat)

def launch_new_instance():
    utility = CSV2Sql()
    utility.main()

if __name__ == "__main__":
    launch_new_instance()
