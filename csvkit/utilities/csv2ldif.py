import itertools
from ldif3 import LDIFWriter

from csvkit import CSVKitReader
from csvkit.cli import CSVKitUtility, parse_column_identifiers
from csvkit.headers import make_default_headers

class CSV2Ldif(CSVKitUtility):
    description = 'Convert the csv to ldif format.'

    def add_arguments(self):
        self.argparser.add_argument('-n', '--names', dest='names_only', action='store_true',
                                    help='Display column names and indices from the input CSV and exit.')
        self.argparser.add_argument('-c', '--columns', dest='columns',
                                    help='A comma separated list of column indices or names to be extracted. Defaults to all columns.')
        self.argparser.add_argument('-C', '--not-columns', dest='not_columns',
                                    help='A comma separated list of column indices or names to be excluded. Defaults to no columns.')
        self.argparser.add_argument('-x', '--delete-empty-rows', dest='delete_empty', action='store_true',
                                    help='After cutting, delete rows which are completely empty.')
        self.argparser.add_argument('basedn',
                                    help='Set the basedn to use on ldif entries.')
        self.argparser.add_argument('uid',
                                    help='Set the attribute to use on dn of ldif entries.')

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

        dn_att_id = parse_column_identifiers(self.args.uid, column_names, self.args.zero_based,
                                              self.args.not_columns)

        output = LDIFWriter(self.output_file)

        #output.writerow([column_names[c] for c in column_ids])

        for row in rows:
            out_row = [row[c] if c < len(row) else None for c in column_ids]

            if self.args.delete_empty:
                if ''.join(out_row) == '':
                    continue
            zipped_row = zip(column_names, map(lambda x: [x],out_row))

            dn = self.args.uid + "=" + out_row[dn_att_id[0]] + "," + self.args.basedn
            output.unparse(dn, zipped_row)

def launch_new_instance():
    utility = CSV2Ldif()
    utility.main()

if __name__ == "__main__":
    launch_new_instance()
