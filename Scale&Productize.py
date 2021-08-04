import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
import csv


class EcommerceOpition(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input', help='Input dataset for the pipeline')
        parser.add_argument('--output', help='Output file name for the pipeline')
        parser.add_argument('--ntop', type=int, help='Number of top customer cases to show', default='5')

def parse_file(element):
  for line in csv.reader([element], quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True):
    invoice, stock, descrip, quantity, Uprice, cust, ___ = line
    return [{'StockCode': stock,
                 'UnitPrice': Uprice,
                 'Quantity': quantity,
                 'Description': descrip,
                 'CustomerID': cust}]
def print_row(element):
  print(element)

### Still error
"""
class DeleteDataWithoutCustomerID(beam.DoFn):
    def process(self, element):
        return None if '' in element["CustomerID"] else str([element])
class DeleteDataWrongPrice(beam.DoFn):
    def process(self, element):
        return None if '-' in str(element["UnitPrice"]) else str([element])
        #return None if element["UnitPrice"] < 0 else [element]
class DeleteDataWrongQuantity(beam.DoFn):
    def process(self, element):
        return None if '-' in element["Quantity"] < 0 else [element]
"""

class CollectStockCodeKey(beam.DoFn):
    def process(self, element):
        return [(f"{element['StockCode']},{element['UnitPrice']}", int(element['CustomerID']))]


def my_pipeline():
    options = EcommerceOpition()
    with beam.Pipeline(options=options) as p:
        csv_formatted_data = (p
                              | "Reading the input dataset" >> beam.io.ReadFromText(options.input, skip_header_lines=1)
                              | 'Parse file' >> beam.ParDo(parse_file)
                              #| "Deleting data with no CustomerID" >> beam.ParDo(DeleteDataWithoutCustomerID())
                              #| "Deleting data with wrong price" >> beam.ParDo(DeleteDataWrongPrice())
                              #| "Deleting data with wrong Quantity" >> beam.ParDo(DeleteDataWrongQuantity())
                              )

        grouped_by_location = (csv_formatted_data
                               | "Colleting Location as Key" >> beam.ParDo(CollectStockCodeKey())
                               | "Grouping by location" >> beam.GroupByKey()
                               )

        top_cases = (grouped_by_location
                    | "Calculating top 5" >> beam.CombineValues(beam.combiners.TopCombineFn(n=int(options.ntop)))
                    )

        output_pipe = (
                {
                    f'top_{options.ntop}_customer': top_cases
                }
                | "CoGrouping by key" >> beam.CoGroupByKey()
                | 'format json' >> beam.Map(json.dumps)
                | "Writing out" >> beam.io.WriteToText(options.output))


if __name__ == "__main__":
    my_pipeline()
