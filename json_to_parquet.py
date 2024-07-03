import ijson
import pyarrow as pa
import pyarrow.parquet as pq
import os

def process_json_file(input_file, output_file, batch_size=10000):
    schema = pa.schema([
        ('reporting_entity_name', pa.string()),
        ('reporting_entity_type', pa.string()),
        ('version', pa.string()),
        ('reporting_structure', pa.list_(pa.struct([
            ('reporting_plans', pa.list_(pa.struct([
                ('plan_name', pa.string()),
                ('plan_id_type', pa.string()),
                ('plan_id', pa.string()),
                ('plan_market_type', pa.string())
            ]))),
            ('in_network_files', pa.list_(pa.struct([
                ('description', pa.string()),
                ('location', pa.string())
            ]))),
            ('allowed_amount_file', pa.struct([
                ('description', pa.string()),
                ('location', pa.string())
            ]))
        ])))
    ])

    writer = None
    batch = []
    processed_count = 0

    with open(input_file, 'rb') as file:
        parser = ijson.parse(file)
        current_item = {}
        current_structure = None
        current_reporting_plan = None
        current_in_network_file = None

        for prefix, event, value in parser:
            if prefix == 'reporting_entity_name':
                current_item['reporting_entity_name'] = value
            elif prefix == 'reporting_entity_type':
                current_item['reporting_entity_type'] = value
            elif prefix == 'version':
                current_item['version'] = value
            elif prefix == 'reporting_structure':
                if event == 'start_array':
                    current_item['reporting_structure'] = []
            elif prefix.startswith('reporting_structure.item'):
                if event == 'start_map':
                    current_structure = {'reporting_plans': [], 'in_network_files': [], 'allowed_amount_file': None}
                elif event == 'end_map':
                    current_item['reporting_structure'].append(current_structure)
                    current_structure = None
            elif prefix.startswith('reporting_structure.item.reporting_plans.item'):
                if event == 'start_map':
                    current_reporting_plan = {}
                elif event == 'end_map':
                    current_structure['reporting_plans'].append(current_reporting_plan)
                    current_reporting_plan = None
                else:
                    key = prefix.split('.')[-1]
                    current_reporting_plan[key] = value
            elif prefix.startswith('reporting_structure.item.in_network_files.item'):
                if event == 'start_map':
                    current_in_network_file = {}
                elif event == 'end_map':
                    current_structure['in_network_files'].append(current_in_network_file)
                    current_in_network_file = None
                else:
                    key = prefix.split('.')[-1]
                    current_in_network_file[key] = value
            elif prefix.startswith('reporting_structure.item.allowed_amount_file'):
                key = prefix.split('.')[-1]
                if current_structure['allowed_amount_file'] is None:
                    current_structure['allowed_amount_file'] = {}
                current_structure['allowed_amount_file'][key] = value

            if prefix == '' and event == 'end_map':
                batch.append(current_item)
                current_item = {}
                processed_count += 1
                print(f"Successfully processed JSON line {processed_count}")

                if len(batch) >= batch_size:
                    table = pa.Table.from_pylist(batch, schema=schema)
                    if writer is None:
                        writer = pq.ParquetWriter(output_file, schema=table.schema)
                    writer.write_table(table)
                    batch = []

    if batch:
        table = pa.Table.from_pylist(batch, schema=schema)
        if writer is None:
            writer = pq.ParquetWriter(output_file, schema=table.schema)
        writer.write_table(table)

    if writer:
        writer.close()

if __name__ == "__main__":
    input_file = "/Users/psparks/Public/Github/spark-essentials/src/main/resources/health/2024-06-01_anthem_index.json"
    output_file = "2024-06-01_anthem_index.parquet"

    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"Input file '{input_file}' not found.")
        exit(1)

    # Process the JSON file
    process_json_file(input_file, output_file)

    print(f"Parquet file '{output_file}' has been created successfully.")