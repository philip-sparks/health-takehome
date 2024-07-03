import ijson
import os
import csv

def extract_descriptions_and_locations(input_file, output_file):
    with open(input_file, 'rb') as file, open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        parser = ijson.parse(file)
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['Description', 'Location'])  # Write header
        
        current_description = None
        pair_count = 0
        
        for prefix, event, value in parser:
            if prefix.endswith('.description'):
                current_description = value
            elif prefix.endswith('.location') and current_description is not None:
                csv_writer.writerow([current_description, value])
                pair_count += 1
                current_description = None  # Reset for the next pair
                
                if pair_count % 1000 == 0:
                    print(f"Processed {pair_count} pairs...")

    return pair_count

if __name__ == "__main__":
    input_file = "/Users/psparks/Public/Github/spark-essentials/src/main/resources/health/2024-06-01_anthem_index.json"
    output_file = "descriptions_and_locations.csv"

    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"Input file '{input_file}' not found.")
        exit(1)

    # Process the JSON file
    pair_count = extract_descriptions_and_locations(input_file, output_file)

    print(f"Extraction complete. {pair_count} description-location pairs written to '{output_file}'.")