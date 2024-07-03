import ijson
import os

def extract_new_york_ppo_urls(input_file, output_file):
    seen_urls = set()  # To avoid duplicates
    
    with open(input_file, 'rb') as file, open(output_file, 'w') as output:
        parser = ijson.parse(file)
        current_description = None
        
        for prefix, event, value in parser:
            if prefix.endswith('.description'):
                current_description = value.upper()
            elif prefix.endswith('.location') and current_description:
                if 'NEW YORK' in current_description and 'PPO' in current_description:
                    if value not in seen_urls:
                        output.write(f"{value}\n")
                        seen_urls.add(value)
                current_description = None  # Reset for the next item

    return len(seen_urls)

if __name__ == "__main__":
    input_file = "/Users/psparks/Public/Github/spark-essentials/src/main/resources/health/2024-06-01_anthem_index.json"
    output_file = "new_york_ppo_urls.txt"

    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"Input file '{input_file}' not found.")
        exit(1)

    # Process the JSON file
    url_count = extract_new_york_ppo_urls(input_file, output_file)

    print(f"Extraction complete. {url_count} unique URLs written to '{output_file}'.")