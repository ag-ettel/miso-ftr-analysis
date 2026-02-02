import urllib.request
import json
import csv
import os
from datetime import datetime 

## configuration
API_BASE = "https://apim.misoenergy.org/pricing/v1"
SUBSCRIPTION_KEY = os.getenv('MISO_PRICING_KEY')
OUTPUT_DIR = "data/miso_nodes"
OUTPUT_FILE = f"{OUTPUT_DIR}/miso_nodes.csv"

def fetch_miso_nodes():
    """
    Fetches all aggregated pnode data from MISO API across all pages
    Returns list of node dictionaries
    """
    all_nodes = []
    page = 1
    max_pages = 10  

    while page <= max_pages:
        url = f"{API_BASE}/aggregated-pnode?pageNumber={page}"

        headers = {
            'Cache-Control': 'no-cache',
            'Ocp-Apim-Subscription-Key': SUBSCRIPTION_KEY
        }

        try:
            print(f"Fetching page {page}...")
            req = urllib.request.Request(url, headers=headers)
            response = urllib.request.urlopen(req, timeout=30)

            if response.getcode() != 200:
                print(f"Error fetching page {page}: HTTP {response.getcode()}")
                break

            # Get the raw JSON response
            raw_data = response.read().decode('utf-8')
            data = json.loads(raw_data)

            # Extract the list from the "data" key
            nodes_list = data.get("data", [])

            ## check for data
            if not nodes_list or len(nodes_list) == 0:
                print(f"No more data found. Completed at page {page - 1}.")
                break

            ## add page number to each node record
            for node in nodes_list:
                node['page_fetched'] = page

            all_nodes.extend(nodes_list)
            print(f"  Retrieved {len(nodes_list)} nodes.")

            # check if less than expected records, indicating last page
            if len(nodes_list) < 1000:
                print("Last page reached based on record count.")
                break

            page += 1

        except urllib.error.HTTPError as e:
            if e.code == 404:
                print(f"Page {page} not found (404). Ending fetch.")
                break
            else:
                print(f"HTTP error on page {page}: {e}")
                break

    return all_nodes

def save_nodes_to_csv(nodes, output_file):
    """
    Saves list of node dictionaries to CSV file
    """
    if not nodes:
        print("No nodes to save.")
        return False

    ## ensure output directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    ## get field names from first record
    fieldnames = nodes[0].keys()

    with open(output_file, mode='w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for node in nodes:
            writer.writerow(node)

    print(f"Saved {len(nodes)} nodes to {output_file}")

def main():
    print(f"Starting MISO aggregated pnode retrieval at {datetime.now()}")

    nodes = fetch_miso_nodes()

    if nodes:
         # save to CSV
         save_nodes_to_csv(nodes, OUTPUT_FILE)
         print(f"\nCompleted MISO aggregated pnode retrieval at {datetime.now()}")

         # basic stats
         node_types = set(node.get('nodeType') for node in nodes if node.get('nodeType'))
         print(f"\nTotal nodes retrieved: {len(nodes)}")
         print(f"\nUnique node types: {sorted(node_types)}")

    else:
        print("No nodes were retrieved.")

if __name__ == "__main__":
    main()

