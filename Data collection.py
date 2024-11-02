import subprocess
import os
import pandas as pd
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

# === Step 1: Setup Directories and Paths ===
sra_dir = r"C:\Users\wilbu\PycharmProjects\QS Masters\SRA_DATA_RAW_HOTSPRING"  # Directory to store SRA files
input_dir = r"S:\SRA_DATA_HOTSPRING_FASTQ"  # Directory to store converted FASTQ files
csv_file_path = r"C:\Users\wilbu\PycharmProjects\QS Masters\HOTSPRINGS_ACC.csv"  # Path to CSV file with accession numbers
output_txt_path = r"C:\Users\wilbu\PycharmProjects\QS Masters\SELECTED_ACC.txt"  # Path to save selected accession numbers

# Create directories if they don't exist
os.makedirs(sra_dir, exist_ok=True)
os.makedirs(input_dir, exist_ok=True)

# === Step 2: Load Accession Numbers from CSV File ===
try:
    df = pd.read_csv(csv_file_path)
    sra_accessions = df['acc'].tolist()
    # Randomly select 100 accession numbers from the list
    if len(sra_accessions) > 100:
        sra_accessions = random.sample(sra_accessions, 100)

    # Write the selected accession numbers to a text file
    with open(output_txt_path, 'w') as f:
        for accession in sra_accessions:
            f.write(f"{accession}\n")
    print(f"Selected accession numbers saved to: {output_txt_path}")

except Exception as e:
    print(f"Error: Unable to load accession numbers from {csv_file_path}: {e}")
    sra_accessions = []

# === Step 3: Download and Convert SRA Files in Parallel ===
def download_and_convert(accession):
    """Function to download and convert SRA file for a given accession"""
    max_retries = 3
    success = False

    # Step 3a: Download the SRA file using prefetch with retry logic
    for attempt in range(max_retries):
        try:
            subprocess.run(["prefetch", accession], cwd=sra_dir, check=True)
            success = True
            break
        except subprocess.CalledProcessError:
            print(f"Attempt {attempt + 1} failed to download {accession}. Retrying...")
            time.sleep(5)

    if not success:
        return f"Error: Failed to download {accession} after {max_retries} attempts"

    # Identify the downloaded file by looking for the matching prefix (e.g., DRR, SRR, ERR)
    try:
        downloaded_files = [f for f in os.listdir(sra_dir) if f.startswith(accession)]
        if not downloaded_files:
            return f"Error: No file found for {accession} after downloading."
        # Assuming only one file is related to the accession
        sra_file = downloaded_files[0]
    except Exception as e:
        return f"Error: Failed to locate the file for {accession}: {e}"

    # Step 3b: Convert the SRA file to FASTQ using fastq-dump
    sra_path = os.path.join(sra_dir, sra_file)
    try:
        subprocess.run(["fastq-dump", "--split-files", sra_path, "-O", input_dir], check=True)
    except subprocess.CalledProcessError:
        return f"Error: Failed to convert {sra_file} to FASTQ"

    return f"Successfully processed {accession}"


# Parallel execution using ThreadPoolExecutor
print("Downloading and converting SRA files to FASTQ format in parallel...")
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = [executor.submit(download_and_convert, accession) for accession in sra_accessions]
    for future in as_completed(futures):
        print(future.result())

print("Download and conversion complete. FASTQ files available in:", input_dir)
