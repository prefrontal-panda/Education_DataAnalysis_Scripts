# Importing Libraries
import pandas as pd
import os
import argparse
import datetime

# Set chunk size for reading large CSVs
CHUNK_SIZE = 5000  # Adjust as needed

# Dynamically add, remove columns and process each chunk
def process_chunk(chunk, file_name):
    # Clean column names
    chunk.columns = chunk.columns.str.strip().str.replace(r'\s+', ' ', regex=True)

    # Add Campus, Test Year, and Year Level
    chunk['Campus'] = file_name.split('_')[3].split('.')[0]
    chunk['Test Year'] = file_name.split('_')[0].split('.')[0]
    chunk['Year Level'] = file_name.split('_')[2].split('.')[0]

    # Make full name column
    chunk['Full Name'] = chunk[['First Name', 'Second Name', 'Surname']].fillna('').agg(
        lambda x: ' '.join(filter(None, x)), axis=1
    )

    # Adding processing stamp
    chunk['Processed On'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Adding source file
    chunk['Source File'] = file_name

    # Drop unnecessary columns
    drop_cols = ['APS Year', 'Reporting Test', 'First Name', 'Second Name', 'Surname', 
                 'Home Group', 'Date of Birth', 'Home School Name', 'Reporting School Name']
    for col in drop_cols:
        if col in chunk.columns:
            chunk.drop(columns=[col], inplace=True)

    # Rename student ID column
    if 'Cases ID' in chunk.columns:
        chunk.rename(columns={'Cases ID': 'Student ID'}, inplace=True)

    # Rearranging column order
    new_col_order = ['Full Name', 'Campus', 'Test Year', 'Year Level', 'Student ID',
                     'READING', 'READING Proficiency', 'WRITING', 'WRITING Proficiency',
                     'SPELLING', 'SPELLING Proficiency', 'NUMERACY', 'NUMERACY Proficiency',
                     'GRAMMAR & PUNCTUATION', 'GRAMMAR & PUNCTUATION Proficiency',
                     'Date of birth', 'Gender', 'LBOTE', 'ATSI', 'Processed On', 'Source File'
                     ]

    # Keeping existing columns
    existing_cols = [col for col in new_col_order if col in chunk.columns]
    # Keep other columns 
    remaining_cols = [col for col in chunk.columns if col not in existing_cols]


    chunk = chunk[existing_cols + remaining_cols]

    # Returns chunk
    return chunk

# Incremental pipeline function
def incremental_pipeline(input_folder, master_file, missing_file, processed_log, force_rebuild=False):
    # If force_rebuild is True, start fresh
    if force_rebuild:
        if os.path.exists(master_file):
            os.remove(master_file)
        if os.path.exists(processed_log):
            os.remove(processed_log)
        print("Force rebuild: master file and processed log cleared.")
        processed_files = set()
    else:
        # Load processed files normally
        if os.path.exists(processed_log):
            with open(processed_log) as f:
                processed_files = set(line.strip() for line in f)
        else:
            processed_files = set()

    # Loop through all CSV files
    for file_name in os.listdir(input_folder):
        if file_name.endswith('.csv'):
            file_path = os.path.join(input_folder, file_name)
            print(f"\nProcessing file: {file_name}")

            # Process in chunks
            for chunk in pd.read_csv(file_path, chunksize=CHUNK_SIZE):
                df_chunk = process_chunk(chunk, file_name)

                # Append to master CSV
                df_chunk.to_csv(master_file, mode='a', header=not os.path.exists(master_file), index=False)

                # Save missing student IDs
                missing = df_chunk.loc[df_chunk['Student ID'].isna(), ['Full Name', 'Campus', 'Test Year']]
                if not missing.empty:
                    missing.to_csv(missing_file, mode='a', header=not os.path.exists(missing_file), index=False)

            print(f"Finished processing: {file_name}")

# Main function
def main():
    parser = argparse.ArgumentParser(description='Cleaning StudentOutcomeLevel files for NAPLAN.')
    parser.add_argument('--input', type=str, required=True, help='Input folder with campus files.')
    parser.add_argument('--output', type=str, required=True, help='Path to save or append master CSV.')
    parser.add_argument('--missing', type=str, default='missing_ids.csv', help='Path to save missing student IDs.')
    parser.add_argument('--log', type=str, default='Append_log.csv', help='Log files to store processing information.')
    parser.add_argument('--force_rebuild', action='store_true', help='Rebuild the master CSV from scratch, ignoring processed files log.')

    args = parser.parse_args()

    incremental_pipeline(args.input, args.output, args.missing, args.log, force_rebuild=args.force_rebuild)
    print(f"\nMaster CSV saved to: {args.output}")
    print(f"Missing IDs saved to: {args.missing}")

if __name__ == '__main__':
    print("Files should be named as: [test_year]_StudentOutcomeLevel_Yr[x]_[campus].csv")
    form_check = input("Do your files follow this format (y/n)?").strip().lower()

    if form_check == "y":
        main()
    elif form_check == "n":
        print("Please ensure that your files follow the naming convention required.")
    else:
        print("Please type 'y' or 'n'.")









# With store_true, you don’t actually pass a value like True or False on the command line. By default, the flag is False.
# If you include the flag when calling the script, it becomes True.

# For example, if your parser has:
# parser.add_argument('--force_rebuild', action='store_true', help='Rebuild master CSV from scratch.')

# You would run your script like this:
# python your_script.py --input input_folder --output master.csv

