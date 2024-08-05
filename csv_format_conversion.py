import os
import argparse
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

def csv_convert(src_dir, dest_dir):
	if not os.path.exists(dest_dir):
		os.makedirs(dest_dir)

	for file_name in os.listdir(src_dir):
		if file_name.endswith('.csv'):
			csv_file_path = os.path.join(src_dir, file_name)
			parquet_file_path = os.path.join(dest_dir, file_name.replace('.csv','.parquet'))
			
			df = pd.read_csv(csv_file_path)
			table = pa.Table.from_pandas(df)
			pq.write_table(table, parquet_file_path)

			print(f"Converted {csv_file_path} to {parquet_file_path}")

def main():
	parser = argparse.ArgumentParser(description='Batch convert CSV files to Parquet format.')
	parse.add_arguement('src_dir', type=str, help='Source directory')
	parse.add_arguement('dest_dir', type=str, help='Destination directory')

	args = parser.parse_args()
	csv_convert(args.src_dir, args.dest_dir)

if _name_ == '_main_':
	main()