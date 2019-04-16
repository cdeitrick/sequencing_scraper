import csv
import datetime
import itertools
from pathlib import Path
from typing import List, Optional

import pandas
from loguru import logger
import time
COLUMNMAP = {
	'I5_Index_ID': 'i5IndexId',
	'I7_Index_ID': 'i7IndexId',
	'Index_ID':    'indexId',
	'Name':        'sampleName',
	'NucleicAcid': 'nucleicAcid',
	'Pop':         'pop',
	'Project':     'projectName',
	'SampleID':    'sampleId',
	'Sample_ID':   'sampleId',
	'Sample_Name': 'sampleName',
	'Sample_Well': 'sampleWell',
	'Species':     'species',
	'Well':        'sampleWell',
	'index':       'index',
	'index2':      'index2'
}


def extract_date_from_sample_id(sample_id: str) -> Optional[datetime.date]:
	if not isinstance(sample_id, str): return None
	string = sample_id.split('_')[0]

	try:
		month = int(string[:2])
		day = int(string[2:4])
		year = 2000 + int(string[4:])
		result = datetime.date(year = year, month = month, day = day)
	except ValueError:
		result = None
	return result


def search_for_sample_sheets(folder: Path, index = 0) -> List[Path]:
	sample_sheets = list()
	if index > 3: return sample_sheets
	for path in folder.iterdir():
		if path.is_dir():
			sample_sheets += search_for_sample_sheets(path, index + 1)
		elif path.name == 'SampleSheet.csv':
			sample_sheets.append(path)
	return sample_sheets


def find_all_sample_sheets(*paths) -> List[Path]:
	""" Finds all sample sheets within the given folders. """
	logger.info(f"Searching for all sample sheets in the folders {paths}")
	sample_sheets = list()
	for path in paths:
		files = list(path.glob('**/*SampleSheet.csv'))
		logger.info(f"Found {len(files)} samplesheets in folder {path}")
		sample_sheets += files
	# Resolve the paths to their absolute value.
	sample_sheets = [i.resolve() for i in sample_sheets]
	logger.info(f"Found {len(sample_sheets)} SampleSheets")
	return sample_sheets


def combine_sample_sheets(filenames: List[Path]) -> pandas.DataFrame:
	""" Combines the individual samplesheets into a single DataFrame."""
	fieldnames = ['Sample_ID', 'Sample_Name', 'Species', 'Project', 'NucleicAcid', 'Sample_Well', 'I7_Index_ID', 'index', 'I5_Index_ID', 'index2']
	sample_sheets = list()
	for filename in filenames:
		with filename.open() as file1:
			reader = csv.DictReader(file1, fieldnames = fieldnames)
			for line in reader:
				# Find the header line, then break. The rest of the `reader` object should be the samples.
				if line['Sample_ID'] == 'Sample_ID':
					break
			# Consume the rest of the `reader` object.
			sample_sheets += list(reader)

	df = pandas.DataFrame(sample_sheets)
	if None in df.columns:
		# This happens when there are extra columns in the samplesheet.
		df.pop(None)
	df.columns = [COLUMNMAP[i] for i in df.columns]
	return df


def generate_combined_sample_sheet(path: Path = None, *paths) -> pandas.DataFrame:
	""" Generates a single samplesheet from all of the individual sample sheets.
		Parameters
		----------
		path: Path
			If a folder, the filename will be generated based on the date.
	"""

	logger.info("Searching for all sample sheets...")
	sample_sheets = find_all_sample_sheets(*paths)

	logger.info("Combining all sample sheets...")
	sample_sheet = combine_sample_sheets(sample_sheets)

	billing_table = sample_sheet
	logger.info(f"Found {len(billing_table)} samples")

	if billing_table.empty:
		logger.warning("The scraper did not find any samplesheets.")
	else:
		billing_table['date'] = billing_table['sampleId'].apply(extract_date_from_sample_id)
		current_date = datetime.datetime.now().date().isoformat()
		basename = f"combined_sample_sheet.{current_date}.tsv"
		if path:
			if path.is_dir():
				filename = path / basename
			else:
				filename = path
		else:
			filename = basename
		billing_table.to_csv(str(filename))

	return billing_table


def read_index(filename: Path) -> List[Path]:
	try:
		return [Path(line) for line in filename.read_text().split('\n')]
	except FileNotFoundError:
		return []

def write_index(paths: List[Path], filename: Path):
	with filename.open('a') as file1:
		for path in paths:
			p = path.resolve()
			file1.write(f"{p}\n")


def filter_samplesheets(current_sheets: List[Path], indexed_sheets: List[Path]) -> List[Path]:

	#result = itertools.filterfalse(lambda s: s in indexed_sheets, current_sheets)

	# Messy implementation, but ensures that the `path` objects refer to identical files.
	# It would be easier to convert to strings and test for membership, but then the test for identical files would not exist.
	files = list()
	for left in current_sheets:
		for right in indexed_sheets:
			if left.samefile(right):
				break
		else:
			# There are no paths in `indexed_sheets` that refer to this path.
			# Use a list instead of a generator to avoid complications.
			files.append(left)
	return files



def schedule_scraping():
	""" Runs the scraper every week to find new SampleSheets."""
	# Need to keep track of which samplesheets have already been processed.
	# Check daily.
	delay = 3600 * 24 # The number of seconds to wait before checking for more samplesheets.
	#config_path = Path("/home/data/raw")
	#dmux_folder = Path("/home/data/dmux")


	# Assign the files that will be used by the script.
	folder = Path(__file__).parent
	folders = [Path("/home/data/dmux")]
	index_filename = folder / "samplesheet.index.txt"
	log_filename = folder / "log.txt"
	logger.add(log_filename)

	for i in range(10):  # for debugging. replace with while loop in future.
		indexed_sheets = read_index(index_filename)

		# Generate the name of the samplesheet based on which week it is.

		sample_sheets = find_all_sample_sheets(*folders)
		# Remove sample sheets that have already been indexed.
		new_sheets = filter_samplesheets(sample_sheets, indexed_sheets)
		logger.info(f"There are {len(new_sheets)} file(s) after filtering.")
		# Combine the new samplesheets into a table.
		table = combine_sample_sheets(new_sheets)

		current_datetime = datetime.datetime.now()
		current_date = current_datetime.date().isoformat()
		filename = folder / f"{current_date}.SampleSheet.csv"
		logger.info(f"Saving as {filename}")
		table.to_csv(str(filename))
		write_index(new_sheets, index_filename)

		time.sleep(delay)


if __name__ == "__main__":
	schedule_scraping()


