import csv
from pathlib import Path
from typing import Dict, List

from loguru import logger


def extract_fieldnames_from_samplesheet(samplesheet: Path) -> List[str]:
	for line in samplesheet.read_text().split('\n'):
		if line.startswith('Sample'):
			return line.split(',')


def get_sheet_header(reader: csv.DictReader) -> List[Dict[str, str]]:
	# Iterate until the first line is found.
	ls = list()
	for line in reader:
		if line['Sample_ID'] == 'Sample_ID':
			break
		# After the check since we will write the header manually later.
		ls.append(line)
	return ls


def get_expected_filename(folder: Path) -> Path:
	return folder / "SampleSheet.csv"


def get_sequencing_date_from_folder(folder: Path) -> str:
	date_string = folder.name.split('_')[0].strip()
	year = date_string[:2]
	month = date_string[2:4]
	day = date_string[4:]

	return f"20{year}-{month}-{day}"


class SampleSheetFinder:
	def __init__(self, index_file, output_folder: Path):
		self.index_filename = index_file
		self.output_folder = output_folder
		if not self.output_folder.exists(): self.output_folder.mkdir()
		self.defined_sample_sheets = list()
		self.missing_sheets = list()  # Folders which are missing sample sheets
		self.globbed_sample_sheets = list()
		self.organized_samplesheets: Dict[str, List[Path]] = dict()
		self.selected_sample_sheets = list()  # New sheets not present in the index file.
		self.fieldnames = "Sample_ID,Sample_Name,Species,Project,NucleicAcid,Sample_Well,I7_Index_ID,index,I5_Index_ID,index2".split(',')

	def run(self, root_folder: Path):
		index = self.read_index()
		self.get_expected_files(root_folder)
		logger.info(f"Found {len(self.defined_sample_sheets)} SampleSheets.")
		logger.info(f"{len(self.missing_sheets)} folders were missing SampleSheets.")
		#self.globbed_sample_sheets = self.get_globbed_files(root_folder)
		#logger.info(f"Found {len(self.globbed_sample_sheets)} SampleSheets using glob.")
		self.selected_sample_sheets = self.filter_samplesheets(index)
		logger.info(f"Selected {len(self.selected_sample_sheets)} SampleSheets.")
		self.organized_samplesheets = self.organize_samplesheets(self.selected_sample_sheets)
		logger.info("Generating the output files...")
		self.generate_output()
		self.update_index()

	# Search for samplesheets

	def get_expected_files(self, root_folder: Path):
		""" Assumes samplesheets are present in a specific location within each folder."""
		for subfolder in root_folder.iterdir():
			if subfolder.is_file(): continue
			expected_filename = get_expected_filename(subfolder)
			# logger.debug(f"Testing if {expected_filename} exists...")
			if expected_filename.exists():
				self.defined_sample_sheets.append(expected_filename)
			else:
				self.missing_sheets.append(subfolder)

	@staticmethod
	def get_globbed_files(root_folder: Path):
		""" Uses glob to find all files named `SampleSheet.csv`"""
		logger.info(f"Searching for all sample sheets in the folders {root_folder}")

		files = list(root_folder.glob('**/SampleSheet.csv'))
		# Resolve the paths to their absolute value.
		sample_sheets = [i.resolve() for i in files]
		logger.info(f"Found {len(sample_sheets)} SampleSheets")
		return sample_sheets

	# General helper methods
	@staticmethod
	def organize_samplesheets(selected_sheets) -> Dict[str, List[Path]]:
		""" organizes samplesheets by sequencing date."""
		dates = dict()
		for samplesheet in selected_sheets:
			parent_folder = samplesheet.parent
			key = get_sequencing_date_from_folder(parent_folder)
			if key not in dates:
				dates[key] = [samplesheet]
			else:
				dates[key].append(samplesheet)
		# Remove repeated samplesheets. Will happen if glob is used.
		dates = {k:sorted(set(v)) for k,v in dates.items()}
		return dates

	def filter_samplesheets(self, indexed_sheets: List[Path]):
		# result = itertools.filterfalse(lambda s: s in indexed_sheets, current_sheets)

		# Messy implementation, but ensures that the `path` objects refer to identical files.
		# It would be easier to convert to strings and test for membership, but then the test for identical files would not exist.
		files = list()
		for left in self.defined_sample_sheets + self.globbed_sample_sheets:
			for right in indexed_sheets:
				if left.samefile(right):
					break
			else:
				# There are no paths in `indexed_sheets` that refer to this path.
				# Use a list instead of a generator to avoid complications.
				files.append(left)
		return files

	# Read and write samplesheets
	def combine_samplesheets(self, sheets, output_filename):
		all_lines = list()
		for sheet_filename in sheets:
			header, lines = self.read_samplesheet(sheet_filename)
			all_lines += lines

		with output_filename.open('w') as output:
			writer = csv.DictWriter(output, fieldnames = self.fieldnames)
			# Need to write the full header.
			writer.writerows(header)
			writer.writeheader()
			writer.writerows(all_lines)

	def read_samplesheet(self, filename: Path):
		with filename.open() as file1:
			reader = csv.DictReader(file1, fieldnames = self.fieldnames)
			# Iterate `reader` until it finds the first row.
			header = get_sheet_header(reader)
			lines = list(reader)
		return header, lines

	# Generate Output
	def generate_output(self):
		self.write_logs()
		file_folder = self.output_folder / "files"
		if not file_folder.exists(): file_folder.mkdir()
		self.write_organized_samplesheets(self.organized_samplesheets, file_folder)

	def write_organized_samplesheets(self, organized, output_folder):
		for date_string, sheets in organized.items():
			output_filename = output_folder / f"{date_string}.SampleSheet.csv"
			if len(sheets) > 1:
				self.combine_samplesheets(sheets, output_filename)
			elif len(sheets) == 1:
				output_filename.write_text(sheets[0].read_text())

	def write_logs(self):
		found_files_filename = self.output_folder / "filelist.txt"
		with found_files_filename.open('w') as file1:
			file1.write(f"Directories missing samplesheets:\n")
			for i in sorted(self.missing_sheets):
				file1.write(f"\t{i}\n")

			file1.write(f"Found {len(self.defined_sample_sheets)} expected sheets:\n")
			for i in sorted(self.defined_sample_sheets):
				file1.write(f"\t{i}\n")

			file1.write(f"Found {len(self.globbed_sample_sheets)} globbed sheets:\n")
			for i in sorted(self.globbed_sample_sheets):
				file1.write(f"\t{i}\n")

			file1.write(f"Final layout:\n")
			for key, sheets in self.organized_samplesheets.items():
				file1.write(f"\t{key}\n")
				for sheet in sheets:
					file1.write(f"\t\t{sheet}\n")

	# methods for accessing and updating the index file.
	def read_index(self) -> List[Path]:
		try:
			return [Path(line) for line in self.index_filename.read_text().split('\n')]
		except FileNotFoundError:
			return []

	def update_index(self):
		with self.index_filename.open('a') as file1:
			for path in self.selected_sample_sheets:
				p = path.resolve()
				file1.write(f"{p}\n")
