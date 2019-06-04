import csv
from datetime import date
from pathlib import Path

import pytest

import scraper

DATA_FOLDER = Path(__file__).with_name("data")
SAMPLES = [
	DATA_FOLDER / "170120.SampleSheet.csv",
	DATA_FOLDER / "181017.SampleSheet.csv",
	DATA_FOLDER / "190519.SampleSheet.csv"
]


def checkdir(path: Path) -> Path:
	if path.is_dir() and not path.exists():
		path.mkdir()
	else:
		checkdir(path.parent)
	return path


@pytest.fixture
def raw_folder(tmp_path) -> Path:
	""" Sets up a folder layed out in a similar manner to the `raw` directory on beagle."""
	parent_folder = tmp_path / "raw_folder"
	parent_folder.mkdir()
	samplefile1 = DATA_FOLDER / "170120.SampleSheet.csv"
	samplefile2 = DATA_FOLDER / "181017.SampleSheet.csv"

	folder1 = parent_folder / "170120"
	folder1.mkdir()
	(folder1 / "SampleSheet.csv").write_text(samplefile1.read_text())

	folder2 = parent_folder / "181017"
	folder2.mkdir()
	(folder2 / "SampleSheet.csv").write_text(samplefile2.read_text())

	folder3 = parent_folder / "190519"
	folder3.mkdir()

	return parent_folder


@pytest.fixture
def index_filename(raw_folder, tmp_path) -> Path:
	f = tmp_path / "index.txt"

	indexed_files = [
		raw_folder / "170120" / "SampleSheet.csv",
		raw_folder / "150311" / "sampleSheet.csv"
	]

	f.write_text("\n".join(str(i) for i in indexed_files))

	return f


@pytest.fixture
def filename() -> Path:
	return DATA_FOLDER / "170120.SampleSheet.csv"


@pytest.fixture
def empty_finder(tmp_path) -> scraper.SampleSheetFinder:
	output_folder = tmp_path / "output"
	index_filename = output_folder / "index.txt"
	return scraper.SampleSheetFinder(index_filename, output_folder)


@pytest.fixture
def indexed_finder(tmp_path, index_filename):
	output_folder = tmp_path / "output"
	return scraper.SampleSheetFinder(index_filename, output_folder)


@pytest.mark.parametrize("value,expected",
	[
		("032117_19", date(2017, 3, 21)),
		("032117_86", date(2017, 3, 21)),
		("041416_005", date(2016, 4, 14))
	])
def test_extract_date_from_sample_id(value, expected):
	result = scraper.extract_date_from_sample_id(value)
	assert result == expected


@pytest.mark.parametrize("filename, expected",
	[
		(DATA_FOLDER / "170120.SampleSheet.csv", "2017-01-20"),
		(DATA_FOLDER / "181017.SampleSheet.csv", "2018-10-17"),
		(DATA_FOLDER / "190519.SampleSheet.csv", "2019-05-19")
	])
def test_extract_date_from_samplesheet(filename, expected):
	result = scraper.get_container_id(filename)
	result = scraper.containerid_to_date(result)

	assert result == expected


@pytest.mark.parametrize("filename, expected",
	[
		(DATA_FOLDER / "170120.SampleSheet.csv", "170120"),
		(DATA_FOLDER / "181017.SampleSheet.csv", "181017"),
		(DATA_FOLDER / "190519.SampleSheet.csv", "190519")
	])
def test_extract_containerid_from_samplesheet(filename, expected):
	result = scraper.get_container_id(filename)

	assert result == expected


@pytest.mark.parametrize("filename", SAMPLES)
def test_extract_fieldnames_from_samplesheet(filename):
	result = scraper.extract_fieldnames_from_samplesheet(filename)
	assert result == "Sample_ID,Sample_Name,Species,Project,NucleicAcid,Sample_Well,I7_Index_ID,index,I5_Index_ID,index2".split(',')


@pytest.mark.parametrize("filename", SAMPLES)
def test_get_sheet_header(filename, empty_finder):
	reader = csv.DictReader(filename.open(), fieldnames = empty_finder.fieldnames)
	result = scraper.get_sheet_header(reader)

	assert result[0] == dict(zip(empty_finder.fieldnames, "[Header],,,,,,,,,".split(',')))
	assert result[1] == dict(zip(empty_finder.fieldnames, "FileVersion,1,,,,,,,,".split(',')))
	assert result[2] == dict(zip(empty_finder.fieldnames, "LibraryPrepKit,Nextera DNA,,,,,,,,".split(',')))
	assert result[3] == dict(zip(empty_finder.fieldnames, "ContainerType,Plate96,,,,,,,,".split(',')))


@pytest.mark.parametrize("containerid,expected",
	[
		("190519", "2019-05-19"),
		("170120", "2017-01-20")
	])
def test_containerid_to_date(containerid, expected):
	result = scraper.containerid_to_date(containerid)
	assert result == expected


def test_get_expected_files(empty_finder, raw_folder):
	empty_finder.get_expected_files(raw_folder)
	samplefile1 = raw_folder / "170120" / "SampleSheet.csv"
	samplefile2 = raw_folder / "181017" / "SampleSheet.csv"

	assert len(empty_finder.defined_sample_sheets) == 2
	assert samplefile1 in empty_finder.defined_sample_sheets
	assert samplefile2 in empty_finder.defined_sample_sheets

	assert raw_folder / "190519" in empty_finder.missing_sheets


def test_get_globbed_files(empty_finder, raw_folder):
	result = empty_finder.get_globbed_files(raw_folder)
	samplefile1 = raw_folder / "170120" / "SampleSheet.csv"
	samplefile2 = raw_folder / "181017" / "SampleSheet.csv"
	assert len(result) == 2
	assert samplefile1 in result
	assert samplefile2 in result


def test_read_index(raw_folder, indexed_finder):
	result = indexed_finder.read_index()

	expected_indexed_files = [
		raw_folder / "170120" / "SampleSheet.csv",
		raw_folder / "150311" / "sampleSheet.csv"
	]

	assert result == expected_indexed_files

@pytest.mark.parametrize("filename", SAMPLES)
def test_read_samplesheet(filename, empty_finder):
	header_result, content_result = empty_finder.read_samplesheet(filename)

	assert header_result[0] == dict(zip(empty_finder.fieldnames, "[Header],,,,,,,,,".split(',')))
	assert header_result[1] == dict(zip(empty_finder.fieldnames, "FileVersion,1,,,,,,,,".split(',')))
	assert header_result[2] == dict(zip(empty_finder.fieldnames, "LibraryPrepKit,Nextera DNA,,,,,,,,".split(',')))
	assert header_result[3] == dict(zip(empty_finder.fieldnames, "ContainerType,Plate96,,,,,,,,".split(',')))

	# Very simple check since it's parametrized.
	assert len(content_result) > 0

def test_organize_samplesheets(empty_finder):
	expected = {
		'2017-01-20': [SAMPLES[0]],
		'2018-10-17': [SAMPLES[1]],
		'2019-05-19': [SAMPLES[2]]
	}

	result = empty_finder.organize_samplesheets(SAMPLES)

	assert result == expected


def test_nonindexed_run(raw_folder, empty_finder):
	empty_finder.run(raw_folder)
	output_folder = raw_folder.parent / "output"

	expected_filelist = output_folder / "filelist.txt"
	assert expected_filelist.exists()
	assert expected_filelist.stat().st_size > 0


	expected_files_folder = output_folder / "files"
	# Make sure the expected generated samplesheets were made.
	expected_file_1 = expected_files_folder / "2017-01-20.SampleSheet.csv"
	expected_file_2 = expected_files_folder / "2018-10-17.SampleSheet.csv"
	assert expected_file_1.exists()
	assert expected_file_1.stat().st_size > 0
	assert expected_file_2.exists()
	assert expected_file_2.stat().st_size > 0

def test_save_organized_files(tmp_path, empty_finder):
	organized_samplesheets = {
		'2017-01-20': [SAMPLES[0]],
		'2018-10-17': [SAMPLES[1]],
		'2019-05-19': [SAMPLES[2]]
	}
	output_folder = tmp_path / "files"
	if not output_folder.exists(): output_folder.mkdir()
	empty_finder.write_organized_samplesheets(organized_samplesheets, output_folder)

	expected_output1 = output_folder / "2017-01-20.SampleSheet.csv"
	expected_output2 = output_folder / "2018-10-17.SampleSheet.csv"
	expected_output3 = output_folder / "2019-05-19.SampleSheet.csv"

	assert expected_output1.exists()
	assert expected_output1.stat().st_size > 0
	assert expected_output2.exists()
	assert expected_output2.stat().st_size > 0
	assert expected_output3.exists()
	assert expected_output3.stat().st_size > 0


