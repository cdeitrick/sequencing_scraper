from pathlib import Path
import time
from loguru import logger
import scraper
import datetime
def checkdir(path)->Path:
	if not path.exists():
		path.mkdir()
	return path

class Scheduler:
	def __init__(self, wait_time:int = 3600*24, update_interval = 3600):
		self.wait_time = wait_time
		self.update_interval = update_interval # Time between check as to whether it is time to run the scraper again.
		parent_config_folder = checkdir(Path("/home/cld100/github/config"))

		self.config_folder = checkdir(parent_config_folder / "beagle_sync_config_data")
		self.index_filename = self.config_folder / "indexed.samplesheets.txt"
		self.beagle_sync_output_files = checkdir(self.config_folder / "beagle_sync_files")

	def run(self, source_folder:Path):

		while True:
			logger.info("Starting the scheduled scraping process...")
			start = time.time()
			#self.run_syncer(source_folder)
			self.run_scraper(source_folder)

			while True:
				duration = time.time() - start
				remaining = self.wait_time - duration
				logger.info(f"Sleeping for {self.wait_time} seconds, {remaining:.1f} seconds remain.")
				if duration >= self.wait_time:
					break
				time.sleep(self.update_interval)

	def run_scraper(self, source_folder:Path):
		now = datetime.datetime.now()
		output_folder = self.beagle_sync_output_files / f"{now.year}-{now.month}-{now.day}"
		finder = scraper.SampleSheetFinder(self.index_filename, output_folder)
		finder.run(source_folder)


if __name__ == "__main__":
	scheduler = Scheduler()
	scheduler.run(Path("/home/data/raw/"))