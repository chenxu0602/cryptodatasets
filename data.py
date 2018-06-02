
import sys, os
import re, argparse, pytz, logging, logging.handlers
from collections import defaultdict

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def setup_logger(name="Logger", level=logging.INFO):
	print(f"Creating logger {name} with level {level} ...")
	logger = logging.getLogger(name)
	logger.setLevel(level)
	FORMAT = "%(asctime)s %(levelname)s %(message)s"
	logging.basicConfig(format=FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
	return logger

def cryptodatasets(root, products, freq, logger=None):
	results = defaultdict(pd.DataFrame)

	for prod in sorted(products):
		datadir = os.path.join(root, freq, prod)

		if not os.path.exists(datadir):
			logger.error(f"Cryptodatasets directory {datadir} doesn't exist!")
			continue

		files = os.listdir(datadir)
		frames = []

		for f in files:
			logger.info(f"Loading {f} ...")
			filename = os.path.join(datadir, f)
			df = pd.read_csv(filename, index_col=[0], parse_dates=[0])
			frames.append(df)

		results[prod] = pd.concat(frames).sort_index()

	return results

def resample(data, freq, logger=None):
	results = defaultdict(pd.DataFrame)

	for prod in data:
		df = data[prod]

		if df.empty:
			logger.error(f"{prod} doesn't have data!")
			continue

		if not "price" in df.columns:
			logger.error(f"{prod} data doesn't have a price columns!")
			continue

		if not "amount" in df.columns:
			logger.error(f"{prod} data doesn't have a amount columns!")
			continue

		price = df.price
		amount = df.amount

		logger.info(f"Resampling {prod} data to {freq} ...")

		volume = amount.abs().resample(freq, label="left", closed="left").sum()
		op = price.resample(freq, label="left", closed="left").first()
		hi = price.resample(freq, label="left", closed="left").max()
		lo = price.resample(freq, label="left", closed="left").min()
		cl = price.resample(freq, label="left", closed="left").last()

		df = pd.DataFrame({"volume":volume, "open":op, "high":hi, "low":lo, "close":cl})
		results[prod] = df

	return results



if __name__ == "__main__":

	parser = argparse.ArgumentParser(prog="Crypto Data Loader", description="",
			formatter_class=argparse.ArgumentDefaultsHelpFormatter)

	parser.add_argument("--rootdir", nargs="?", type=str, default=".", dest="rootdir", help="root directory")
	parser.add_argument("--datadir", nargs="?", type=str, default="data", dest="datadir", help="data directory")
	parser.add_argument("--products", nargs="*", type=str, default=[], dest="products", help="products")
	parser.add_argument("--freq", nargs="?", type=str, default="tick", dest="freq", help="data frequency (1min|tick)")

	args = parser.parse_args()

	logger = setup_logger("crypto")

	rootdir = os.path.expandvars(args.rootdir)
	if not os.path.exists(rootdir):
		logger.critical(f"Root directory {rootdir} doesn't exist!")
		sys.exit(1)

	datadir = os.path.join(rootdir, args.datadir)
	if not os.path.exists(datadir):
		logger.critical(f"Data directory {datadir} doesn't exist!")
		sys.exit(1)


	data = cryptodatasets(datadir, args.products, args.freq, logger)

	if args.freq == "tick":
		data1min = resample(data, "1T", logger)
