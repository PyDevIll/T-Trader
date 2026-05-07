# t_scheduler.py

from t_services import (
	StreamMonitor,
	AccountManagerSandbox,
	OrderManagerSandbox,
	instruments_by_filter,
	etf_ticker_to_figi,
	share_ticker_to_figi
)
from datetime import datetime, now
import pandas as pd
from enum import Enum

# datetime.weekday() = 0..6
weekday_str = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

class Weekday(Enum):
	Mon = 0
	Tue = 1
	Wed = 2
	Thu = 3
	Fri = 4
	Sat = 5
	Sun = 6

# fill-in figis
# check tickers for trading availablility
schedule_tab_shares = {
	"4":
		{
			"Mon": ["ABRD", "FIXR", "MBNK", "MRKP", "MRKU", "MSRS", "MSTT"],
			"Tue": ["ABIO", "ABRD", "DATA", "DOMRF", "ELFV", "EUTR", "GEMC", "LENT", "LSRG", "MDMG", "MRKP". "MSRS", "MVID"],
			"Wed": ["ABRD", "ASTR", "CBOM", "DATA", "DOMRF", "FIXR", "LENT", "MDMG", "MRKP", "MSRS", "OGKB"],
			"Thu": ["ABRD", "APTK", "ASTR", "BSPB", "CBOM", "DATA", "DOMRF", "GEMC", "GMKN", "LENT", "MDMG", "MRKU", "MSRS", "OGKB"],
			"Fri": ["BANE", "DOMRF", "ELFV", "LENT", "MDMG", "MRKP", "MRKU", "MSNG", "MSRS"],
			"Sat": ["BANE"],
			"Sun": []
		},
	"5":  { "Mon": [],						"Tue": ["ABRD"],		"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"6":  { "Mon": [],						"Tue": ["ABRD"],		"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"7":
	{
		"Mon": ["ABIO", "APTK", "MSTT", "NKHP", "NMTP"],
		"Tue": [],
		"Wed": ["ABRD", "BLNG", "MSRS"],
		"Thu": [],
		"Fri": [],
		"Sat": [],
		"Sun": []
	},
	"8":  { "Mon": ["LIFE"],				"Tue": [],				"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"9":  { "Mon": [],						"Tue": [],				"Wed": [],				"Thu": ["LENT"],		"Fri": [],				"Sat": [],				"Sun": []},
	"15": { "Mon": ["AKRN"],				"Tue": ["AKRN", "EUTR"],"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"16":
	{
		"Mon": ["DOMRF","LIFE", "MRKU", "MRKZ"],
		"Tue": ["LIFE", "MRKZ"],
		"Wed": ["BLNG", "LIFE"],
		"Thu": ["LIFE", "MRKU", "MRKZ"],
		"Fri": ["LIFE"],
		"Sat": ["DOMRF"],
		"Sun": ["MRKZ", "NKHP"]
	},
	"17": { "Mon": [],						"Tue": ["AKRN"],		"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"19": { "Mon": [],						"Tue": [],				"Wed": [],				"Thu": ["MRKZ"],		"Fri": [],				"Sat": [],				"Sun": []},
	"21":
	{
		"Mon": ["MSNG", "NKHP", "NMTP"],
		"Tue": ["BLNG", "NMTP"],
		"Wed": ["BLNG", "NKHP", "NMTP"],
		"Thu": ["NKHP"],
		"Fri": ["HYDR", "MTLR", "NKHP"],
		"Sat": ["MVID", "NKHP", "OGKB"],
		"Sun": []
	},
	"22": { "Mon": ["MVID"],				"Tue": [],				"Wed": ["MSRS"],		"Thu": [],				"Fri": ["CBOM"],		"Sat": ["MSNG"],		"Sun": []},
}
schedule_tab_etfs = {
	"4":  { "Mon": [],				"Tue": [],				"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"5":  { "Mon": [],				"Tue": [],				"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"6":  { "Mon": [],				"Tue": [],				"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"7":  { "Mon": [],				"Tue": [],				"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"8":  { "Mon": [],				"Tue": [],				"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"9":  { "Mon": [],				"Tue": [],				"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"15": { "Mon": [],				"Tue": [],				"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"16": { "Mon": [],				"Tue": [],				"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"17": { "Mon": [],				"Tue": [],				"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"21": { "Mon": [],				"Tue": [],				"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"22": { "Mon": [],				"Tue": [],				"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
}
schedule = {
	"ABIO": {
		"ticker": "ABIO", "figi": "",
		"times": [
 			{"hour": 7, "weekday": "Mon"},
 			{"hour": 4,	"weekday": "Tue"},
		],
	},
	"ABRD": {
		"ticker": "ABRD", "figi": "",
		"times": [
 			{"hour": 4, "weekday": "Mon"},
 			{"hour": 4, "weekday": "Tue"}, # continuous
 			{"hour": 5, "weekday": "Tue"}, #
 			{"hour": 6, "weekday": "Tue"}, #
 			{"hour": 4, "weekday": "Wed"},
 			{"hour": 7, "weekday": "Wed"},
 			{"hour": 4, "weekday": "Thu"},
		],
	},
	"AKRN": {
		"ticker": "AKRN", "figi": "",
		"times": [
 			{"hour": 15, "weekday": "Mon"},
 			{"hour": 15, "weekday": "Tue"},
 			{"hour": 17, "weekday": "Tue"},
		],
	},
	"APTK": {
		"ticker": "ATPK", "figi": "",
		"times": [
 			{"hour": 7, "weekday": "Mon"},
 			{"hour": 4, "weekday": "Thu"},
		],
	},
	"ASTR": {
		"ticker": "ASTR", "figi": "",
		"times": [
 			{"hour": 4, "weekday": "Wed"},
 			{"hour": 4, "weekday": "Thu"},
		],
	},
	"BANE": {
		"ticker": "BANE", "figi": "",
		"times": [
 			{"hour": 4, "weekday": "Fri"},
 			{"hour": 4, "weekday": "Sat"},
		],
	},
	"BLNG": {
		"ticker": "BLNG", "figi": "",
		"times": [
 			{"hour": 21, "weekday": "Tue"},
 			{"hour": 7, "weekday": "Wed"},
 			{"hour": 16, "weekday": "Wed"},
 			{"hour": 21, "weekday": "Wed"},
		],
	},
	"BSPB": {
		"ticker": "BSPB", "figi": "",
		"times": [
 			{"hour": 4, "weekday": "Thu"},
		],
	},
	"CBOM": {
		"ticker": "CBOM", "figi": "",
		"times": [
 			{"hour": 4, "weekday": "Wed"},
 			{"hour": 4, "weekday": "Thu"},
 			{"hour": 22, "weekday": "Fri"},
		],
	},
	"DATA": {
		"ticker": "DATA", "figi": "",
		"times": [
 			{"hour": 4, "weekday": "Tue"},
 			{"hour": 4, "weekday": "Wed"},
 			{"hour": 4, "weekday": "Thu"},
		],
	},
	"DOMRF": {
		"ticker": "DOMRF", "figi": "",
		"times": [
 			{"hour": 4, "weekday": "Tue"},
 			{"hour": 4, "weekday": "Wed"},
 			{"hour": 4, "weekday": "Thu"},
 			{"hour": 4, "weekday": "Fri"},
 			{"hour": 16, "weekday": "Tue"},
 			{"hour": 16, "weekday": "Sat"},
		],
	},
	"ELFV": {
		"ticker": "ELFV", "figi": "",
		"times": [
 			{"hour": 4, "weekday": "Tue"},
 			{"hour": 4, "weekday": "Fri"},
		],
	},
	"EUTR": {
		"ticker": "EUTR", "figi": "",
		"times": [
 			{"hour": 4, "weekday": "Tue"},
 			{"hour": 15, "weekday": "Tue"},
		],
	},
	"FIXR": {
		"ticker": "FIXR", "figi": "",
		"times": [
 			{"hour": 4, "weekday": "Mon"},
 			{"hour": 4, "weekday": "Wed"},
		],
	},
	"GEMC": {
		"ticker": "GEMC", "figi": "",
		"times": [
 			{"hour": 4, "weekday": "Tue"},
 			{"hour": 4, "weekday": "Thu"},
		],
	},
	"GMKN": {
		"ticker": "GMKN", "figi": "",
		"times": [
 			{"hour": 4, "weekday": "Thu"},
		],
	},
	"HYDR": {
		"ticker": "HYDR", "figi": "",
		"times": [
 			{"hour": 21, "weekday": "Fri"},
		],
	},
	"LENT": {
		"ticker": "LENT", "figi": "",
		"times": [
 			{"hour": 4, "weekday": "Tue"},
 			{"hour": 4, "weekday": "Wed"},
 			{"hour": 4, "weekday": "Thu"},
 			{"hour": 4, "weekday": "Fri"},
 			{"hour": 9, "weekday": "Thu"},
		],
	},
	"LIFE": {
		"ticker": "LIFE", "figi": "",
		"times": [
 			{"hour": 8, "weekday": "Mon"},
 			{"hour": 16, "weekday": "Mon"},
 			{"hour": 16, "weekday": "Tue"},
 			{"hour": 16, "weekday": "Wed"},
 			{"hour": 16, "weekday": "Thu"},
 			{"hour": 16, "weekday": "Fri"},
		],
	},

}

