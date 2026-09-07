# test_t_log_rotation.py
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "t_trader"))

import t_order_manager as m


def test_log_rotates_past_size_limit(tmp_path, monkeypatch):
	log_file = tmp_path / "t_trader.log"
	monkeypatch.setattr(m, "LOG_FILE", str(log_file))
	monkeypatch.setattr(m, "MAX_LOG_BYTES", 200)
	monkeypatch.setattr(m, "LOG_BACKUP_COUNT", 2)

	for _ in range(80):
		m.log("x" * 100)

	# the current file exists and never stays more than a line above the cap
	assert os.path.exists(str(log_file))
	assert os.path.getsize(str(log_file)) <= 200 + 140
	# ...and the oversized content rolled into backups
	assert os.path.exists(str(log_file) + ".1")


def test_log_rotation_keeps_only_newest_backups(tmp_path, monkeypatch):
	log_file = tmp_path / "t_trader.log"
	monkeypatch.setattr(m, "LOG_FILE", str(log_file))
	monkeypatch.setattr(m, "MAX_LOG_BYTES", 150)
	monkeypatch.setattr(m, "LOG_BACKUP_COUNT", 2)

	for _ in range(120):
		m.log("y" * 100)

	# only the capped number of backups survives (no .3)
	assert os.path.exists(str(log_file) + ".1")
	assert os.path.exists(str(log_file) + ".2")
	assert not os.path.exists(str(log_file) + ".3")


def test_log_rotation_small_file_stays_unrotated(tmp_path, monkeypatch):
	log_file = tmp_path / "t_trader.log"
	monkeypatch.setattr(m, "LOG_FILE", str(log_file))
	monkeypatch.setattr(m, "MAX_LOG_BYTES", 10 * 1024 * 1024)
	monkeypatch.setattr(m, "LOG_BACKUP_COUNT", 2)

	m.log("short message")

	assert os.path.exists(str(log_file))
	assert not os.path.exists(str(log_file) + ".1")
