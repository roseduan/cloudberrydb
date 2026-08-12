-- Shared cleanup for time_series regression tests.
-- Include this at the bottom of every test file via:
--   \ir include/cleanup.sql

RESET timezone;
RESET optimizer;
RESET datestyle;
RESET extra_float_digits;
