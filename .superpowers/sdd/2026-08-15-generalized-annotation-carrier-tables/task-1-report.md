# Task 1 Implementation Report

## Changed Files

- `scripts/extract_lof_carriers.py`
- `tests/test_extract_lof_carriers.py`

## Commit(s)

- `985f9b2` - `feat: generalize standalone annotation carrier extraction`

## Tests

- `python3 -m unittest tests.test_extract_lof_carriers.ExtractLoFCarriersTests.test_writes_grouped_variant_map -v`
  - Result: PASS
- `python3 -m unittest tests.test_extract_lof_carriers.ExtractLoFCarriersTests.test_writes_separate_group_carrier_tables -v`
  - Result: PASS
- `python3 -m unittest tests/test_extract_lof_carriers.py -v`
  - Result: PASS

## Notes

- `write-sites` now emits a grouped variant map with `group` and `annotation` columns plus the union region set across LoF and splice groups.
- `collect-carriers` now writes the legacy LoF carrier tables plus `splice_acceptor` and `splice_donor` carrier tables.
- The compatibility `parse_transcript_annotations` wrapper still returns the old LoF-shaped structure for direct callers.

## Concerns

- None at this time.
