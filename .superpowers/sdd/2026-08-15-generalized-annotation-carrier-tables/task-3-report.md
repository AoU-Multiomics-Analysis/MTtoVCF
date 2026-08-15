# Task 3 Report: WDL Integration for Generalized Carrier Tables

Date: 2026-08-15

## Scope

Implemented the Task 3 WDL integration portion of the approved
carrier-table generalization directly in the current repository workspace.
This change propagates splice acceptor and splice donor carrier outputs
through the standalone utility WDL, the Hail utility WDL, and `main.wdl`
without changing existing LoF output names, LoF output schemas, or the
default `MakeLoFCarriers = false` behavior.

## Changed Files

- `workflow/LoFCarrierTable.wdl`
  - Renamed local temporary region/map filenames to generalized names.
  - Added workflow outputs `SpliceAcceptorCarriers` and
    `SpliceDonorCarriers`.
  - Added task outputs for:
    - `~{output_prefix}.splice_acceptor_carriers.tsv.gz`
    - `~{output_prefix}.splice_donor_carriers.tsv.gz`
- `workflow/HailLoFCarrierTable.wdl`
  - Added workflow outputs `SpliceAcceptorCarriers` and
    `SpliceDonorCarriers`.
  - Added task outputs reading:
    - `splice_acceptor_carriers_outpath.txt`
    - `splice_donor_carriers_outpath.txt`
- `main.wdl`
  - Propagated optional outputs:
    - `File? SpliceAcceptorCarriers = HailLoFCarriers.SpliceAcceptorCarriers`
    - `File? SpliceDonorCarriers = HailLoFCarriers.SpliceDonorCarriers`
- `tests/test_filter_and_write_mt_contract.py`
  - Added WDL contract coverage for standalone WDL output exposure,
    Hail WDL outpath wiring, and `main.wdl` propagation.

## Commits

- `2d03509` `feat: expose generalized carrier table outputs`

## Test-First Workflow

1. Added failing WDL contract assertions in
   `tests/test_filter_and_write_mt_contract.py`.
2. Ran the named contract test before WDL edits and confirmed failure on the
   new Task 3 expectations only.
3. Updated the WDLs.
4. Re-ran the contract test and `miniwdl check` commands to green.

## Commands and Results

- `python3 -m unittest tests.test_filter_and_write_mt_contract -v`
  - Before WDL edits: FAIL
  - New failures:
    - `test_lof_carrier_workflow_exposes_generalized_outputs`
    - `test_hail_lof_workflow_exposes_generalized_outputs`
    - `test_main_workflow_propagates_generalized_carrier_outputs`
- `python3 -m unittest tests.test_filter_and_write_mt_contract -v`
  - After WDL edits: PASS
  - Result: `Ran 18 tests ... OK`
- `miniwdl check workflow/LoFCarrierTable.wdl`
  - PASS
- `miniwdl check workflow/HailLoFCarrierTable.wdl`
  - PASS
- `miniwdl check main.wdl`
  - PASS

## Self-Review

- Confirmed existing LoF outputs remain unchanged:
  - `~{output_prefix}.lof_carriers.HC.tsv.gz`
  - `~{output_prefix}.lof_carriers.HC_or_LC.tsv.gz`
- Confirmed `main.wdl` keeps `MakeLoFCarriers = false`.
- Confirmed Hail utility WDL reuses the existing post-hoc VCF indexing flow.
- Confirmed no README changes were made.

## Concerns

- No functional concerns found in the WDL/output propagation itself.
- Process concern: this session did not expose a reviewer-subagent interface,
  so the final review step was completed as a local read-only diff
  self-review instead of a dispatched reviewer pass.
