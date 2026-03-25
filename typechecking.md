# Typechecking progress (`ty check`)

## Baseline
- Command: `uv run --group lint ty check`
- Initial diagnostics: 445
- First major clusters observed:
  - `mismo/types/_updates.py`
  - `mismo/types/_links_table.py`
  - `mismo/vector/_vector.py`
  - tests under `mismo/types/tests/`

## Notes
- `ty` is run via `uv run --group lint ty check` (not directly from shell PATH).
- Some test files intentionally define runtime-invalid type signatures to verify wrapper dispatch behavior; these should likely use targeted ignore comments.

## Completed
- Fixed `mismo/types/_links_table.py` selector argument typing to align with ibis `select` usage.
- Fixed `mismo/types/_linked_table.py` selector argument typing for `with_linked_values`.
- Fixed `mismo/types/_updates.py` null narrowing, struct field handling, and return typing.
- Added targeted test-only `ty: ignore` comments in:
  - `mismo/types/tests/test_union.py`
  - `mismo/types/tests/test_wrapper.py`
- Fixed `mismo/vector/_vector.py` type signatures/casts around map and array operations.
- Fixed `mismo/compare/_match_level.py` metaclass/class typing, narrowing, and comparer-case typing.
- Added targeted test-only `ty: ignore` comments and expression adjustments in:
  - `mismo/compare/tests/test_match_level.py`
- Fixed `mismo/linker/_or_linker.py` internal mapping/boolean typing.

## Progress
- Baseline: 445 diagnostics.
- After first fixes: 416 diagnostics.
- After vector fixes: 406 diagnostics.
- After match-level and OrLinker fixes: 333 diagnostics.

## Next
- Re-run full `ty check` and record updated count.
- Triage next high-count files from `/tmp/mismo-ty.txt`, currently led by:
  - `mismo/lib/geo/_address.py`
  - `mismo/lib/name/_compare.py`
  - `mismo/lib/email/_core.py`
