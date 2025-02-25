# Changelog

---

## 0.1.7 - UNRELEASED

-

## 0.1.6

- Add support for new Hex API parameters in `run_project`: `dry_run`, `update_published_results`, and `view_id`
- Improve deprecation notice for `update_cache` to clarify both alternatives
- Add comprehensive test coverage for new parameters

## 0.1.5

- Hex project asset definition with `build_hex_asset` function using the project_id
- Update to hex resource to sync with new hex API updates to deprecate `update_cache` and add `use_cached_sql` instead.

## 0.1.4

- Mark `hex_resource` with deprecation warning and recommendation to use `HexResource`
- `HexResource` has been refactored from instantiation a `hex_resource` function to a `ConfigurableResource`

## 0.1.3

- Support for notifications

## 0.1.2

- Don't sell input parameters when they are null

## 0.1.0
