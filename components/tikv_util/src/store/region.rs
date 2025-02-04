// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::metapb::Region;
use dashmap::DashMap;
use once_cell::sync::Lazy;
// Each range in a region will have a start key, end key, and a guard_value
#[derive(Clone, Debug)]
struct RangeGuard {
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    guard_value: String,
}

// Globally accessible map: region_id -> vector of RangeGuard
static REGION_TO_GUARD_MAP: Lazy<DashMap<u64, Vec<RangeGuard>>> = Lazy::new(DashMap::new);

/// Creates or updates a guard covering the entire region.
///
/// This will *replace* any existing range definitions for the given region_id
/// with a single guard that covers "everything" (conceptually).
pub fn update_region_guard(region_id: u64, guard_value: String) {
    info!(
        "Updating region guard (entire region): region_id={}, guard_value={}",
        region_id, guard_value
    );

    // Create a single RangeGuard that covers the entire region.
    // Here, we use empty start and end keys to mean "covers everything".
    // Adjust the logic to match your actual notion of "entire region".
    let entire_guard = RangeGuard {
        start_key: Vec::new(),
        end_key: Vec::new(),
        guard_value: guard_value.clone(),
    };

    let mut rg_vec = REGION_TO_GUARD_MAP.entry(region_id).or_default();
    rg_vec.clear();
    rg_vec.push(entire_guard);
}

/// Updates the region with a partial range guard based on `guard_value` prefix.
///
/// If `guard_value` starts with "START", we interpret it as setting a new range with
/// the given `key` as its start_key (and empty end_key for now).
///
/// If `guard_value` starts with "END", we interpret it as setting the end_key for
/// the most recently added range guard (that presumably has an empty `end_key`).
///
/// You can adjust/extend this logic as needed.
pub fn update_region_guard_with_key(region_id: u64, guard_value: String, key: Vec<u8>) {
    info!(
        "Updating region guard with key: region_id={}, guard_value={}, key={:?}",
        region_id, guard_value, key
    );

    let mut rg_vec = REGION_TO_GUARD_MAP.entry(region_id).or_default();

    if let Some(stripped_value) = guard_value.strip_prefix("START_") {
        // Search the list of RangeGuards for the same guard_value
        match rg_vec.iter_mut().find(|rg| rg.guard_value == stripped_value) {
            Some(existing_guard) => {
                // Reuse the existing guard
                existing_guard.start_key = key.clone();
                info!(
                    "Reused existing START range guard: region_id={}, guard_value={}, new start_key={:?}, end_key={:?}",
                    region_id, stripped_value, key, existing_guard.end_key
                );
            }
            None => {
                // No guard with the same guard_value found; create a new one
                rg_vec.push(RangeGuard {
                    start_key: key.clone(),
                    end_key: Vec::new(),
                    guard_value: stripped_value.to_owned(),
                });
                info!(
                    "Added START range guard: region_id={}, guard_value={}, start_key={:?}",
                    region_id, stripped_value, key
                );
            }
        }
    } else if let Some(stripped_value) = guard_value.strip_prefix("END_") {
        // Find the *last* RangeGuard in rg_vec that has the same guard_value and no end_key set
        // OR simply the same guard_value, depending on your business logic.
        // Here, we show an example of searching from the end to find the first match.
        match rg_vec.iter_mut().rev().find(|rg| rg.guard_value == stripped_value) {
            Some(guard) => {
                guard.end_key = key;
                info!(
                    "Matched END range guard: region_id={}, guard_value={}, start_key={:?}, end_key={:?}",
                    region_id, stripped_value, guard.start_key, guard.end_key
                );
            }
            None => {
                warn!(
                    "No existing RangeGuard found for region_id={} matching guard_value='{}' to set END key!",
                    region_id, stripped_value
                );
            }
        }
    } else {
        // If guard_value does not begin with "START_" or "END_", call update_region_guard 
        // to make the guard_value cover the entire region.
        warn!(
            "Guard value must begin with 'START_' or 'END_': got {}. Resetting entire region.",
            guard_value
        );
        // Call update_region_guard
        update_region_guard(region_id, guard_value);
    }
}

/// Concatenates all guard values for a given region_id into a single comma-separated string.
///
/// Returns `None` if the region_id is not found, or an empty string if the region exists but
/// has no guard entries.
pub fn get_region_guard(region_id: u64) -> Option<String> {
    match REGION_TO_GUARD_MAP.get(&region_id) {
        Some(range_guards) => {
            // Collect all guard_value fields into a vector of Strings, then join with commas
            let all_guards = range_guards
                .iter()
                .map(|rg| rg.guard_value.clone())
                .collect::<Vec<_>>()
                .join(",");

            info!(
                "Retrieved all guard values for region_id={}: {}",
                region_id, all_guards
            );

            // If you want to differentiate between "no entries" vs. "entries exist but are empty strings",
            // you can do so here. For now, we'll simply return the possibly-empty joined string.
            Some(all_guards)
        }
        None => {
            warn!("Region {} not found in REGION_TO_GUARD_MAP", region_id);
            None
        }
    }
}

/// Retrieve the guard_value covering a given `key` in a particular `region_id`.
///
/// If multiple ranges could match, you might need additional logic (e.g., the
/// first match, the most specific match, etc.). Here we simply return the first
/// matching range in the vector.
pub fn get_region_guard_for_key(region_id: u64, key: &[u8]) -> Option<String> {
    // Get the vector of RangeGuard for the region. If none, return None immediately.
    let rg_vec = match REGION_TO_GUARD_MAP.get(&region_id) {
        Some(r) => r,
        None => {
            warn!("Region {} not found in REGION_TO_GUARD_MAP", region_id);
            return None;
        }
    };

    // Find the first range that covers `key`.
    for range_guard in rg_vec.iter() {
        let start_empty = range_guard.start_key.is_empty();
        let end_empty = range_guard.end_key.is_empty();

        // Decide on how you interpret "empty" boundaries:
        // - If start_key is empty, treat it as "minus infinity" (start).
        // - If end_key is empty, treat it as "plus infinity" (end).
        let in_range_start = start_empty || key >= range_guard.start_key.as_slice();
        let in_range_end = end_empty || key < range_guard.end_key.as_slice();

        if in_range_start && in_range_end {
            info!(
                "Key {:?} is in range [{:?}, {:?}]) for region_id={}, guard_value={}",
                key,
                range_guard.start_key,
                range_guard.end_key,
                region_id,
                range_guard.guard_value
            );
            return Some(range_guard.guard_value.clone());
        }
    }

    warn!(
        "No matching guard found for region_id={} and key={:?}",
        region_id, key
    );
    None
}

/// Check if key in region range (`start_key`, `end_key`).
pub fn check_key_in_region_exclusive(key: &[u8], region: &Region) -> bool {
    let end_key = region.get_end_key();
    let start_key = region.get_start_key();
    start_key < key && (key < end_key || end_key.is_empty())
}

/// Check if key in region range [`start_key`, `end_key`].
pub fn check_key_in_region_inclusive(key: &[u8], region: &Region) -> bool {
    let end_key = region.get_end_key();
    let start_key = region.get_start_key();
    key >= start_key && (end_key.is_empty() || key <= end_key)
}

/// Check if key in region range [`start_key`, `end_key`).
pub fn check_key_in_region(key: &[u8], region: &Region) -> bool {
    let end_key = region.get_end_key();
    let start_key = region.get_start_key();
    key >= start_key && (end_key.is_empty() || key < end_key)
}

/// Check if guard matches region range [`start_key`, `end_key`).
pub fn check_guard_in_region(guard: String, region: &Region) -> bool {
    guard == region.get_guard_value()
}

/// Check if replicas of two regions are on the same stores.
pub fn region_on_same_stores(lhs: &Region, rhs: &Region) -> bool {
    if lhs.get_peers().len() != rhs.get_peers().len() {
        return false;
    }

    // Because every store can only have one replica for the same region,
    // so just one round check is enough.
    lhs.get_peers().iter().all(|lp| {
        rhs.get_peers().iter().any(|rp| {
            rp.get_store_id() == lp.get_store_id()
                && rp.get_role() == lp.get_role()
                && rp.get_is_witness() == lp.get_is_witness()
        })
    })
}

/// Check if the given region exists on stores, by checking whether any one of
/// the peers belonging to this region exist on the given stores.
pub fn region_on_stores(region: &Region, store_ids: &Vec<u64>) -> bool {
    if store_ids.is_empty() {
        return true;
    }
    // If one of peers in this region exists on any on in `store_ids`, it shows that
    // the region exists on the given stores.
    region.get_peers().iter().any(|p| {
        store_ids
            .iter()
            .any(|store_id| *store_id == p.get_store_id())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    // Tests the util function `check_key_in_region`.
    #[test]
    fn test_check_key_in_region() {
        let test_cases = vec![
            ("", "", "", true, true, false),
            ("", "", "6", true, true, false),
            ("", "3", "6", false, false, false),
            ("4", "3", "6", true, true, true),
            ("4", "3", "", true, true, true),
            ("3", "3", "", true, true, false),
            ("2", "3", "6", false, false, false),
            ("", "3", "6", false, false, false),
            ("", "3", "", false, false, false),
            ("6", "3", "6", false, true, false),
        ];
        for (key, start_key, end_key, is_in_region, inclusive, exclusive) in test_cases {
            let mut region = Region::default();
            region.set_start_key(start_key.as_bytes().to_vec());
            region.set_end_key(end_key.as_bytes().to_vec());
            let mut result = check_key_in_region(key.as_bytes(), &region);
            assert_eq!(result, is_in_region);
            result = check_key_in_region_inclusive(key.as_bytes(), &region);
            assert_eq!(result, inclusive);
            result = check_key_in_region_exclusive(key.as_bytes(), &region);
            assert_eq!(result, exclusive);
        }
    }
}
