// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::metapb::Region;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use hex;
// Each range in a region will have a start key, end key, and a guard_value
#[derive(Clone, Debug)]
struct RangeGuard {
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    guard_value: String,
}

// Globally accessible map: region_id -> vector of RangeGuard
static REGION_TO_GUARD_MAP: Lazy<DashMap<u64, Vec<RangeGuard>>> = Lazy::new(DashMap::new);

/// Helper function to print the entire REGION_TO_GUARD_MAP.
pub fn print_region_guard_map() {
    info!("Printing REGION_TO_GUARD_MAP:");

    for entry in REGION_TO_GUARD_MAP.iter() {
        let region_id = entry.key();
        let guards = entry.value();

        let mut printed = false;
        for guard in guards.iter() {
            if guard.guard_value != "default_guard" {
                if !printed {
                    info!("Region ID: {}", region_id);
                    printed = true;
                }
                info!(
                    "  - Guard: start_key={}, end_key={}, guard_value={}",
                    hex::encode_upper(&guard.start_key),
                    hex::encode_upper(&guard.end_key),
                    guard.guard_value
                );
            }
        }
    }
}


/// Creates or updates a guard covering the entire region.
///
/// This *replaces* any existing range definitions for the given `region_id`
/// with a single guard that covers the whole region.
pub fn update_region_guard(region_id: u64, guard_value: String) {
    print_region_guard_map();
    info!(
        "Updating region guard (entire region): region_id={}, guard_value={}",
        region_id, guard_value
    );

    let entire_guard = RangeGuard {
        start_key: Vec::new(),
        end_key: Vec::new(),
        guard_value: guard_value.clone(),
    };

    // SAFER CONCURRENCY: Do this in one atomic operation
    REGION_TO_GUARD_MAP
        .entry(region_id)
        .and_modify(|rg_vec| {
            // "Original logic": clear existing, then push the new guard
            rg_vec.clear();
            rg_vec.push(entire_guard.clone());
        })
        .or_insert_with(|| {
            // If no entry, create a fresh Vec with our guard
            vec![entire_guard]
        });

    info!(
        "Finished updating region guard (entire region): region_id={}, guard_value={}",
        region_id, guard_value
    );
    print_region_guard_map();
}

/// Returns true if the two RangeGuards [start, end) overlap.
/// An empty `end_key` is considered "infinite".
fn ranges_overlap(a: &RangeGuard, b: &RangeGuard) -> bool {
    let a_has_infinite_end = a.end_key.is_empty();
    let b_has_infinite_end = b.end_key.is_empty();

    // Overlap condition (1D interval logic):
    //  (a_start < b_end || b_end is infinite) AND
    //  (b_start < a_end || a_end is infinite)

    if !b_has_infinite_end && a.start_key >= b.end_key {
        return false;
    }
    if !a_has_infinite_end && b.start_key >= a.end_key {
        return false;
    }

    info!(
        "DROP overlap guard: start_key={}, end_key={}, guard_value={}",
        hex::encode_upper(&a.start_key),
        hex::encode_upper(&a.end_key),
        a.guard_value
    );

    true
}

/// Removes any RangeGuards in `rg_vec` that overlap with the newly updated guard at `main_idx`.
fn remove_overlaps(rg_vec: &mut Vec<RangeGuard>, main_idx: usize) {
    // 1. Remove the newly updated guard from rg_vec.
    //    This avoids accidentally removing it in the retain() step.
    let updated_guard = rg_vec.remove(main_idx);

    // 2. Retain only those RangeGuards that do NOT overlap.
    rg_vec.retain(|g| !ranges_overlap(&updated_guard, g));

    // 3. Re-insert the updated guard.
    rg_vec.push(updated_guard);
}

/// Updates the region with a partial range guard based on `guard_value` prefix.
/// - If guard_value starts with "START_", sets a new or existing start_key.
/// - If guard_value starts with "END_", sets the end_key for the most recently added or matched guard.
/// - Otherwise, resets the entire region using `update_region_guard`.
pub fn update_region_guard_with_key(region_id: u64, guard_value: String, key: Vec<u8>) {
    print_region_guard_map();
    info!(
        "Updating region guard with key: region_id={}, guard_value={}, key={}",
        region_id,
        guard_value,
        hex::encode_upper(&key)
    );

    let mut remove_overlap = true;
    if let Some(mut stripped_value) = guard_value.strip_prefix("START_") {
        // Modify existing range or insert a new vector if none
        
        // If START_1234_END; this means 
        if let Some(new_value) = stripped_value.strip_suffix("_END") {
            stripped_value = new_value;
            remove_overlap = false;
        }
        
        REGION_TO_GUARD_MAP
            .entry(region_id)
            .and_modify(|rg_vec| {
                // Find an existing guard with same guard_value
                
                if let Some(idx) = rg_vec.iter().position(|rg| rg.guard_value == stripped_value) {
                    // Now we have the index of the existing guard
                    rg_vec[idx].start_key = key.clone();
                    info!(
                        "Reused existing START RangeGuard: region_id={}, guard_value={}, \
                         new start_key={}, end_key={}",
                        region_id,
                        stripped_value,
                        hex::encode_upper(&key),
                        hex::encode_upper(&rg_vec[idx].end_key),
                    );
                    if remove_overlap {
                        remove_overlaps(rg_vec, idx);
                    }
                } else {
                    // No guard with the same guard_value found; create a new one
                    rg_vec.push(RangeGuard {
                        start_key: key.clone(),
                        end_key: Vec::new(),
                        guard_value: stripped_value.to_owned(),
                    });
                    // The new RangeGuard is always pushed to the last
                    let idx_new = rg_vec.len() - 1;
                    info!(
                        "Added START RangeGuard: region_id={}, guard_value={}, start_key={}",
                        region_id,
                        stripped_value,
                        hex::encode_upper(&key)
                    );
                    if remove_overlap {
                        remove_overlaps(rg_vec, idx_new);
                    }
                }
            })
            .or_insert_with(|| {
                // If the region_id doesn't exist yet, create a new guard vector
                vec![RangeGuard {
                    start_key: key.clone(),
                    end_key: Vec::new(),
                    guard_value: stripped_value.to_owned(),
                }]
            });
    } else if let Some(stripped_value) = guard_value.strip_prefix("END_") {
        REGION_TO_GUARD_MAP
            .entry(region_id)
            .and_modify(|rg_vec| {
                // Search from the end to find the last matching guard_value
                if let Some(idx) = rg_vec.iter().position(|rg| rg.guard_value == stripped_value) {
                    // Now we have the index of the existing guard
                    rg_vec[idx].end_key = key.clone();

                    info!(
                        "Matched END RangeGuard: region_id={}, guard_value={}, start_key={}, end_key={}",
                        region_id,
                        stripped_value,
                        hex::encode_upper(&rg_vec[idx].start_key),
                        hex::encode_upper(&key),
                    );
                    remove_overlaps(rg_vec, idx);
                } else {
                    warn!(
                        "No existing RangeGuard found for region_id={} matching guard_value='{}' \
                         to set END key!",
                        region_id,
                        stripped_value
                    );
                }
            })
            // If the region didn't exist at all, we insert a new guard
            .or_insert_with(|| {
                vec![RangeGuard {
                    start_key: Vec::new(),
                    end_key: key.clone(),
                    guard_value: stripped_value.to_owned(),
                }]
            });
    } else {
        // If guard_value does not begin with "START_" or "END_", call update_region_guard 
        // to make the guard_value cover the entire region.
        warn!(
            "Guard value must begin with 'START_' or 'END_': got {}. Resetting entire region.",
            guard_value
        );
        update_region_guard(region_id, guard_value);
    }
}

/// Retrieve the guard_value covering a given `key` in a particular `region_id`.
pub fn get_region_guard_for_key(region_id: u64, key: &[u8]) -> Option<String> {
    info!(
        "get_region_guard_for_key, region_id={}, key={}",
        region_id,
        hex::encode_upper(key)
    );
    print_region_guard_map();

    // Read access is also concurrency-safe; we get a read lock for region_id.
    let rg_vec = match REGION_TO_GUARD_MAP.get(&region_id) {
        Some(r) => r,
        None => {
            warn!(
                "Region {} not found in REGION_TO_GUARD_MAP, key={}",
                region_id,
                hex::encode_upper(key)
            );
            return None;
        }
    };

    // The "original logic": find the first range in which key fits
    let mut matched_guards = Vec::new();

    for range_guard in rg_vec.iter() {
        let start_empty = range_guard.start_key.is_empty();
        let end_empty = range_guard.end_key.is_empty();
    
        let in_range_start = start_empty || key >= range_guard.start_key.as_slice();
        let in_range_end = end_empty || key <= range_guard.end_key.as_slice();
    
        if in_range_start && in_range_end {
            info!(
                "Key {} is in range [{}, {}] for region_id={}, guard_value={}",
                hex::encode_upper(key),
                hex::encode_upper(&range_guard.start_key),
                hex::encode_upper(&range_guard.end_key),
                region_id,
                range_guard.guard_value
            );
            matched_guards.push(range_guard.guard_value.clone());
        }
    }
    
    // Join matched guard values into a comma-separated string
    if !matched_guards.is_empty() {
        return Some(matched_guards.join(","));
    }

    warn!(
        "No matching guard found for region_id={}, key={}",
        region_id,
        hex::encode_upper(key)
    );
    return Some("NoExistingGuard".to_string());
}

/// Concatenates all guard values for a given `region_id` into a single comma-separated string.

pub fn get_region_guard(region_id: u64) -> Option<String> {
    print_region_guard_map();
    
    match REGION_TO_GUARD_MAP.get(&region_id) {
        Some(range_guards) => {
            let all_guards = range_guards
                .iter()
                .map(|rg| {
                    let start_key_hex = if rg.start_key.is_empty() {
                        ":".to_string()
                    } else {
                        hex::encode_upper(&rg.start_key)
                    };
                    let end_key_hex = if rg.end_key.is_empty() {
                        ":".to_string()
                    } else {
                        hex::encode_upper(&rg.end_key)
                    };
                    format!("{}({},{})", rg.guard_value, start_key_hex, end_key_hex)
                })
                .collect::<Vec<_>>()
                .join(",");

            info!(
                "Retrieved all guard values for region_id={}: {}",
                region_id, all_guards
            );
            Some(all_guards)
        }
        None => {
            warn!("Region {} not found in REGION_TO_GUARD_MAP", region_id);
            None
        }
    }
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
