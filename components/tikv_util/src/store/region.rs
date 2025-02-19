// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::metapb::Region;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use hex;
use std::cmp::Ordering;
use regex::Regex;

// Each range in a region will have a start key, end key, and a guard_value
#[derive(Clone, Debug)]
struct RangeGuard {
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    guard_value: String,
}

// Globally accessible map: region_id -> vector of RangeGuard
static REGION_TO_GUARD_MAP: Lazy<DashMap<u64, Vec<RangeGuard>>> = Lazy::new(DashMap::new);
// Precompile the regex pattern once.
static GUARD_REGEX: Lazy<Regex> = Lazy::new(|| {
    // guard_value must be alphanumeric, start_key and end_key are either ":" or hexadecimal.
    Regex::new(r"(?P<guard_value>[0-9A-Za-z]+)\((?P<start_key>:|[0-9A-Fa-f]+),(?P<end_key>:|[0-9A-Fa-f]+)\)").unwrap()
});

/// Compare two start keys, where an empty key is treated as -∞.
fn compare_start_keys(a: &[u8], b: &[u8]) -> Ordering {
    match (a.is_empty(), b.is_empty()) {
        (true, true) => Ordering::Equal,       // -∞ == -∞
        (true, false) => Ordering::Less,       // -∞ < anything
        (false, true) => Ordering::Greater,    // anything > -∞
        (false, false) => a.cmp(b),            // normal lexicographical compare
    }
}

/// Compare two end keys, where an empty key is treated as +∞.
fn compare_end_keys(a: &[u8], b: &[u8]) -> Ordering {
    match (a.is_empty(), b.is_empty()) {
        (true, true) => Ordering::Equal,       // +∞ == +∞
        (true, false) => Ordering::Greater,    // +∞ > anything
        (false, true) => Ordering::Less,       // anything < +∞
        (false, false) => a.cmp(b),            // normal lexicographical compare
    }
}

/// Return true if key is in [range_start, range_end).
// fn key_in_range(key: &[u8], range_start: &[u8], range_end: &[u8]) -> bool {
//     // range_start <= key < range_end
//     // using specialized comparisons:
//     compare_start_keys(range_start, key) != Ordering::Greater
//         && compare_end_keys(key, range_end) == Ordering::Less
// }


/// Return `true` if the [guard_start, guard_end) range is entirely within [region_start, region_end).
fn guard_in_region_range(guard: &RangeGuard, region_start: &[u8], region_end: &[u8]) -> bool {
    // region_start <= guard.start_key AND guard.end_key <= region_end
    (compare_start_keys(region_start, &guard.start_key) != Ordering::Greater)
        && (compare_end_keys(&guard.end_key, region_end) != Ordering::Greater)
}

/// Check if [guard_start, guard_end] overlaps with [region_start, region_end].
/// Returns (overlap_start, overlap_end) if there is an overlap, or None otherwise.
///
/// An empty `guard_start` or `region_start` is treated as -∞.
/// An empty `guard_end` or `region_end` is treated as +∞.
fn range_overlap(
    guard_start: &[u8],
    guard_end: &[u8],
    region_start: &[u8],
    region_end: &[u8],
) -> Option<(Vec<u8>, Vec<u8>)> {

    // overlap_start = max(guard_start, region_start) using compare_start_keys
    let overlap_start = match compare_start_keys(guard_start, region_start) {
        Ordering::Greater => guard_start,
        _ => region_start,
    };

    // overlap_end = min(guard_end, region_end) using compare_end_keys
    let overlap_end = match compare_end_keys(guard_end, region_end) {
        Ordering::Greater => region_end,
        _ => guard_end,
    };

    info!(
        "range_overlap: guard_start: {:?}, guard_end: {:?}, region_start: {:?}, region_end: {:?}, overlap_start: {:?}, overlap_end: {:?}",
        guard_start, guard_end, region_start, region_end, overlap_start, overlap_end,
    );

    // Now check if overlap_start <= overlap_end in the sense of end-key comparison
    // (empty => +∞). If overlap_start > overlap_end, no valid overlap.
    if compare_end_keys(overlap_start, overlap_end) != Ordering::Greater {
        Some((overlap_start.to_vec(), overlap_end.to_vec()))
    } else {
        None
    }
}

/// Merge old_region into new_region, but **only** those RangeGuards that fit
/// entirely within the new region range. Guards out of range are discarded.
pub fn handle_region_merge(
    old_region_id: u64,
    old_region_start_key: &[u8],
    old_region_end_key: &[u8],
    new_region_id: u64,
    new_region_start_key: &[u8],
    new_region_end_key: &[u8],
) {
    // Debug info
    info!(
        "handle_region_merge: old_region_id={}, new_region_id={}, \
         old_range=[{}, {}), new_range=[{}, {})",
        old_region_id,
        new_region_id,
        hex::encode_upper(old_region_start_key),
        hex::encode_upper(old_region_end_key),
        hex::encode_upper(new_region_start_key),
        hex::encode_upper(new_region_end_key)
    );

    // 1) Locate the old region's guards.
    let old_guards = match REGION_TO_GUARD_MAP.get(&old_region_id) {
        Some(guard_vec) => guard_vec.clone(),
        None => {
            info!(
                "No RangeGuards found for old_region_id={}, nothing to merge.",
                old_region_id
            );
            return;
        }
    };

    // // (Optional) Log a warning if the old region is not fully contained in the new region.
    // if compare_start_keys(old_region_start_key, new_region_start_key) == Ordering::Less
    //     || compare_end_keys(old_region_end_key, new_region_end_key) == Ordering::Greater
    // {
    //     info!("Old region is not fully contained in new region. Some guards may be out of range.");
    //     return;
    // }

    // 2) Build a list of only the guards that fall inside [new_region_start_key, new_region_end_key).
    let mut transferred_guards = Vec::new();
    let mut skipped_count = 0;
    for guard in &old_guards {
        if guard_in_region_range(guard, new_region_start_key, new_region_end_key) {
            transferred_guards.push(guard.clone());
        } else {
            skipped_count += 1;
            info!(
                "Skipping guard not in new region range => guard_value='{}', range=[{},{})",
                guard.guard_value,
                hex::encode_upper(&guard.start_key),
                hex::encode_upper(&guard.end_key)
            );
        }
    }

    // 3) Insert/extend old region's *valid* guards into new region's guard list.
    let mut new_guards = REGION_TO_GUARD_MAP.entry(new_region_id).or_insert_with(Vec::new);
    let old_count = old_guards.len();
    let new_count_before = new_guards.len();
    new_guards.extend(transferred_guards);

    // 4) Remove the old region from the map entirely (since it's merged).
    REGION_TO_GUARD_MAP.remove(&old_region_id);

    // 5) Final log / verification
    info!(
        "Merged old_region_id={} into new_region_id={}. \
        Moved {} guards; skipped {} out-of-range guards. \
        New region had {} => now has {} guards total.",
        old_region_id,
        new_region_id,
        old_count - skipped_count,
        skipped_count,
        new_count_before,
        new_guards.len()
    );
}


/// Filters the RangeGuard vector for `old_region_id` so that only guards that
/// fit into [old_region_start_key, old_region_end_key) remain.
pub fn filter_region_split(
    old_region_id: u64,
    old_region_start_key: &[u8],
    old_region_end_key: &[u8],
) {
    // Look up the old region's RangeGuard list in the DashMap.
    let mut guard_vec = match REGION_TO_GUARD_MAP.get_mut(&old_region_id) {
        Some(gv) => gv,
        None => {
            info!("No RangeGuards found for region {}", old_region_id);
            return;
        }
    };
    let original_count = guard_vec.len();

    // Build a new vector where each RangeGuard is trimmed to the overlap.
    // If there is no overlap, the guard is dropped.
    let updated_guards: Vec<RangeGuard> = guard_vec
        .iter()
        .cloned() // clone each RangeGuard for modification
        .filter_map(|mut g| {
            if let Some((new_start, new_end)) = range_overlap(
                &g.start_key,
                &g.end_key,
                old_region_start_key,
                old_region_end_key,
            ) {
                // Update the guard's keys to the overlapping range.
                g.start_key = new_start;
                g.end_key = new_end;
                Some(g)
            } else {
                info!(
                    "Discarding guard out of range => region={}, guard={:?}",
                    old_region_id, g
                );
                None
            }
        })
        .collect();

    // Replace the old list with the filtered list.
    *guard_vec = updated_guards;

    // Debug info
    info!(
        "handle_region_split: region={} filtered from {} => {} guards. \
         Now only those that fit [{}, {}) remain.",
        old_region_id,
        original_count,
        guard_vec.len(),
        hex::encode_upper(old_region_start_key),
        hex::encode_upper(old_region_end_key)
    );
}


/// Handle region split: old_region -> new_region.
///
/// - old_region_id: the region being split
/// - old_region_start_key, old_region_end_key: old region's boundaries
/// - new_region_id: the newly created region
/// - new_region_start_key, new_region_end_key: new region's boundaries
///
/// After splitting, we verify that all old_region guards lie within [old_region_start_key, old_region_end_key),
/// and all new_region guards lie within [new_region_start_key, new_region_end_key).
pub fn handle_region_split(
    old_region_id: u64,
    old_region_start_key: &[u8],
    old_region_end_key: &[u8],
    new_region_id: u64,
    new_region_start_key: &[u8],
    new_region_end_key: &[u8],
) {
    // Debug info
    info!(
        "handle_region_split: old_region_id={}, new_region_id={}, \
        old_range=[{}, {}], new_range=[{}, {}]",
        old_region_id,
        new_region_id,
        hex::encode_upper(old_region_start_key),
        hex::encode_upper(old_region_end_key),
        hex::encode_upper(new_region_start_key),
        hex::encode_upper(new_region_end_key)
    );

    // Get the RangeGuard vector for the old region (if none, nothing to do).
    let mut old_guards = match REGION_TO_GUARD_MAP.get_mut(&old_region_id) {
        Some(guard_vec) => guard_vec,
        None => {
            info!(
                "No RangeGuards found for old_region_id={}, nothing to split.",
                old_region_id
            );
            return;
        }
    };

    // Prepare a place to store new region's guards (or get existing).
    let mut new_guards = REGION_TO_GUARD_MAP.entry(new_region_id).or_insert_with(Vec::new);

    // We'll build a new list of old_region's guards after we handle splitting.
    // let mut updated_old_guards = Vec::with_capacity(old_guards.len());
    // For each RangeGuard in old region, check if part of it belongs to the new region.
    for guard in old_guards.iter() {
        // If there's an overlap between guard and the new region's [start, end],
        // we move (or copy) that overlapping part to new region.
        if let Some((overlap_start, overlap_end)) = range_overlap(
            &guard.start_key,
            &guard.end_key,
            &new_region_start_key,
            &new_region_end_key,
        ) {
            // This means at least part of the guard belongs to the new region.
            // We'll push a new RangeGuard for the new region.
            let mut new_guard: RangeGuard = guard.clone();
            new_guard.start_key = overlap_start.clone();
            new_guard.end_key = overlap_end.clone();
            new_guards.push(new_guard);
        }
    }

    // Replace old region's guards with the updated list.
    // *old_guards = updated_old_guards;


    // Verification step: all old_region's guards must lie in [old_region_start_key, old_region_end_key).
    // for guard in old_guards.iter() {
    //     if !key_in_range(&guard.start_key, &old_region_start_key, &old_region_end_key)
    //         || !key_in_range(&guard.end_key, &old_region_start_key, &old_region_end_key)
    //     {
    //         info!(
    //             "Warning: old_region_id={} has guard out of range => {:?}",
    //             old_region_id, guard
    //         );
    //     }
    // }

    // Verification step: all new_region's guards must lie in [new_region_start_key, new_region_end_key).
    for guard in new_guards.iter() {
        if !guard_in_region_range(guard, new_region_start_key, new_region_end_key)
        {
            info!(
                "Warning: new_region_id={} has guard out of range => {:?}",
                new_region_id, guard
            );
        }
    }

    info!(
        "After region split, old_region_id={} has {} guards, new_region_id={} has {} guards.",
        old_region_id,
        old_guards.len(),
        new_region_id,
        new_guards.len()
    );
}




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
    // print_region_guard_map();
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
    // print_region_guard_map();
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
    // print_region_guard_map();
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
                    if remove_overlap {
                        remove_overlaps(rg_vec, idx);
                    }
                } else {
                    warn!(
                        "No existing RangeGuard found for region_id={} matching guard_value='{}' \
                         to set END key!",
                        region_id,
                        stripped_value
                    );
                    rg_vec.push(RangeGuard {
                        start_key: Vec::new(),
                        end_key: key.clone(),
                        guard_value: stripped_value.to_owned(),
                    });
                    // The new RangeGuard is always pushed to the last
                    let idx_new = rg_vec.len() - 1;
                    info!(
                        "Added END RangeGuard: region_id={}, guard_value={}, end_key={}",
                        region_id,
                        stripped_value,
                        hex::encode_upper(&key)
                    );
                    if remove_overlap {
                        remove_overlaps(rg_vec, idx_new);
                    }
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
    } else if let Some(stripped_value) = guard_value.strip_prefix("GUARD_") {
        let parts: Vec<&str> = stripped_value.split('_').collect();
        if parts.len() == 3 {
            let custom_guard = parts[0];  // The custom guard value.
            let guard_start = parts[1];   // The start key (expected as hex).
            let guard_end = parts[2];     // The end key (expected as hex).
    
            REGION_TO_GUARD_MAP
                .entry(region_id)
                .and_modify(|rg_vec| {
                    // Look for an existing RangeGuard with the same custom guard value.
                    if let Some(idx) = rg_vec.iter().position(|rg| rg.guard_value == custom_guard) {
                        // Update both the start and end keys.
                        rg_vec[idx].start_key = hex::decode(guard_start).unwrap_or_default();
                        rg_vec[idx].end_key = hex::decode(guard_end).unwrap_or_default();
                        info!(
                            "Reused existing GUARD RangeGuard: region_id={}, guard_value={}, start_key={}, end_key={}",
                            region_id,
                            custom_guard,
                            guard_start,
                            guard_end,
                        );
                        if remove_overlap {
                            remove_overlaps(rg_vec, idx);
                        }
                    } else {
                        // No matching guard exists; insert a new one.
                        rg_vec.push(RangeGuard {
                            start_key: hex::decode(guard_start).unwrap_or_default(),
                            end_key: hex::decode(guard_end).unwrap_or_default(),
                            guard_value: custom_guard.to_owned(),
                        });
                        let idx_new = rg_vec.len() - 1;
                        info!(
                            "Added GUARD RangeGuard: region_id={}, guard_value={}, start_key={}, end_key={}",
                            region_id,
                            custom_guard,
                            guard_start,
                            guard_end,
                        );
                        if remove_overlap {
                            remove_overlaps(rg_vec, idx_new);
                        }
                    }
                })
                .or_insert_with(|| {
                    // If the region_id did not exist yet, create a new vector with the new guard.
                    vec![RangeGuard {
                        start_key: hex::decode(guard_start).unwrap_or_default(),
                        end_key: hex::decode(guard_end).unwrap_or_default(),
                        guard_value: custom_guard.to_owned(),
                    }]
                });
        } else {
            warn!("Invalid GUARD pattern: {}", guard_value);
        }
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
    // info!(
    //     "get_region_guard_for_key, region_id={}, key={}",
    //     region_id,
    //     hex::encode_upper(key)
    // );
    // print_region_guard_map();

    // Read access is also concurrency-safe; we get a read lock for region_id.
    let rg_vec = match REGION_TO_GUARD_MAP.get(&region_id) {
        Some(r) => r,
        None => {
            // warn!(
            //     "Region {} not found in REGION_TO_GUARD_MAP, key={}",
            //     region_id,
            //     hex::encode_upper(key)
            // );
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
    // print_region_guard_map();
    
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

            // info!(
            //     "Retrieved all guard values for region_id={}: {}",
            //     region_id, all_guards
            // );
            Some(all_guards)
        }
        None => {
            // warn!("Region {} not found in REGION_TO_GUARD_MAP", region_id);
            None
        }
    }
}


/// Parses a comma-separated guard string and updates the guard list for the given region.
/// The expected format for each guard is: `guard_value(start_key_hex,end_key_hex)`.
/// If the start or end key is empty, it is represented by a colon (`:`).
pub fn set_region_guard_from_string(region_id: u64, guard_value: String) {
    info!("set_region_guard_from_string: {:?}, {:?}", region_id, guard_value);

    if get_region_guard(region_id).unwrap_or_else(|| "".to_string()) == guard_value {
        return;
    }

    // If the guard string is empty, remove any existing guards.
    if guard_value.trim().is_empty() {
        REGION_TO_GUARD_MAP.remove(&region_id);
        return;
    }

    // If the guard string doesn't contain a '(', then it's just a simple string.
    if !guard_value.contains('(') {
        update_region_guard(region_id, guard_value);
        return;
    }

    let mut guards: Vec<RangeGuard> = Vec::new();

    // Iterate over all matches.
    for cap in GUARD_REGEX.captures_iter(&guard_value) {
        let guard_val = cap
            .name("guard_value")
            .map(|m| m.as_str().trim().to_string())
            .unwrap_or_default();
        let start_key_str = cap.name("start_key").map(|m| m.as_str()).unwrap_or("");
        let end_key_str = cap.name("end_key").map(|m| m.as_str()).unwrap_or("");

        // info!(
        //     "Parsed guard entry";
        //     "guard_value" => guard_val.clone(),
        //     "start_key" => start_key_str.clone(),
        //     "end_key" => end_key_str.clone(),
        // );

        // If the value is ":", interpret it as an empty vector.
        let start_key = if start_key_str == ":" {
            Vec::new()
        } else {
            match hex::decode(start_key_str) {
                Ok(val) => val,
                Err(e) => {
                    warn!("Failed to decode start key '{}' in guard entry {}: {}", start_key_str, guard_val, e);
                    continue;
                }
            }
        };

        let end_key = if end_key_str == ":" {
            Vec::new()
        } else {
            match hex::decode(end_key_str) {
                Ok(val) => val,
                Err(e) => {
                    warn!("Failed to decode end key '{}' in guard entry {}: {}", end_key_str, guard_val, e);
                    continue;
                }
            }
        };

        guards.push(RangeGuard {
            guard_value: guard_val,
            start_key,
            end_key,
        });
    }

    // Insert (or update) the parsed list into the global map.
    REGION_TO_GUARD_MAP.insert(region_id, guards);

    // info!(
    //     "Inserted region guard mapping";
    //     "region_id" => region_id,
    //     "guards" => ?guards
    // );

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
