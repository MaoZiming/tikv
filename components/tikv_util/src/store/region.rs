// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;

use dashmap::DashMap;
use hex;
use kvproto::metapb::Region;
use once_cell::sync::Lazy;
use regex::Regex;

// Each range in a region will have a start key, end key, and a guard_value
#[derive(Clone, Debug)]
pub struct RangeGuard {
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    guard_value: String,
}

// Globally accessible map: region_id -> vector of RangeGuard
pub static REGION_TO_GUARD_MAP: Lazy<DashMap<u64, Vec<RangeGuard>>> = Lazy::new(DashMap::new);
// Precompile the regex pattern once.
static GUARD_REGEX: Lazy<Regex> = Lazy::new(|| {
    // guard_value must be alphanumeric, start_key and end_key are either ":" or
    // hexadecimal.
    Regex::new(r"(?P<guard_value>[0-9A-Za-z]+)\((?P<start_key>:|[0-9A-Fa-f]+),(?P<end_key>:|[0-9A-Fa-f]+)\)").unwrap()
});

/// Compare two start keys, where an empty key is treated as -∞.
fn compare_start_keys(a: &[u8], b: &[u8]) -> Ordering {
    match (a.is_empty(), b.is_empty()) {
        (true, true) => Ordering::Equal,    // -∞ == -∞
        (true, false) => Ordering::Less,    // -∞ < anything
        (false, true) => Ordering::Greater, // anything > -∞
        (false, false) => a.cmp(b),         // normal lexicographical compare
    }
}

/// Compare two end keys, where an empty key is treated as +∞.
fn compare_end_keys(a: &[u8], b: &[u8]) -> Ordering {
    match (a.is_empty(), b.is_empty()) {
        (true, true) => Ordering::Equal,    // +∞ == +∞
        (true, false) => Ordering::Greater, // +∞ > anything
        (false, true) => Ordering::Less,    // anything < +∞
        (false, false) => a.cmp(b),         // normal lexicographical compare
    }
}

/// Return true if key is in [range_start, range_end).
// fn key_in_range(key: &[u8], range_start: &[u8], range_end: &[u8]) -> bool {
//     // range_start <= key < range_end
//     // using specialized comparisons:
//     compare_start_keys(range_start, key) != Ordering::Greater
//         && compare_end_keys(key, range_end) == Ordering::Less
// }

/// Return `true` if the [guard_start, guard_end) range is entirely within
/// [region_start, region_end).
fn guard_in_region_range(guard: &RangeGuard, region_start: &[u8], region_end: &[u8]) -> bool {
    // region_start <= guard.start_key AND guard.end_key <= region_end
    (compare_start_keys(region_start, &guard.start_key) != Ordering::Greater)
        && (compare_end_keys(&guard.end_key, region_end) != Ordering::Greater)
}

/// Check if [guard_start, guard_end] overlaps with [region_start, region_end].
/// Returns (overlap_start, overlap_end) if there is an overlap, or None
/// otherwise.
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

    // info!(
    //     "range_overlap: guard_start: {:?}, guard_end: {:?}, region_start: {:?},
    // region_end: {:?}, overlap_start: {:?}, overlap_end: {:?}",
    //     guard_start, guard_end, region_start, region_end, overlap_start,
    // overlap_end, );

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
    // info!(
    //     "handle_region_merge: old_region_id={}, new_region_id={}, \
    //      old_range=[{}, {}), new_range=[{}, {})",
    //     old_region_id,
    //     new_region_id,
    //     hex::encode_upper(old_region_start_key),
    //     hex::encode_upper(old_region_end_key),
    //     hex::encode_upper(new_region_start_key),
    //     hex::encode_upper(new_region_end_key)
    // );

    // 1) Atomically remove old region to get consistent snapshot and drop lock immediately.
    //    This avoids deadlock: we don't hold a read guard while acquiring write guard on new_region.
    let old_guards = match REGION_TO_GUARD_MAP.remove(&old_region_id) {
        Some((_, guards)) => guards,
        None => {
            // info!(
            //     "No RangeGuards found for old_region_id={}, nothing to merge.",
            //     old_region_id
            // );
            return;
        }
    };

    // 2) Filter guards in local memory (no locks held)
    let old_count = old_guards.len();
    let mut skipped_count = 0;
    let filtered_guards: Vec<RangeGuard> = old_guards
        .into_iter()
        .filter(|guard| {
            if guard_in_region_range(guard, new_region_start_key, new_region_end_key) {
                true
            } else {
                skipped_count += 1;
                // info!(
                //     "Skipping guard not in new region range => guard_value='{}',
                // range=[{},{})",     guard.guard_value,
                //     hex::encode_upper(&guard.start_key),
                //     hex::encode_upper(&guard.end_key)
                // );
                false
            }
        })
        .collect();

    // 3) Extend new region in a separate operation (single lock, no deadlock risk)
    let new_count_before = {
        let mut new_guards = REGION_TO_GUARD_MAP
            .entry(new_region_id)
            .or_insert_with(Vec::new);
        let count = new_guards.len();
        new_guards.extend(filtered_guards);
        count
    };

    // 4) Final log / verification
    // info!(
    //     "Merged old_region_id={} into new_region_id={}. \
    //     Moved {} guards; skipped {} out-of-range guards. \
    //     New region had {} => now has {} total.",
    //     old_region_id,
    //     new_region_id,
    //     old_count - skipped_count,
    //     skipped_count,
    //     new_count_before,
    //     new_count_before + (old_count - skipped_count)
    // );
}

/// Filters the RangeGuard vector for `old_region_id` so that only guards that
/// fit into [old_region_start_key, old_region_end_key) remain.
pub fn filter_region_split(
    old_guards: Vec<RangeGuard>,
    old_region_id: u64,
    old_region_start_key: &[u8],
    old_region_end_key: &[u8],
) {
    for guard in &old_guards {
        if guard.guard_value.contains("NOSPLIT") {
            // info!("NOSPLIT {}", old_region_id);
            return;
        }
    }

    // Look up the old region's RangeGuard list in the DashMap.
    let mut guard_vec = match REGION_TO_GUARD_MAP.get_mut(&old_region_id) {
        Some(gv) => gv,
        None => {
            // info!("No RangeGuards found for region {}", old_region_id);
            return;
        }
    };
    let _original_count = guard_vec.len();

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
                // info!(
                //     "Discarding guard out of range => region={}, guard={:?}",
                //     old_region_id, g
                // );
                None
            }
        })
        .collect();

    // Replace the old list with the filtered list.
    *guard_vec = updated_guards;

    // Debug info
    // info!(
    //     "handle_region_split: region={} filtered from {} => {} guards. \
    //      Now only those that fit [{}, {}) remain.",
    //     old_region_id,
    //     original_count,
    //     guard_vec.len(),
    //     hex::encode_upper(old_region_start_key),
    //     hex::encode_upper(old_region_end_key)
    // );
}

/// Handle region split: old_region -> new_region.
/// Clone guards from old_region that overlap with new_region boundaries.
pub fn handle_region_split(
    old_region_id: u64,
    new_region_id: u64,
    new_region_start_key: &[u8],
    new_region_end_key: &[u8],
) {
    // 1) Clone snapshot with scoped guard to avoid deadlock
    let old_guards: Vec<RangeGuard> = {
        match REGION_TO_GUARD_MAP.get(&old_region_id) {
            Some(guard_ref) => guard_ref.clone(),
            None => {
                // info!("No RangeGuards found for old_region_id={}, nothing to split.", old_region_id);
                return;
            }
        }
    }; // Read guard dropped here

    // 2) Filter and trim in local memory
    let new_guards: Vec<RangeGuard> = old_guards
        .into_iter()
        .filter_map(|mut guard| {
            if let Some((overlap_start, overlap_end)) = range_overlap(
                &guard.start_key,
                &guard.end_key,
                new_region_start_key,
                new_region_end_key,
            ) {
                guard.start_key = overlap_start;
                guard.end_key = overlap_end;
                Some(guard)
            } else {
                None
            }
        })
        .collect();

    // 3) Insert into new region (single lock, no deadlock risk)
    if !new_guards.is_empty() {
        REGION_TO_GUARD_MAP.insert(new_region_id, new_guards);
    }
}

pub fn handle_region_split_with_old_guards(
    old_guards: Vec<RangeGuard>,
    new_region_id: u64,
    new_region_start_key: &[u8],
    new_region_end_key: &[u8],
) {
    for guard in &old_guards {
        if guard.guard_value.contains("NOSPLIT") {
            return;
        }
    }
    let mut old_guards = old_guards;
    old_guards = old_guards
        .into_iter()
        .filter_map(|mut guard| {
            if let Some((overlap_start, overlap_end)) = range_overlap(
                &guard.start_key,
                &guard.end_key,
                new_region_start_key,
                new_region_end_key,
            ) {
                // Update the guard with the overlapping keys.
                guard.start_key = overlap_start;
                guard.end_key = overlap_end;
                Some(guard)
            } else {
                None
            }
        })
        .collect();

    REGION_TO_GUARD_MAP.insert(new_region_id, old_guards);
}

/// Creates or updates a guard covering the entire region.
///
/// This *replaces* any existing range definitions for the given `region_id`
/// with a single guard that covers the whole region.
pub fn update_region_guard(region_id: u64, guard_value: String) {
    // info!(
    //     "Updating region guard (entire region): region_id={}, guard_value={}",
    //     region_id, guard_value
    // );

    let entire_guard = RangeGuard {
        start_key: Vec::new(),
        end_key: Vec::new(),
        guard_value,
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

    // info!(
    //     "Finished updating region guard (entire region): region_id={},
    // guard_value={}",     region_id, guard_value
    // );
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

    // info!(
    //     "DROP overlap guard: start_key={}, end_key={}, guard_value={}",
    //     hex::encode_upper(&a.start_key),
    //     hex::encode_upper(&a.end_key),
    //     a.guard_value
    // );

    true
}

/// Removes any RangeGuards in `rg_vec` that overlap with the newly updated
/// guard at `main_idx`.
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
/// - If guard_value starts with "END_", sets the end_key for the most recently
///   added or matched guard.
/// - Otherwise, resets the entire region using `update_region_guard`.
pub fn update_region_guard_with_key(region_id: u64, guard_value: String, key: Vec<u8>) {
    // info!(
    //     "Updating region guard with key: region_id={}, guard_value={}, key={}",
    //     region_id,
    //     guard_value,
    //     hex::encode_upper(&key)
    // );

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

                if let Some(idx) = rg_vec
                    .iter()
                    .position(|rg| rg.guard_value == stripped_value)
                {
                    // Now we have the index of the existing guard
                    rg_vec[idx].start_key = key.clone();
                    // info!(
                    //     "Reused existing START RangeGuard: region_id={}, guard_value={}, \
                    //      new start_key={}, end_key={}",
                    //     region_id,
                    //     stripped_value,
                    //     hex::encode_upper(&key),
                    //     hex::encode_upper(&rg_vec[idx].end_key),
                    // );
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
                    // info!(
                    //     "Added START RangeGuard: region_id={}, guard_value={}, start_key={}",
                    //     region_id,
                    //     stripped_value,
                    //     hex::encode_upper(&key)
                    // );
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

                    // info!(
                    //     "Matched END RangeGuard: region_id={}, guard_value={}, start_key={}, end_key={}",
                    //     region_id,
                    //     stripped_value,
                    //     hex::encode_upper(&rg_vec[idx].start_key),
                    //     hex::encode_upper(&key),
                    // );
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
                    // info!(
                    //     "Added END RangeGuard: region_id={}, guard_value={}, end_key={}",
                    //     region_id,
                    //     stripped_value,
                    //     hex::encode_upper(&key)
                    // );
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
            let custom_guard = parts[0]; // The custom guard value.
            let guard_start = parts[1]; // The start key (expected as hex).
            let guard_end = parts[2]; // The end key (expected as hex).

            REGION_TO_GUARD_MAP
                .entry(region_id)
                .and_modify(|rg_vec| {
                    // Look for an existing RangeGuard with the same custom guard value.
                    if let Some(idx) = rg_vec.iter().position(|rg| rg.guard_value == custom_guard) {
                        // Update both the start and end keys.
                        rg_vec[idx].start_key = hex::decode(guard_start).unwrap_or_default();
                        rg_vec[idx].end_key = hex::decode(guard_end).unwrap_or_default();
                        // info!(
                        //     "Reused existing GUARD RangeGuard: region_id={}, guard_value={},
                        // start_key={}, end_key={}",     region_id,
                        //     custom_guard,
                        //     guard_start,
                        //     guard_end,
                        // );
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
                        // info!(
                        //     "Added GUARD RangeGuard: region_id={}, guard_value={}, start_key={},
                        // end_key={}",     region_id,
                        //     custom_guard,
                        //     guard_start,
                        //     guard_end,
                        // );
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
        // If guard_value does not begin with "START_" or "END_", call
        // update_region_guard to make the guard_value cover the entire region.
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
            // info!(
            //     "Key {} is in range [{}, {}] for region_id={}, guard_value={}",
            //     hex::encode_upper(key),
            //     hex::encode_upper(&range_guard.start_key),
            //     hex::encode_upper(&range_guard.end_key),
            //     region_id,
            //     range_guard.guard_value
            // );
            matched_guards.push(range_guard.guard_value.clone());
        }
    }

    // Join matched guard values into a comma-separated string
    if !matched_guards.is_empty() {
        return Some(matched_guards.join(","));
    }

    // warn!(
    //     "No matching guard found for region_id={}, key={}",
    //     region_id,
    //     hex::encode_upper(key)
    // );
    return Some("NoExistingGuard".to_string());
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
