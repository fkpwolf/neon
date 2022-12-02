use crate::repository::{key_range_size, singleton_range, Key};
use postgres_ffi::BLCKSZ;
use std::ops::Range;

///
/// Represents a set of Keys, in a compact form.
///
#[derive(Clone, Debug)]
pub struct KeySpace {
    /// Contiguous ranges of keys that belong to the key space. In key order,
    /// and with no overlap.
    pub ranges: Vec<Range<Key>>,
}

//
// See comment to partition() method below. This constant is limited each layer to contain 16k relation.
// 16k - is first user Oid, so it separates system and user relations.
//
const MAX_LAYER_KEY_RANGE: i128 = 0x00_00000000_00000000_00004000_00_00000000i128;

impl KeySpace {
    ///
    /// Partition a key space into roughly chunks of roughly 'target_size' bytes
    /// in each partition. But also limit key range of each partition to avoid mixing of rel/nonrel entries
    /// and system/catalog relations. With current key encoding, frequently updated objects belong to opposite ends of key
    /// dimension range. It means that layers generated after compaction are used to cover all database space.
    /// Which cause image layer generation for the whole database, leading to huge rite amplification.
    /// Catalog tables (like pg_class) are also used to be updated frequently (for example with estimated value of relation rows/size).
    /// Even if we have on append only table, still generated delta layers will cover all this table, despite to the fact that only tail is updated.
    ///
    pub fn partition(&self, target_size: u64) -> KeyPartitioning {
        // Assume that each value is 8k in size.
        let target_nblocks = (target_size / BLCKSZ as u64) as usize;

        let mut parts = Vec::new();
        let mut current_part = Vec::new();
        let mut current_part_size: usize = 0;
        let mut current_part_start = 0i128;
        for range in &self.ranges {
            // If appending the next contiguous range in the keyspace to the current
            // partition would cause it to be too large, start a new partition.
            let this_size = key_range_size(range) as usize;
            let this_start = range.start.to_i128();
            if (current_part_size + this_size > target_nblocks
                || this_start - current_part_start > MAX_LAYER_KEY_RANGE)
                && !current_part.is_empty()
            {
                parts.push(KeySpace {
                    ranges: current_part,
                });
                current_part = Vec::new();
                current_part_size = 0;
                current_part_start = this_start;
            }

            // If the next range is larger than 'target_size', split it into
            // 'target_size' chunks.
            let mut remain_size = this_size;
            let mut start = range.start;
            while remain_size > target_nblocks {
                let next = start.add(target_nblocks as u32);
                parts.push(KeySpace {
                    ranges: vec![start..next],
                });
                start = next;
                remain_size -= target_nblocks
            }
            current_part.push(start..range.end);
            current_part_size += remain_size;
        }

        // add last partition that wasn't full yet.
        if !current_part.is_empty() {
            parts.push(KeySpace {
                ranges: current_part,
            });
        }

        KeyPartitioning { parts }
    }
}

///
/// Represents a partitioning of the key space.
///
/// The only kind of partitioning we do is to partition the key space into
/// partitions that are roughly equal in physical size (see KeySpace::partition).
/// But this data structure could represent any partitioning.
///
#[derive(Clone, Debug, Default)]
pub struct KeyPartitioning {
    pub parts: Vec<KeySpace>,
}

impl KeyPartitioning {
    pub fn new() -> Self {
        KeyPartitioning { parts: Vec::new() }
    }
}

///
/// A helper object, to collect a set of keys and key ranges into a KeySpace
/// object. This takes care of merging adjacent keys and key ranges into
/// contiguous ranges.
///
#[derive(Clone, Debug, Default)]
pub struct KeySpaceAccum {
    accum: Option<Range<Key>>,

    ranges: Vec<Range<Key>>,
}

impl KeySpaceAccum {
    pub fn new() -> Self {
        Self {
            accum: None,
            ranges: Vec::new(),
        }
    }

    pub fn add_key(&mut self, key: Key) {
        self.add_range(singleton_range(key))
    }

    pub fn add_range(&mut self, range: Range<Key>) {
        match self.accum.as_mut() {
            Some(accum) => {
                if range.start == accum.end {
                    accum.end = range.end;
                } else {
                    assert!(range.start > accum.end);
                    self.ranges.push(accum.clone());
                    *accum = range;
                }
            }
            None => self.accum = Some(range),
        }
    }

    pub fn to_keyspace(mut self) -> KeySpace {
        if let Some(accum) = self.accum.take() {
            self.ranges.push(accum);
        }
        KeySpace {
            ranges: self.ranges,
        }
    }
}
