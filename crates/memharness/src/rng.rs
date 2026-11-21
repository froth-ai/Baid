//! A small, stable random number generator and integer-only sampling (G27).
//!
//! A benchmark dataset must come out byte-identical from the same seed on every platform and
//! for every future build. So:
//!
//! - The generator is SplitMix64 (Steele, Lea and Flood, 2014; Vigna's public-domain
//!   reference `splitmix64.c`), tested against its published reference vectors. `rand` is not
//!   used: its generators do not promise the same output across versions.
//! - Every sampling step uses integers only. libm functions such as `powf`, `ln` and `exp`
//!   are not guaranteed bit-identical between Windows and Linux, so power laws are cumulative
//!   integer weight tables and bounded integers use Lemire's unbiased multiply-shift.

/// SplitMix64: 64 bits of state, one addition and a 64-bit mix per output.
#[derive(Clone, Debug)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    /// Starts the generator from `seed`.
    pub fn new(seed: u64) -> Self {
        SplitMix64 { state: seed }
    }

    /// The next 64 random bits.
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A uniform integer in `0..n`, without bias (Lemire, "Fast Random Integer Generation
    /// in an Interval", 2019). `n` must be at least 1.
    pub fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0, "below(0)");
        let mut m = u128::from(self.next_u64()) * u128::from(n);
        if (m as u64) < n {
            let threshold = n.wrapping_neg() % n;
            while (m as u64) < threshold {
                m = u128::from(self.next_u64()) * u128::from(n);
            }
        }
        (m >> 64) as u64
    }

    /// A uniform integer in `lo..=hi`.
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        assert!(lo <= hi, "empty range");
        lo + self.below(hi - lo + 1)
    }

    /// True with probability `num / den`.
    pub fn chance(&mut self, num: u64, den: u64) -> bool {
        self.below(den) < num
    }

    /// A uniformly chosen element. `xs` must not be empty.
    pub fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below(xs.len() as u64) as usize]
    }

    /// A generator for a sub-task, so adding draws to one part of the dataset does not shift
    /// every later part.
    pub fn fork(&mut self) -> SplitMix64 {
        SplitMix64::new(self.next_u64())
    }
}

/// Sampling from integer weights: `cumulative[i]` is the sum of the weights up to and
/// including `i`.
#[derive(Clone, Debug)]
pub struct Weighted {
    cumulative: Vec<u64>,
}

/// Numerator of the power-law weights: rank `k` (1-based) weighs `ZIPF_SCALE / k`.
const ZIPF_SCALE: u64 = 1 << 40;

impl Weighted {
    /// A table from explicit weights. At least one weight must be non-zero.
    pub fn new(weights: impl IntoIterator<Item = u64>) -> Self {
        let mut total = 0u64;
        let cumulative: Vec<u64> = weights
            .into_iter()
            .map(|w| {
                total = total.checked_add(w).expect("weights overflow u64");
                total
            })
            .collect();
        assert!(total > 0, "all weights are zero");
        Weighted { cumulative }
    }

    /// Zipf's law with exponent 1 over `n` ranks (rank 1 is the most likely), plus a floor of
    /// `floor_per_mille` thousandths of rank 1's weight on every rank, so the tail is never
    /// empty. Integer division only.
    pub fn zipf(n: usize, floor_per_mille: u64) -> Self {
        let floor = ZIPF_SCALE / 1000 * floor_per_mille;
        Weighted::new((1..=n as u64).map(|k| ZIPF_SCALE / k + floor))
    }

    /// Draws an index.
    pub fn sample(&self, rng: &mut SplitMix64) -> usize {
        let total = *self.cumulative.last().unwrap_or(&0);
        let x = rng.below(total);
        self.cumulative.partition_point(|&c| c <= x)
    }

    /// Number of outcomes.
    pub fn len(&self) -> usize {
        self.cumulative.len()
    }

    /// True when there are no outcomes (never, after `new`).
    pub fn is_empty(&self) -> bool {
        self.cumulative.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Published reference output of `splitmix64.c` for seed 1234567, as listed by the
    /// Rosetta Code task "Pseudo-random numbers/Splitmix64".
    #[test]
    fn splitmix64_matches_the_reference_vectors() {
        let mut r = SplitMix64::new(1_234_567);
        let got: Vec<u64> = (0..5).map(|_| r.next_u64()).collect();
        assert_eq!(
            got,
            [
                6_457_827_717_110_365_317,
                3_203_168_211_198_807_973,
                9_817_491_932_198_370_423,
                4_593_380_528_125_082_431,
                16_408_922_859_458_223_821,
            ]
        );
    }

    /// The same task's second check: seed 987654321, 100,000 draws mapped to `floor(x * 5)`
    /// where `x = (next >> 11) / 2^53`. Computed here with integers, which is exact.
    #[test]
    fn splitmix64_matches_the_reference_bucket_counts() {
        let mut r = SplitMix64::new(987_654_321);
        let mut counts = [0u32; 5];
        for _ in 0..100_000 {
            let b = ((r.next_u64() >> 11) * 5) >> 53;
            counts[b as usize] += 1;
        }
        assert_eq!(counts, [20_027, 19_892, 20_073, 19_978, 20_030]);
    }

    #[test]
    fn below_stays_in_range_and_covers_it() {
        let mut r = SplitMix64::new(7);
        let mut seen = [0u32; 7];
        for _ in 0..7_000 {
            seen[r.below(7) as usize] += 1;
        }
        assert!(seen.iter().all(|&c| (800..1_200).contains(&c)), "{seen:?}");
        assert_eq!(r.below(1), 0);
        // The rejection branch: a bound just over half of 2^64 rejects ~half the draws.
        let big = (1u64 << 63) + 1;
        assert!((0..1_000).all(|_| r.below(big) < big));
    }

    /// The rejection step is what makes `below` unbiased; for small bounds it almost never
    /// fires, so test it where it matters. With `n = (2^65 + 1) / 3` (about 2/3 of 2^64),
    /// plain multiply-shift maps two inputs to every even result and one to every odd one,
    /// so even results come out 2/3 of the time. Unbiased, it is 1/2.
    #[test]
    fn below_rejects_to_stay_unbiased_for_huge_bounds() {
        let n = 0xAAAA_AAAA_AAAA_AAAB_u64;
        let mut r = SplitMix64::new(5);
        let even = (0..30_000).filter(|_| r.below(n).is_multiple_of(2)).count();
        assert!((14_100..15_900).contains(&even), "{even} of 30000 even");
    }

    #[test]
    fn weighted_follows_its_weights() {
        let mut r = SplitMix64::new(11);
        let w = Weighted::new([1, 0, 3]);
        let mut seen = [0u32; 3];
        for _ in 0..8_000 {
            seen[w.sample(&mut r)] += 1;
        }
        assert_eq!(seen[1], 0);
        assert!((1_700..2_300).contains(&seen[0]), "{seen:?}");
    }

    #[test]
    fn zipf_puts_most_weight_on_low_ranks() {
        let mut r = SplitMix64::new(3);
        let z = Weighted::zipf(100, 0);
        let mut seen = [0u32; 100];
        for _ in 0..50_000 {
            seen[z.sample(&mut r)] += 1;
        }
        // Rank 1 ~ 1/H(100) = 19%; rank 10 ~ 1.9%; rank 100 ~ 0.19%.
        assert!(seen[0] > 8 * seen[9], "{} vs {}", seen[0], seen[9]);
        assert!(seen[9] > 4 * seen[99], "{} vs {}", seen[9], seen[99]);
    }
}
