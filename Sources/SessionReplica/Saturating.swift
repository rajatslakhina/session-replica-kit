/// Saturating arithmetic used everywhere a counter, budget or delay is derived
/// from data the client does not control (server sequence numbers, byte
/// counts, backoff exponents). Nothing in this module may trap on input.
///
/// Every ceiling is derived from `Int.max` / `UInt64.max` rather than a
/// hard-coded 64-bit literal, because `Int` is 32-bit on watchOS.
public enum Saturating {
    /// `a + b`, clamped to `Int.max` / `Int.min` instead of overflowing.
    @inlinable
    public static func add(_ a: Int, _ b: Int) -> Int {
        let (result, overflow) = a.addingReportingOverflow(b)
        if !overflow { return result }
        return b > 0 ? Int.max : Int.min
    }

    /// `a + b` for unsigned 64-bit sequence numbers, clamped to `UInt64.max`.
    @inlinable
    public static func add(_ a: UInt64, _ b: UInt64) -> UInt64 {
        let (result, overflow) = a.addingReportingOverflow(b)
        return overflow ? UInt64.max : result
    }

    /// `a - b`, clamped to zero for unsigned values (never wraps).
    @inlinable
    public static func subtract(_ a: UInt64, _ b: UInt64) -> UInt64 {
        a >= b ? a - b : 0
    }

    /// `a * b`, clamped to `Int.max` / `Int.min`.
    @inlinable
    public static func multiply(_ a: Int, _ b: Int) -> Int {
        let (result, overflow) = a.multipliedReportingOverflow(by: b)
        if !overflow { return result }
        return (a < 0) == (b < 0) ? Int.max : Int.min
    }

    /// `a * b` for unsigned 64-bit values, clamped to `UInt64.max`.
    @inlinable
    public static func multiply(_ a: UInt64, _ b: UInt64) -> UInt64 {
        let (result, overflow) = a.multipliedReportingOverflow(by: b)
        return overflow ? UInt64.max : result
    }

    /// `2^exponent` as a `UInt64`, saturating instead of shifting past the
    /// width. A shift by 64 or more silently yields zero in Swift's smart
    /// shift, which would turn an intended "very long backoff" into "retry
    /// immediately" — the opposite of what a degraded link needs.
    @inlinable
    public static func powerOfTwo(_ exponent: Int) -> UInt64 {
        guard exponent >= 0 else { return 1 }
        guard exponent < UInt64.bitWidth - 1 else { return UInt64.max }
        return UInt64(1) << UInt64(exponent)
    }

    /// Converts a `Double` to `Int` without trapping: NaN maps to `fallback`,
    /// ±infinity and out-of-range values clamp to `Int.max` / `Int.min`.
    @inlinable
    public static func int(from value: Double, fallback: Int = 0) -> Int {
        if value.isNaN { return fallback }
        // `Double(Int.max)` rounds *up* to 2^(bitWidth-1), and
        // `value >= 2^(bitWidth-1)` is precisely the range `Int(value)` traps on.
        if value >= Double(Int.max) { return Int.max }
        if value <= Double(Int.min) { return Int.min }
        return Int(value)
    }

    /// `total * fraction` as a `UInt64` without trapping: NaN and negative
    /// fractions map to zero, fractions of one or more clamp to `total`.
    @inlinable
    public static func scaled(_ total: UInt64, by fraction: Double) -> UInt64 {
        if fraction.isNaN || fraction <= 0 { return 0 }
        if fraction >= 1 { return total }
        let scaled = Double(total) * fraction
        if scaled >= Double(UInt64.max) { return total }
        // Rounding in `Double(total)` can push the product above `total`.
        return min(total, UInt64(scaled))
    }

    /// Integer division that treats a zero divisor as "no division": returns
    /// `fallback` instead of trapping. `Int.min / -1` is also handled.
    @inlinable
    public static func divide(_ a: Int, by b: Int, fallback: Int = 0) -> Int {
        if b == 0 { return fallback }
        let (result, overflow) = a.dividedReportingOverflow(by: b)
        return overflow ? Int.max : result
    }

    /// Clamps `value` into `range`.
    @inlinable
    public static func clamp<T: Comparable>(_ value: T, to range: ClosedRange<T>) -> T {
        min(max(value, range.lowerBound), range.upperBound)
    }
}
