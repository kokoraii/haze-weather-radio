// Package feedconfig reads the managed feed config the Haze daemon owns.
//
// The daemon parses and validates managed/feeds (crates/haze-feed-config) and
// publishes the result on the host event bridge. Go services keep a [Store] of
// the latest snapshot and evaluate coverage with the methods here instead of
// parsing feed XML. These rules mirror the Rust crate, and both run the shared
// cases in contracts/feedconfig/v1/coverage-cases.json. See docs/feed-config.md.
package feedconfig

import (
	"sort"
	"strings"
)

// SnapshotVersion is the snapshot format this package understands.
const SnapshotVersion = 1

// SameMaxLocations is the most location codes one SAME header can carry.
const SameMaxLocations = 31

// Snapshot is the daemon's compiled feed config.
type Snapshot struct {
	Version    int    `json:"version"`
	Generation string `json:"generation"`
	Feeds      []Feed `json:"feeds"`
}

// Feed returns one feed's coverage.
func (s *Snapshot) Feed(feedID string) (Feed, bool) {
	for _, feed := range s.Feeds {
		if feed.FeedID == feedID {
			return feed, true
		}
	}
	return Feed{}, false
}

// Feed is one feed's coverage. No entries means it takes alerts for every
// location, subject to its own alert source settings.
type Feed struct {
	FeedID  string  `json:"feed_id"`
	Entries []Entry `json:"entries"`
}

// Entry is one compiled coverage region.
type Entry struct {
	// ID is the id as configured, such as 065500, 0655*, or SKZ012.
	ID             string `json:"id"`
	Source         string `json:"source"`
	Name           string `json:"name"`
	DeriveForecast string `json:"derive_forecast"`
	// RegionCode is the SAME code the entry adds to a header, or empty.
	RegionCode string       `json:"region_code"`
	Matcher    Matcher      `json:"matcher"`
	Header     HeaderPolicy `json:"header"`
	Subregions []Matcher    `json:"subregions"`
}

// Matcher selects location codes: an exact code or a leading-digit prefix.
type Matcher struct {
	Kind  MatchKind `json:"kind"`
	Value string    `json:"value"`
}

type MatchKind string

const (
	MatchExact  MatchKind = "exact"
	MatchPrefix MatchKind = "prefix"
)

// HeaderPolicy says which affected subregions of an entry go into a header.
type HeaderPolicy string

const (
	// HeaderAll keeps every affected code the entry covers.
	HeaderAll HeaderPolicy = "all"
	// HeaderListed keeps only listed subregions; others alert under RegionCode.
	HeaderListed HeaderPolicy = "listed"
)

// Matches reports whether the matcher covers a normalized location code.
func (m Matcher) Matches(code string) bool {
	if m.Kind == MatchPrefix {
		return isSAMECode(code) && strings.HasPrefix(code, m.Value)
	}
	return code == m.Value
}

// SameHeader is a SAME location list plus the codes the 31-code limit dropped.
type SameHeader struct {
	Codes   []string
	Dropped []string
}

// NormalizeCode uppercases a location code and strips everything but letters
// and digits.
func NormalizeCode(raw string) string {
	var out strings.Builder
	for _, char := range strings.TrimSpace(raw) {
		switch {
		case char >= '0' && char <= '9', char >= 'A' && char <= 'Z':
			out.WriteRune(char)
		case char >= 'a' && char <= 'z':
			out.WriteRune(char - 'a' + 'A')
		}
	}
	return out.String()
}

// Covers reports whether the feed's coverage includes code.
func (f Feed) Covers(code string) bool {
	_, wildcard := f.RegionFor(code)
	return f.Lists(code) || wildcard
}

// Lists reports whether code is configured itself: an exact entry or a listed
// subregion.
func (f Feed) Lists(code string) bool {
	for _, entry := range f.Entries {
		if entry.Matcher.Kind == MatchExact && entry.Matcher.Value == code {
			return true
		}
		for _, sub := range entry.Subregions {
			if sub.Matches(code) {
				return true
			}
		}
	}
	return false
}

// RegionFor returns the wildcard entry whose pattern contains code.
func (f Feed) RegionFor(code string) (Entry, bool) {
	for _, entry := range f.Entries {
		if entry.Matcher.Kind == MatchPrefix && entry.Matcher.Matches(code) {
			return entry, true
		}
	}
	return Entry{}, false
}

// SameHeader returns the SAME locations for an alert affecting codes. Each
// covering region code is added, so radios set to the region or a subregion
// both alarm. A region with a subregion list keeps only listed subregions; one
// without keeps every affected code. Regions come first, then listed
// subregions, then the rest, so the 31-code limit drops the least specific.
func (f Feed) SameHeader(codes []string) SameHeader {
	regions, listed, rest := map[string]struct{}{}, map[string]struct{}{}, map[string]struct{}{}
	for _, code := range codes {
		region, wildcard := f.RegionFor(code)
		if wildcard && region.RegionCode != "" {
			regions[region.RegionCode] = struct{}{}
			if region.RegionCode == code {
				// The whole region is affected, so its listed codes are too.
				for _, sub := range exactSubregions(region) {
					listed[sub] = struct{}{}
				}
				continue
			}
		}
		switch {
		case f.Lists(code):
			listed[code] = struct{}{}
		case !wildcard || region.Header == HeaderAll:
			rest[code] = struct{}{}
		}
	}
	ordered := append(sortedKeys(regions), sortedKeys(listed)...)
	return limit(append(ordered, sortedKeys(rest)...))
}

// FullHeader returns the SAME locations for the feed's whole coverage, used
// when an alert names no locations of its own (automations and operator
// alerts).
func (f Feed) FullHeader() SameHeader {
	var ordered []string
	listed := map[string]struct{}{}
	for _, entry := range f.Entries {
		if isSAMECode(entry.RegionCode) {
			ordered = append(ordered, entry.RegionCode)
		}
		for _, sub := range exactSubregions(entry) {
			listed[sub] = struct{}{}
		}
	}
	return limit(append(ordered, sortedKeys(listed)...))
}

func exactSubregions(entry Entry) []string {
	var out []string
	for _, sub := range entry.Subregions {
		if sub.Kind == MatchExact && isSAMECode(sub.Value) {
			out = append(out, sub.Value)
		}
	}
	return out
}

func limit(codes []string) SameHeader {
	seen := map[string]struct{}{}
	ordered := make([]string, 0, len(codes))
	for _, code := range codes {
		if _, ok := seen[code]; ok {
			continue
		}
		seen[code] = struct{}{}
		ordered = append(ordered, code)
	}
	header := SameHeader{Codes: ordered, Dropped: []string{}}
	if len(ordered) > SameMaxLocations {
		header.Codes = ordered[:SameMaxLocations]
		header.Dropped = ordered[SameMaxLocations:]
	}
	return header
}

func sortedKeys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func isSAMECode(value string) bool {
	if len(value) != 6 {
		return false
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}
