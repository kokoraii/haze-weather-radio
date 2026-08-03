package locationclient

import "testing"

func TestConfiguredIdentifierUsesQualifiedFeedNamespaces(t *testing.T) {
	tests := []struct {
		purpose   string
		value     string
		scheme    string
		authority string
	}{
		{"observation", "sk-40", "eccc_citypage", "eccc"},
		{"aviation", "CYXE", "icao", "icao"},
		{"marine_forecast", "m0000109", "marine", "eccc"},
		{"marine_observation", "9401177", "msc", "eccc"},
		{"hydrometric", "05HG001", "hydrometric", "eccc"},
	}
	for _, test := range tests {
		input, ok := ConfiguredIdentifier("eccc", test.purpose, test.value)
		if !ok || input.Scheme != test.scheme || input.Authority != test.authority {
			t.Fatalf("%s/%s = %#v, %t", test.purpose, test.value, input, ok)
		}
	}
}

func TestConfiguredIdentifierRejectsWildcard(t *testing.T) {
	if _, ok := ConfiguredIdentifier("eccc", "coverage", "06*"); ok {
		t.Fatal("wildcard must not become an exact location query")
	}
}
