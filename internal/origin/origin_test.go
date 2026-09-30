package origin_test

import (
	"testing"

	"github.com/assurrussa/gowebsocket/internal/origin"
)

func TestMatcher(t *testing.T) {
	matcher, err := origin.New([]string{"https://example.com", "https://*.example.org", "http://localhost:3000"}, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"https://example.com", "https://EXAMPLE.com", "https://a.example.org", "http://localhost:3000"} {
		if !matcher.Match(value) {
			t.Errorf("rejected %q", value)
		}
	}
	rejected := []string{
		"", "null", "https://example.com.evil", "https://example.com@evil",
		"https://evil/path/example.com", "http://example.com", "https://a.example.org/", "https://a.example.org?x=1",
	}
	for _, value := range rejected {
		if matcher.Match(value) {
			t.Errorf("accepted %q", value)
		}
	}
	deny, err := origin.New(nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if deny.Match("https://example.com") || deny.Match("") {
		t.Fatal("empty allowlist is not fail-closed")
	}
	native, err := origin.New(nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if !native.Match("") || native.Match("null") {
		t.Fatal("unexpected missing-origin policy")
	}
}

func TestInvalidPatternsAndExplicitWildcard(t *testing.T) {
	for _, pattern := range []string{"http*://example.com", "https://x/path", "ftp://example.com", "https://example.com:4*"} {
		if _, err := origin.New([]string{pattern}, false); err == nil {
			t.Errorf("accepted invalid config %q", pattern)
		}
	}
	anyOrigin, err := origin.New([]string{"*"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if !anyOrigin.Match("https://example.com") || anyOrigin.Match("file://example.com") || anyOrigin.Match("") {
		t.Fatal("unexpected wildcard policy")
	}
}

func FuzzMatcher(f *testing.F) {
	f.Add("https://example.com")
	matcher, err := origin.New([]string{"https://*.example.com"}, false)
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(_ *testing.T, input string) { _ = matcher.Match(input) })
}
