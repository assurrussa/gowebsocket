// Package origin compiles a fail-closed WebSocket Origin allowlist.
package origin

import (
	"errors"
	"net/url"
	"regexp"
	"strings"
)

type Matcher struct {
	patterns     []*regexp.Regexp
	allowMissing bool
}

func normalized(value string) (string, bool) {
	if len(value) > 2048 || strings.ContainsAny(value, "\r\n\t ") {
		return "", false
	}
	u, err := url.Parse(value)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" ||
		u.User != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return "", false
	}
	return strings.ToLower(u.Scheme + "://" + u.Host), true
}

// New supports exact origins, host globs (* and ?) and the explicit all-origins
// entry *. Wildcards must not be used in the scheme, port or URL path.
func New(allowed []string, allowMissing bool) (*Matcher, error) {
	m := &Matcher{allowMissing: allowMissing}
	for _, pattern := range allowed {
		if pattern == "*" {
			m.patterns = append(m.patterns, regexp.MustCompile(`^https?://.+$`))
			continue
		}
		pattern = strings.ToLower(pattern)
		split := strings.SplitN(pattern, "://", 2)
		if len(split) != 2 || (split[0] != "http" && split[0] != "https") {
			return nil, errors.New("invalid allowed origin")
		}
		probe := strings.NewReplacer("*", "x", "?", "x").Replace(pattern)
		if _, ok := normalized(probe); !ok {
			return nil, errors.New("invalid allowed origin")
		}
		// Only DNS host globs are accepted. Ports and IPv6 addresses are literal.
		u, _ := url.Parse(probe)
		if strings.ContainsAny(split[1], "*?") && (strings.Contains(u.Hostname(), ":") ||
			strings.ContainsAny(split[1][len(u.Hostname()):], "*?")) {
			return nil, errors.New("wildcard outside DNS host")
		}
		expression := regexp.QuoteMeta(pattern)
		expression = strings.ReplaceAll(expression, `\*`, `[^:/]*`)
		expression = strings.ReplaceAll(expression, `\?`, `[^:/]`)
		compiled, err := regexp.Compile("^" + expression + "$")
		if err != nil {
			return nil, err
		}
		m.patterns = append(m.patterns, compiled)
	}
	return m, nil
}

func (m *Matcher) Match(value string) bool {
	if value == "" {
		return m.allowMissing
	}
	value, ok := normalized(value)
	if !ok {
		return false
	}
	for _, pattern := range m.patterns {
		if pattern.MatchString(value) {
			return true
		}
	}
	return false
}
