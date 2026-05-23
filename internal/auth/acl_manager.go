// Package auth provides authentication and authorisation primitives
// for gomq.
package auth

import (
	"fmt"
	"strings"
)

// ACLManager holds a list of rules and answers Check queries.
// Rules are evaluated in order; the first matching rule wins.
// If no rule matches, access is denied.
type ACLManager struct {
	rules []Rule
}

// NewACLManager creates an ACL manager from the given rules.
// The caller should order rules from most-specific to least-
// specific, with a final default-deny rule if desired.
func NewACLManager(rules []Rule) *ACLManager {
	return &ACLManager{rules: rules}
}

// Check returns nil if the request is allowed, or ErrAccessDenied
// if no matching allow rule exists.
func (m *ACLManager) Check(
	user, vhost string,
	resType ResourceType,
	resName string,
	perm Permission,
) error {
	for _, r := range m.rules {
		if r.Match(user, vhost, resType, resName, perm) {
			if r.Allow {
				return nil
			}
			return ErrAccessDenied
		}
	}
	return ErrAccessDenied
}

// String returns a human-readable summary of the rule list.
func (m *ACLManager) String() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("ACLManager(%d rules)", len(m.rules)))
	for i, r := range m.rules {
		act := "deny"
		if r.Allow {
			act = "allow"
		}
		b.WriteString(fmt.Sprintf(
			"\n  %d: %s %s/%s %s:%s %s",
			i, act, r.User, r.VHost,
			r.ResourceType, r.ResourceName,
			r.Permission,
		))
	}
	return b.String()
}
