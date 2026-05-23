// Package auth provides authentication and authorisation primitives
// for gomq.
package auth

import (
	"fmt"
)

// ResourceType names the kind of AMQP resource being accessed.
type ResourceType string

const (
	ResExchange ResourceType = "exchange"
	ResQueue    ResourceType = "queue"
	ResTopic    ResourceType = "topic"
)

// Permission names the type of access requested.
type Permission string

const (
	PermConfigure Permission = "configure"
	PermWrite     Permission = "write"
	PermRead      Permission = "read"
)

// Rule is a single ACL entry.  Wildcard "*" is supported in
// User, VHost, ResourceName and Permission fields.
type Rule struct {
	User         string     `toml:"user"`
	VHost        string     `toml:"vhost"`
	ResourceType ResourceType `toml:"resource_type"`
	ResourceName string     `toml:"resource_name"`
	Permission   Permission `toml:"permission"`
	Allow        bool       `toml:"allow"`
}

// Match reports whether this rule matches the given access request.
// A deny rule (Allow == false) that matches is treated as a match
// just like an allow rule; the caller decides the semantic.
func (r *Rule) Match(
	user, vhost string,
	resType ResourceType,
	resName string,
	perm Permission,
) bool {
	if !matchWildcard(r.User, user) {
		return false
	}
	if !matchWildcard(r.VHost, vhost) {
		return false
	}
	if !matchWildcard(string(r.ResourceType), string(resType)) {
		return false
	}
	if !matchWildcard(r.ResourceName, resName) {
		return false
	}
	if !matchWildcard(string(r.Permission), string(perm)) {
		return false
	}
	return true
}

// matchWildcard compares two strings; a literal "*" in the pattern
// matches any value.
func matchWildcard(pattern, value string) bool {
	if pattern == "*" {
		return true
	}
	return pattern == value
}

// Access represents a concrete authorisation request.
type Access struct {
	User         string
	VHost        string
	ResourceType ResourceType
	ResourceName string
	Permission   Permission
}

// ErrAccessDenied is returned when no matching allow rule exists.
var ErrAccessDenied = fmt.Errorf("access refused")
