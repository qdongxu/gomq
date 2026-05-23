// Package auth provides authentication and authorisation primitives
// for gomq.
package auth

import (
	"strings"
	"testing"
)

func TestRuleMatch(t *testing.T) {
	cases := []struct {
		name    string
		rule    Rule
		user    string
		vhost   string
		resType ResourceType
		resName string
		perm    Permission
		want    bool
	}{
		{
			name: "exact match",
			rule: Rule{
				User: "alice", VHost: "/",
				ResourceType: ResExchange,
				ResourceName: "orders",
				Permission: PermWrite,
				Allow: true,
			},
			user: "alice", vhost: "/",
			resType: ResExchange, resName: "orders",
			perm: PermWrite,
			want: true,
		},
		{
			name: "user mismatch",
			rule: Rule{
				User: "alice", VHost: "/",
				ResourceType: ResExchange,
				ResourceName: "orders",
				Permission: PermWrite,
				Allow: true,
			},
			user: "bob", vhost: "/",
			resType: ResExchange, resName: "orders",
			perm: PermWrite,
			want: false,
		},
		{
			name: "wildcard user",
			rule: Rule{
				User: "*", VHost: "/",
				ResourceType: ResExchange,
				ResourceName: "orders",
				Permission: PermWrite,
				Allow: true,
			},
			user: "bob", vhost: "/",
			resType: ResExchange, resName: "orders",
			perm: PermWrite,
			want: true,
		},
		{
			name: "wildcard resource name",
			rule: Rule{
				User: "alice", VHost: "/",
				ResourceType: ResQueue,
				ResourceName: "*",
				Permission: PermRead,
				Allow: true,
			},
			user: "alice", vhost: "/",
			resType: ResQueue, resName: "tasks.001",
			perm: PermRead,
			want: true,
		},
		{
			name: "wildcard permission",
			rule: Rule{
				User: "admin", VHost: "/",
				ResourceType: ResExchange,
				ResourceName: "*",
				Permission: "*",
				Allow: true,
			},
			user: "admin", vhost: "/",
			resType: ResExchange, resName: "logs",
			perm: PermConfigure,
			want: true,
		},
		{
			name: "deny rule still matches",
			rule: Rule{
				User: "guest", VHost: "/",
				ResourceType: ResExchange,
				ResourceName: "amq.direct",
				Permission: PermWrite,
				Allow: false,
			},
			user: "guest", vhost: "/",
			resType: ResExchange, resName: "amq.direct",
			perm: PermWrite,
			want: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := c.rule.Match(
				c.user, c.vhost,
				c.resType, c.resName,
				c.perm,
			)
			if got != c.want {
				t.Fatalf(
					"Match(%q,%q,%q,%q,%q) = %v, want %v",
					c.user, c.vhost,
					c.resType, c.resName,
					c.perm,
					got, c.want,
				)
			}
		})
	}
}

func TestACLManagerDenyFirst(t *testing.T) {
	m := NewACLManager([]Rule{
		{User: "admin", VHost: "/",
			ResourceType: "*", ResourceName: "*",
			Permission: "*", Allow: true},
		{User: "guest", VHost: "/",
			ResourceType: ResExchange,
			ResourceName: "amq.direct",
			Permission: PermWrite, Allow: false},
		{User: "guest", VHost: "/",
			ResourceType: ResQueue, ResourceName: "*",
			Permission: PermRead, Allow: true},
		{User: "*", VHost: "*",
			ResourceType: "*", ResourceName: "*",
			Permission: "*", Allow: false},
	})

	cases := []struct {
		name    string
		user    string
		vhost   string
		resType ResourceType
		resName string
		perm    Permission
		want    bool
	}{
		{
			name: "admin all powerful",
			user: "admin", vhost: "/",
			resType: ResExchange, resName: "secret",
			perm: PermWrite,
			want: true,
		},
		{
			name: "guest can read queue",
			user: "guest", vhost: "/",
			resType: ResQueue, resName: "tasks",
			perm: PermRead,
			want: true,
		},
		{
			name: "guest denied write exchange",
			user: "guest", vhost: "/",
			resType: ResExchange, resName: "amq.direct",
			perm: PermWrite,
			want: false,
		},
		{
			name: "unknown user default deny",
			user: "hacker", vhost: "/",
			resType: ResQueue, resName: "x",
			perm: PermRead,
			want: false,
		},
		{
			name: "guest cannot configure",
			user: "guest", vhost: "/",
			resType: ResQueue, resName: "tasks",
			perm: PermConfigure,
			want: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := m.Check(c.user, c.vhost,
				c.resType, c.resName, c.perm)
			got := err == nil
			if got != c.want {
				t.Fatalf(
					"Check(%q,%q,%q,%q,%q) error=%v, want=%v",
					c.user, c.vhost,
					c.resType, c.resName,
					c.perm,
					err, c.want,
				)
			}
		})
	}
}

func TestACLManagerEmptyDenies(t *testing.T) {
	m := NewACLManager([]Rule{})
	if err := m.Check("alice", "/", ResQueue, "q", PermRead); err == nil {
		t.Fatal("expected deny on empty rule list")
	}
}

func TestACLManagerString(t *testing.T) {
	m := NewACLManager([]Rule{
		{User: "u", VHost: "/", ResourceType: ResQueue,
			ResourceName: "q", Permission: PermRead, Allow: true},
	})
	s := m.String()
	if !strings.Contains(s, "u") || !strings.Contains(s, "queue") {
		t.Fatalf("unexpected String(): %s", s)
	}
}
