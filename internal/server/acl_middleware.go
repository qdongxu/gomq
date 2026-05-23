// acl_middleware.go wraps AMQP method handlers with ACL checks.
package server

import (
	"github.com/qdongxu/gomq/internal/auth"
)

// aclCheck wraps a MethodHandler so that the requested action is
// validated against the server's ACLManager before the inner handler
// runs.  If no ACLManager is configured the check is skipped.
func aclCheck(
	srv *Server,
	resType auth.ResourceType,
	perm auth.Permission,
	extractName func(*Channel, []byte) string,
	inner MethodHandler,
) MethodHandler {
	return func(ch *Channel, payload []byte) error {
		mgr := srv.ACLManager()
		if mgr != nil {
			name := ""
			if extractName != nil {
				name = extractName(ch, payload)
			}
			if err := mgr.Check(
				ch.conn.Username(),
				ch.conn.VHost(),
				resType,
				name,
				perm,
			); err != nil {
				return err
			}
		}
		return inner(ch, payload)
	}
}
