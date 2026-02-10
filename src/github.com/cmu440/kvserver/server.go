// Package kvserver implements the backend server for a
// geographically distributed, highly available, NoSQL key-value store.

package kvserver

import (
	"encoding/json"
	"fmt"
	"net"
	"net/rpc"

	"github.com/cmu440/actor"
)

// A single server in the key-value store, running some number of
// query actors - nominally one per CPU core. Each query actor
// provides a key/value storage service on its own port.
//
// Different query actors (both within this server and across connected
// servers) periodically sync updates (Puts) following an eventually
// consistent, last-writer-wins strategy.

type Server struct {
	system      *actor.ActorSystem
	queryActors []*actor.ActorRef
	listeners   []net.Listener
}

// Struct used for serializing server descriptions
type ServerDesc struct {
	Actors []*actor.ActorRef
}

// OPTIONAL: Error handler for ActorSystem.OnError.
//
// Print the error or call debug.PrintStack() in this function.
// When starting an ActorSystem, call ActorSystem.OnError(errorHandler).
// This can help debug server-side errors more easily.
func errorHandler(err error) {
	// Optional error logging
	// fmt.Println("ActorSystem Error:", err)
}

// Starts a server running queryActorCount query actors.
//
// The server's actor system listens for remote messages (from other actor
// systems) on startPort. The server listens for RPCs from kvclient.Clients
// on ports [startPort + 1, startPort + 2, ..., startPort + queryActorCount].
// Each of these "query RPC servers" answers queries by asking a specific
// query actor.
//
// remoteDescs contains a "description" string for each existing server in the
// key-value store. Specifically, each slice entry is the desc returned by
// an existing server's own NewServer call. The description strings are opaque
// to callers, but typically an implementation uses JSON-encoded data containing,
// e.g., actor.ActorRef's that remote servers' actors should contact.
//
// Before returning, NewServer starts the ActorSystem, all query actors, and
// all query RPC servers. If there is an error starting anything, that error is
// returned instead.

func NewServer(startPort int, queryActorCount int, remoteDescs []string) (server *Server, desc string, err error) {
	system, err := actor.NewActorSystem(startPort)
	if err != nil {
		return nil, "", err
	}
	system.OnError(errorHandler)

	server = &Server{
		system:      system,
		queryActors: make([]*actor.ActorRef, queryActorCount),
		listeners:   make([]net.Listener, 0),
	}

	// Start Query Actors and RPC Servers
	for i := 0; i < queryActorCount; i++ {
		ref := system.StartActor(newQueryActor)
		server.queryActors[i] = ref

		// Start RPC server for this actor
		rpcServer := rpc.NewServer()
		err := rpcServer.RegisterName("QueryReceiver", &queryReceiver{system: system, actorRef: ref})
		if err != nil {
			server.Close()
			return nil, "", err
		}

		ln, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", startPort+1+i))
		if err != nil {
			server.Close()
			return nil, "", err
		}
		server.listeners = append(server.listeners, ln)

		go func(l net.Listener, srv *rpc.Server) {
			for {
				conn, err := l.Accept()
				if err != nil {
					return
				}
				go srv.ServeConn(conn)
			}
		}(ln, rpcServer)
	}

	// Parse Remote Descriptions
	var remoteContacts []*actor.ActorRef
	for _, rd := range remoteDescs {
		var d ServerDesc
		if err := json.Unmarshal([]byte(rd), &d); err == nil && len(d.Actors) > 0 {
			// Pick the first actor of the remote server as the contact point
			// to avoid sending redundant messages to all actors on that server.
			remoteContacts = append(remoteContacts, d.Actors[0])
		}
	}

	// Introduce Peers to Actors
	// Each actor needs to know about all other local actors, and the contact actors for remote servers.
	for _, ref := range server.queryActors {
		peers := make([]*actor.ActorRef, 0)

		// Add local peers
		for _, other := range server.queryActors {
			if other != ref {
				peers = append(peers, other)
			}
		}

		// Add remote contacts
		peers = append(peers, remoteContacts...)

		system.Tell(ref, AddPeersMessage{Peers: peers})
	}

	// Have one of our actors fetch data from the remote servers.
	// The "Fetch" logic will also introduce our actor to the remote, allowing future syncs to flow back.
	if len(remoteContacts) > 0 {
		// We only need to fetch from contacts. The fetched data will be merged
		// and then propagated to other local actors via the periodic Tick sync.
		for _, contact := range remoteContacts {
			system.Tell(contact, FetchMessage{Sender: server.queryActors[0]})
		}
	}

	// Generate Description for this server
	descStruct := ServerDesc{Actors: server.queryActors}
	bytes, _ := json.Marshal(descStruct)
	desc = string(bytes)

	return server, desc, nil
}

// OPTIONAL: Closes the server, including its actor system
// and all RPC servers.
//
// You are not required to implement this function for full credit; the tests end
// by calling Close but do not check that it does anything. However, you
// may find it useful to implement this so that you can run multiple/repeated
// tests in the same "go test" command without cross-test interference (in
// particular, old test servers' squatting on ports.)
//
// Likewise, you may find it useful to close a partially-started server's
// resources if there is an error in NewServer.
func (server *Server) Close() {
	if server.system != nil {
		server.system.Close()
	}
	for _, ln := range server.listeners {
		ln.Close()
	}
}
