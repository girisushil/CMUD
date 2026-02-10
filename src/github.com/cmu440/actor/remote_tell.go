package actor

import (
	"net/rpc"
)

// RPC service for remote tells
type RemoteTellAPI struct {
	system *ActorSystem
}

type RemoteTellArgs struct {
	Recipient *ActorRef
	Message   []byte
}

type RemoteTellReply struct {
}

// Handler for the RPC
func (api *RemoteTellAPI) Tell(args *RemoteTellArgs, reply *RemoteTellReply) error {
	api.system.tellFromRemote(args.Recipient, args.Message)
	return nil
}

// Calls system.tellFromRemote(ref, mars) on the remote ActorSystem listening
// on ref.Address.
//
// This function should NOT wait for a reply from the remote system before
// returning, to allow sending multiple messages in a row more quickly.
// It should ensure that messages are delivered in-order to the remote system.
// (You may assume that remoteTell is not called multiple times
// concurrently with the same ref.Address).
func remoteTell(client *rpc.Client, ref *ActorRef, mars []byte) {
	args := &RemoteTellArgs{
		Recipient: ref,
		Message:   mars,
	}
	reply := &RemoteTellReply{}

	// Use asynchronous call (Go) to not wait for reply and allow higher throughput,
	// satisfying the requirement "should NOT wait for a reply".
	// TCP connection ensures in-order delivery.
	client.Go("RemoteTellAPI.Tell", args, reply, nil)
}

// Registers an RPC handler on server for remoteTell calls to system.
//
// You do not need to start the server's listening on the network;
// just register a handler struct that handles remoteTell RPCs by calling
// system.tellFromRemote(ref, mars).
func registerRemoteTells(system *ActorSystem, server *rpc.Server) error {
	return server.RegisterName("RemoteTellAPI", &RemoteTellAPI{system: system})
}
