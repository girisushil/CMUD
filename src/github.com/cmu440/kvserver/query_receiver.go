package kvserver

import (
	"github.com/cmu440/actor"
	"github.com/cmu440/kvcommon"
)

// RPC handler implementing the kvcommon.QueryReceiver interface.
// There is one queryReceiver per queryActor, each running on its own port,
// created and registered for RPCs in NewServer.
//
// A queryReceiver MUST answer RPCs by sending a message to its query
// actor and getting a response message from that query actor (via
// ActorSystem's NewChannelRef). It must NOT attempt to answer queries
// using its own state, and it must NOT directly coordinate with other
// queryReceivers - all coordination is done within the actor system
// by its query actor.
type queryReceiver struct {
	system   *actor.ActorSystem
	actorRef *actor.ActorRef
}

// Get implements kvcommon.QueryReceiver.Get.
func (rcvr *queryReceiver) Get(args kvcommon.GetArgs, reply *kvcommon.GetReply) error {
	// Create a temporary channel to receive the actor's response
	senderRef, respCh := rcvr.system.NewChannelRef()

	// Send request to the actor
	rcvr.system.Tell(rcvr.actorRef, GetMessage{Key: args.Key, Sender: senderRef})

	// Wait for response
	respAny := <-respCh
	resp := respAny.(GetReplyMessage)

	reply.Value = resp.Value
	reply.Ok = resp.Ok
	return nil
}

// List implements kvcommon.QueryReceiver.List.
func (rcvr *queryReceiver) List(args kvcommon.ListArgs, reply *kvcommon.ListReply) error {
	senderRef, respCh := rcvr.system.NewChannelRef()
	rcvr.system.Tell(rcvr.actorRef, ListMessage{Prefix: args.Prefix, Sender: senderRef})

	respAny := <-respCh
	resp := respAny.(ListReplyMessage)

	reply.Entries = resp.Entries
	return nil
}

// Put implements kvcommon.QueryReceiver.Put.
func (rcvr *queryReceiver) Put(args kvcommon.PutArgs, reply *kvcommon.PutReply) error {
	senderRef, respCh := rcvr.system.NewChannelRef()
	rcvr.system.Tell(rcvr.actorRef, PutMessage{Key: args.Key, Value: args.Value, Sender: senderRef})

	// Wait for the actor to acknowledge the Put to ensure it's processed
	<-respCh
	return nil
}
