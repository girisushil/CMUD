package kvserver

import (
	"encoding/gob"
	"strings"
	"time"

	"github.com/cmu440/actor"
)

/*
Sync Strategy Documentation (for manual grading)

Our synchronization approach revolves around a simple, timed push mechanism for high availability. When a client performs a `Put`, the receiving actor immediately tags the key-value pair with a high-resolution timestamp and its unique ID—our foundation for Last-Writer-Wins (LWW) conflict resolution—and buffers the update locally. To ensure rapid propagation, a periodic `TickMessage` fires every **100 milliseconds**, instructing the actor to compile and send all buffered updates in a single `SyncMessage`. This clockwork timing naturally caps our traffic at **10 messages per second** per connection, satisfying the frequency limit for both local and remote peers. For cross-server communication, we conserve bandwidth by only pushing updates to one designated **contact actor** on each remote server. Additionally, when a new server starts up, one of its actors immediately sends a `FetchMessage` to all existing contact points, retrieving a full snapshot of the store; this initial fetch also serves to notify the older servers of the newcomer's address, establishing the necessary bi-directional sync paths for future operations. Every incoming `SyncMessage` is processed using the LWW merge rule to ensure the system remains eventually consistent.
*/

// Implement your queryActor in this file.

type GetMessage struct {
	Key    string
	Sender *actor.ActorRef
}

type GetReplyMessage struct {
	Value string
	Ok    bool
}

type PutMessage struct {
	Key       string
	Value     string
	Sender    *actor.ActorRef
	Timestamp int64
	WriterID  string
	IsSync    bool
}

type PutReplyMessage struct{}

type ListMessage struct {
	Prefix string
	Sender *actor.ActorRef
}

type ListReplyMessage struct {
	Entries map[string]string
}

type SyncMessage struct {
	Updates []PutMessage
}

type AddPeersMessage struct {
	Peers []*actor.ActorRef
}

type FetchMessage struct {
	Sender *actor.ActorRef
}

type TickMessage struct{}

func init() {
	gob.Register(GetMessage{})
	gob.Register(GetReplyMessage{})
	gob.Register(PutMessage{})
	gob.Register(PutReplyMessage{})
	gob.Register(ListMessage{})
	gob.Register(ListReplyMessage{})
	gob.Register(SyncMessage{})
	gob.Register(AddPeersMessage{})
	gob.Register(FetchMessage{})
	gob.Register(TickMessage{})
}

type Entry struct {
	Value     string
	Timestamp int64
	WriterID  string
}

// Helper struct to track how an update should be propagated
type pendingUpdate struct {
	msg          PutMessage
	sendToLocal  bool
	sendToRemote bool
}

type queryActor struct {
	context *actor.ActorContext
	store   map[string]Entry

	localPeers  []*actor.ActorRef
	remotePeers []*actor.ActorRef

	// Map ensures only the latest update per key is queued (Size optimization)
	pendingUpdates map[string]pendingUpdate
}

func newQueryActor(context *actor.ActorContext) actor.Actor {
	a := &queryActor{
		context:        context,
		store:          make(map[string]Entry),
		localPeers:     make([]*actor.ActorRef, 0),
		remotePeers:    make([]*actor.ActorRef, 0),
		pendingUpdates: make(map[string]pendingUpdate),
	}
	a.context.TellAfter(a.context.Self, TickMessage{}, 100*time.Millisecond)
	return a
}

func (a *queryActor) OnMessage(message any) error {
	switch m := message.(type) {
	case GetMessage:
		val, ok := a.store[m.Key]
		a.context.Tell(m.Sender, GetReplyMessage{Value: val.Value, Ok: ok})

	case PutMessage:
		timestamp := m.Timestamp
		writerID := m.WriterID

		var clientUpdate PutMessage
		if !m.IsSync {
			// Client Put: Generate metadata
			timestamp = time.Now().UnixNano()
			writerID = a.context.Self.Uid()
			clientUpdate = PutMessage{
				Key:       m.Key,
				Value:     m.Value,
				Timestamp: timestamp,
				WriterID:  writerID,
				IsSync:    true,
			}
		}

		shouldUpdate := false
		existing, exists := a.store[m.Key]
		if !exists {
			shouldUpdate = true
		} else {
			if timestamp > existing.Timestamp {
				shouldUpdate = true
			} else if timestamp == existing.Timestamp {
				if writerID > existing.WriterID {
					shouldUpdate = true
				}
			}
		}

		if shouldUpdate {
			a.store[m.Key] = Entry{
				Value:     m.Value,
				Timestamp: timestamp,
				WriterID:  writerID,
			}

			// Propagation Logic:
			if !m.IsSync {
				// Propagate to EVERYONE (Local + Remote)
				a.pendingUpdates[m.Key] = pendingUpdate{
					msg:          clientUpdate,
					sendToLocal:  true,
					sendToRemote: true,
				}
			} else {
				// Check origin
				isSenderRemote := m.Sender != nil && !a.context.IsLocal(m.Sender)
				updateToPropagate := m
				updateToPropagate.IsSync = true

				if isSenderRemote {
					// Received from Remote, Propagate to LOCAL only
					// Don't send back to remotes.
					a.pendingUpdates[m.Key] = pendingUpdate{
						msg:          updateToPropagate,
						sendToLocal:  true,
						sendToRemote: false,
					}
				} else {
					// Received from Local: Propagate to REMOTE only
					// (local neighbor sent this; other locals got it from them.
					a.pendingUpdates[m.Key] = pendingUpdate{
						msg:          updateToPropagate,
						sendToLocal:  false,
						sendToRemote: true,
					}
				}
			}
		}

		if !m.IsSync && m.Sender != nil {
			a.context.Tell(m.Sender, PutReplyMessage{})
		}

	case ListMessage:
		results := make(map[string]string)
		for k, v := range a.store {
			if strings.HasPrefix(k, m.Prefix) {
				results[k] = v.Value
			}
		}
		a.context.Tell(m.Sender, ListReplyMessage{Entries: results})

	case SyncMessage:
		for _, update := range m.Updates {
			a.OnMessage(update)
		}

	case AddPeersMessage:
		for _, peer := range m.Peers {
			if peer.Uid() == a.context.Self.Uid() {
				continue
			}
			if a.context.IsLocal(peer) {
				a.localPeers = append(a.localPeers, peer)
			} else {
				a.remotePeers = append(a.remotePeers, peer)
			}
		}

	case FetchMessage:
		// Add sender to remotes if not exists (Bi-directional link)
		isKnown := false
		for _, p := range a.remotePeers {
			if p.Uid() == m.Sender.Uid() {
				isKnown = true
				break
			}
		}
		if !isKnown && !a.context.IsLocal(m.Sender) {
			a.remotePeers = append(a.remotePeers, m.Sender)
		}

		updates := make([]PutMessage, 0, len(a.store))
		for k, v := range a.store {
			updates = append(updates, PutMessage{
				Key:       k,
				Value:     v.Value,
				Timestamp: v.Timestamp,
				WriterID:  v.WriterID,
				IsSync:    true,
				Sender:    a.context.Self,
			})
		}
		a.context.Tell(m.Sender, SyncMessage{Updates: updates})

	case TickMessage:
		if len(a.pendingUpdates) > 0 {
			updatesLocal := make([]PutMessage, 0)
			updatesRemote := make([]PutMessage, 0)

			for _, pu := range a.pendingUpdates {
				outMsg := pu.msg
				outMsg.Sender = a.context.Self

				if pu.sendToLocal {
					updatesLocal = append(updatesLocal, outMsg)
				}
				if pu.sendToRemote {
					updatesRemote = append(updatesRemote, outMsg)
				}
			}

			// Sync to Local Peers
			if len(updatesLocal) > 0 {
				msgLocal := SyncMessage{Updates: updatesLocal}
				for _, p := range a.localPeers {
					a.context.Tell(p, msgLocal)
				}
			}

			// Sync to Remote Peers
			if len(updatesRemote) > 0 {
				msgRemote := SyncMessage{Updates: updatesRemote}
				for _, p := range a.remotePeers {
					a.context.Tell(p, msgRemote)
				}
			}

			a.pendingUpdates = make(map[string]pendingUpdate)
		}
		a.context.TellAfter(a.context.Self, TickMessage{}, 100*time.Millisecond)
	}
	return nil
}
