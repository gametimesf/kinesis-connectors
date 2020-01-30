package connector

// Checkpoint interface for functions that checkpoints need to
// implement in order to track consumer progress.
type Checkpoint interface {
	CheckpointExists(string) bool
	SequenceNumber(string) string
	SetCheckpoint(string, string)
}
