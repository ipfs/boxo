package blockstore

// CloseReadWrite allows our external tests to close a read-write blockstore
// without finalizing it.
// The public API doesn't expose such a method.
// In the future, we might consider adding NewReadWrite taking io interfaces,
// meaning that the caller could be fully in control of opening and closing files.
// Another option would be to expose a "Discard" method alongside "Finalize".
func CloseReadWrite(b *ReadWrite) error {
	return b.ronly.Close()
}
