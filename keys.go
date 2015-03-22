package leviq

import "bytes"

// splitKey deconstructs a DB key into an item key for the given namespace. If
// the key doesn't match the namespace, nil is returned.
func splitKey(ns, key []byte) (k []byte) {
	if ns == nil {
		return key
	}
	if !bytes.HasPrefix(key, ns) {
		return nil
	}
	return key[len(ns):]
}

// joinKey constructs a DB key from a namespace and an item key (by popping a
// NUL in the middle).
func joinKey(ns, k []byte) (key []byte) {
	if ns == nil {
		return k
	}
	return append(ns[:], k...)
}
