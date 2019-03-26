package tree

import "strings"

// QualifiedName is column QualifiedName
type QualifiedName struct {
	// Parts is list of string
	Parts []string
	// OriginalParts is list of string
	OriginalParts []string
}

// NewQualifiedName creates QualifiedName
func NewQualifiedName(originalParts, parts []string) *QualifiedName {
	return &QualifiedName{
		Parts:         parts,
		OriginalParts: originalParts,
	}
}

// String returns string
func (n *QualifiedName) String() string {
	return strings.Join(n.Parts, ".")
}
