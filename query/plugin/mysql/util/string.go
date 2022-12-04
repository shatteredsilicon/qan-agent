package util

import "strings"

// Ident wrap the db and names in ` to make them legit
func Ident(db, name string) string {
	// Wrap the idents in ` to handle space and weird chars.
	if db != "" {
		db = "`" + db + "`"
	}
	if name != "" {
		name = "`" + name + "`"
	}
	// Join the idents if there's two, else return whichever was given.
	if db != "" && name != "" {
		return db + "." + name
	} else if name != "" {
		return name
	} else {
		return db
	}
}

// EscapeString escape special characters in database system
func EscapeString(v string) string {
	return strings.NewReplacer(
		"\x00", "\\0",
		"\n", "\\n",
		"\r", "\\r",
		"\x1a", "\\Z",
		"'", "\\'",
		"\"", "\\\"",
		"\\", "\\\\",
	).Replace(v)
}
