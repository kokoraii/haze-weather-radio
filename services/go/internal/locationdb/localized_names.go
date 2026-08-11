package locationdb

import (
	"database/sql"
	"path/filepath"
	"strings"
)

// LoadLocalizedNamesByIdentifier returns the preferred localized name for each
// identifier in one scheme from the optional compact Canadian core catalog.
func LoadLocalizedNamesByIdentifier(baseDir string, scheme string, locale string) (map[string]string, bool) {
	baseDir = strings.TrimSpace(baseDir)
	if baseDir == "" {
		baseDir = "."
	}
	scheme = strings.ToLower(strings.TrimSpace(scheme))
	locale = strings.ToLower(strings.TrimSpace(locale))
	if scheme == "" || locale == "" {
		return nil, false
	}

	path := filepath.Clean(filepath.Join(baseDir, populationCoreRelPath))
	db, err := sql.Open("sqlite", sqliteReadOnlyDSN(path))
	if err != nil {
		return nil, false
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	rows, err := db.Query(`
		SELECT i.value, n.name
		FROM identifiers i
		JOIN entities e ON e.entity_pk = i.entity_pk
		JOIN names n ON n.entity_pk = i.entity_pk
		WHERE LOWER(i.scheme) = ?
		  AND LOWER(COALESCE(n.locale, '')) LIKE ?
		  AND LOWER(COALESCE(e.lifecycle_status, '')) <> 'inactive'
		ORDER BY LOWER(i.value), n.is_primary DESC,
		         CASE WHEN LOWER(n.name_kind) = 'canonical' THEN 0 ELSE 1 END,
		         n.name_pk
	`, scheme, locale+"%")
	if err != nil {
		return nil, false
	}
	defer rows.Close()

	names := map[string]string{}
	for rows.Next() {
		var identifier string
		var name string
		if err := rows.Scan(&identifier, &name); err != nil {
			return nil, false
		}
		identifier = strings.ToLower(strings.TrimSpace(identifier))
		name = strings.TrimSpace(name)
		if identifier == "" || name == "" {
			continue
		}
		if _, exists := names[identifier]; !exists {
			names[identifier] = name
		}
	}
	if err := rows.Err(); err != nil {
		return nil, false
	}
	return names, true
}
