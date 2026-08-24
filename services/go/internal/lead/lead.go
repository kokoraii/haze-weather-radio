// Package lead manages operator-configured alert lead-in and lead-out audio.
package lead

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	maxStatements          = 100
	maxConditions          = 32
	maxStatementNameRunes  = 120
	maxConditionValueRunes = 256
)

// Document is the managed lead.xml configuration. Statements are evaluated in
// their configured order and the first matching enabled statement is used.
type Document struct {
	Statements []Statement `json:"statements"`
}

// Statement supplies optional pre-roll and post-roll audio for a matching alert.
type Statement struct {
	Enabled    bool        `json:"enabled"`
	Name       string      `json:"name"`
	Conditions []Condition `json:"conditions"`
	LeadIn     string      `json:"lead_in"`
	LeadOut    string      `json:"lead_out"`
}

// Condition is one XML child under a statement's conditions block. Type is
// one of if, and, or. The and and or tokens join neighboring if expressions.
type Condition struct {
	Type       string `json:"type"`
	Key        string `json:"key,omitempty"`
	Location   string `json:"location,omitempty"`
	Equals     string `json:"equals,omitempty"`
	Includes   string `json:"includes,omitempty"`
	StartsWith string `json:"startswith,omitempty"`
	EndsWith   string `json:"endswith,omitempty"`
	MatchCase  bool   `json:"matchcase,omitempty"`
	MatchWhole bool   `json:"matchwhole,omitempty"`
	UseRegex   bool   `json:"useregex,omitempty"`
}

// Context exposes the CAP facts and alert location identifiers that statements
// can match. Field names are compared without case sensitivity.
type Context struct {
	Values    map[string][]string
	Locations []string
}

type documentXML struct {
	XMLName    xml.Name       `xml:"leads"`
	Statements []statementXML `xml:"lead"`
}

type statementXML struct {
	Enabled    string        `xml:"enabled,attr"`
	Name       string        `xml:"name"`
	Conditions conditionsXML `xml:"conditions"`
	Audio      audioXML      `xml:"audio"`
}

type conditionsXML struct {
	Nodes []conditionXML `xml:",any"`
}

type conditionXML struct {
	XMLName    xml.Name
	Key        string `xml:"key,attr"`
	Location   string `xml:"location,attr"`
	Equals     string `xml:"equals,attr"`
	Includes   string `xml:"includes,attr"`
	StartsWith string `xml:"startswith,attr"`
	EndsWith   string `xml:"endswith,attr"`
	MatchCase  string `xml:"matchcase,attr"`
	MatchWhole string `xml:"matchwhole,attr"`
	UseRegex   string `xml:"useregex,attr"`
}

type audioXML struct {
	LeadIn  string `xml:"lead_in"`
	LeadOut string `xml:"lead_out"`
}

// Load reads a lead configuration. A missing file is an empty, valid document
// so existing deployments can adopt the feature without a migration race.
func Load(filename string) (Document, error) {
	raw, err := os.ReadFile(filepath.Clean(filename))
	if err != nil {
		if os.IsNotExist(err) {
			return Document{}, nil
		}
		return Document{}, err
	}
	return Parse(raw)
}

// Parse validates lead.xml bytes and returns their normalized representation.
func Parse(raw []byte) (Document, error) {
	var parsed documentXML
	if err := xml.Unmarshal(raw, &parsed); err != nil {
		return Document{}, fmt.Errorf("parse lead XML: %w", err)
	}
	document := Document{Statements: make([]Statement, 0, len(parsed.Statements))}
	for index, item := range parsed.Statements {
		enabled, err := boolAttribute(item.Enabled, true)
		if err != nil {
			return Document{}, fmt.Errorf("lead %d enabled: %w", index+1, err)
		}
		statement := Statement{
			Enabled: enabled,
			Name:    item.Name,
			LeadIn:  item.Audio.LeadIn,
			LeadOut: item.Audio.LeadOut,
		}
		for nodeIndex, node := range item.Conditions.Nodes {
			condition, err := conditionFromXML(node)
			if err != nil {
				return Document{}, fmt.Errorf("lead %d condition %d: %w", index+1, nodeIndex+1, err)
			}
			statement.Conditions = append(statement.Conditions, condition)
		}
		document.Statements = append(document.Statements, statement)
	}
	return Normalize(document)
}

// Encode produces a stable, human-editable lead.xml document.
func Encode(document Document) ([]byte, error) {
	normalized, err := Normalize(document)
	if err != nil {
		return nil, err
	}
	encoded := documentXML{Statements: make([]statementXML, 0, len(normalized.Statements))}
	for _, statement := range normalized.Statements {
		item := statementXML{
			Enabled: strconv.FormatBool(statement.Enabled),
			Name:    statement.Name,
			Audio: audioXML{
				LeadIn:  statement.LeadIn,
				LeadOut: statement.LeadOut,
			},
		}
		for _, condition := range statement.Conditions {
			item.Conditions.Nodes = append(item.Conditions.Nodes, conditionToXML(condition))
		}
		encoded.Statements = append(encoded.Statements, item)
	}
	raw, err := xml.MarshalIndent(encoded, "", "    ")
	if err != nil {
		return nil, err
	}
	comment := "<!-- Alert lead-in and lead-out statements. The first matching enabled lead is used. -->\n"
	return append([]byte(xml.Header+comment), append(raw, '\n')...), nil
}

// Normalize validates a document and returns a portable, clean copy.
func Normalize(document Document) (Document, error) {
	if len(document.Statements) > maxStatements {
		return Document{}, fmt.Errorf("at most %d lead statements are allowed", maxStatements)
	}
	normalized := Document{Statements: make([]Statement, 0, len(document.Statements))}
	seenNames := map[string]struct{}{}
	for index, source := range document.Statements {
		statement := source
		statement.Name = strings.TrimSpace(statement.Name)
		if statement.Name == "" {
			return Document{}, fmt.Errorf("lead %d name is required", index+1)
		}
		if utf8.RuneCountInString(statement.Name) > maxStatementNameRunes {
			return Document{}, fmt.Errorf("lead %q name is too long", statement.Name)
		}
		nameKey := strings.ToLower(statement.Name)
		if _, exists := seenNames[nameKey]; exists {
			return Document{}, fmt.Errorf("lead name %q is duplicated", statement.Name)
		}
		seenNames[nameKey] = struct{}{}
		var err error
		statement.LeadIn, err = NormalizeAudioPath(statement.LeadIn)
		if err != nil {
			return Document{}, fmt.Errorf("lead %q lead_in: %w", statement.Name, err)
		}
		statement.LeadOut, err = NormalizeAudioPath(statement.LeadOut)
		if err != nil {
			return Document{}, fmt.Errorf("lead %q lead_out: %w", statement.Name, err)
		}
		if statement.Enabled && statement.LeadIn == "" && statement.LeadOut == "" {
			return Document{}, fmt.Errorf("lead %q needs a lead_in or lead_out audio file", statement.Name)
		}
		if len(statement.Conditions) > maxConditions {
			return Document{}, fmt.Errorf("lead %q has more than %d conditions", statement.Name, maxConditions)
		}
		statement.Conditions = append([]Condition(nil), statement.Conditions...)
		for conditionIndex := range statement.Conditions {
			condition, err := normalizeCondition(statement.Conditions[conditionIndex])
			if err != nil {
				return Document{}, fmt.Errorf("lead %q condition %d: %w", statement.Name, conditionIndex+1, err)
			}
			statement.Conditions[conditionIndex] = condition
		}
		normalized.Statements = append(normalized.Statements, statement)
	}
	return normalized, nil
}

// Select returns the first enabled statement matching the alert context.
func (document Document) Select(context Context) (Statement, bool) {
	for _, statement := range document.Statements {
		if statement.Enabled && statement.Matches(context) {
			return statement, true
		}
	}
	return Statement{}, false
}

// Matches evaluates this statement's ordered condition tokens.
func (statement Statement) Matches(context Context) bool {
	if len(statement.Conditions) == 0 {
		return true
	}
	seenExpression := false
	matched := false
	pendingOperator := "and"
	for _, condition := range statement.Conditions {
		switch condition.Type {
		case "and", "or":
			pendingOperator = condition.Type
		case "if":
			value := condition.Matches(context)
			if !seenExpression {
				matched = value
				seenExpression = true
			} else if pendingOperator == "or" {
				matched = matched || value
			} else {
				matched = matched && value
			}
			pendingOperator = "and"
		}
	}
	return seenExpression && matched
}

// Matches checks one condition against the supplied CAP fields or locations.
func (condition Condition) Matches(context Context) bool {
	values := []string{}
	if condition.Location != "" {
		values = append(values, context.Locations...)
	} else {
		wantedKey := strings.ToLower(strings.TrimSpace(condition.Key))
		for key, items := range context.Values {
			if strings.EqualFold(strings.TrimSpace(key), wantedKey) {
				values = append(values, items...)
			}
		}
	}
	for _, value := range values {
		if condition.matchesText(value) {
			return true
		}
	}
	return false
}

func (condition Condition) matchesText(raw string) bool {
	value := strings.TrimSpace(raw)
	operator, expected := conditionOperator(condition)
	if !condition.MatchCase {
		value = strings.ToLower(value)
		expected = strings.ToLower(expected)
	}
	if condition.UseRegex {
		pattern := expected
		switch operator {
		case "startswith":
			pattern = "^(?:" + pattern + ")"
		case "endswith":
			pattern = "(?:" + pattern + ")$"
		case "equals":
			pattern = "^(?:" + pattern + ")$"
		case "includes":
			if condition.MatchWhole {
				pattern = "^(?:" + pattern + ")$"
			}
		}
		compiled, err := regexp.Compile(pattern)
		return err == nil && compiled.MatchString(value)
	}
	switch operator {
	case "equals":
		return value == expected
	case "includes":
		return strings.Contains(value, expected)
	case "startswith":
		return strings.HasPrefix(value, expected)
	case "endswith":
		return strings.HasSuffix(value, expected)
	default:
		return false
	}
}

// NormalizeAudioPath permits only supported audio files within bundle/audio.
func NormalizeAudioPath(raw string) (string, error) {
	value := strings.TrimSpace(strings.ReplaceAll(raw, "\\", "/"))
	if value == "" {
		return "", nil
	}
	if strings.ContainsRune(value, 0) || strings.HasPrefix(value, "/") || strings.Contains(value, ":") {
		return "", fmt.Errorf("audio path must be a relative path under audio")
	}
	clean := path.Clean(value)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || clean == "audio" || !strings.HasPrefix(clean, "audio/") {
		return "", fmt.Errorf("audio path must be under audio")
	}
	extension := strings.ToLower(path.Ext(clean))
	if !supportedAudioExtension(extension) {
		return "", fmt.Errorf("audio file type %q is not supported", extension)
	}
	return clean, nil
}

// ResolveAudioPath returns the validated absolute bundle/audio path and blocks
// any symlink that resolves outside the managed audio directory.
func ResolveAudioPath(baseDir string, raw string) (string, error) {
	relative, err := NormalizeAudioPath(raw)
	if err != nil {
		return "", err
	}
	if relative == "" {
		return "", nil
	}
	root, err := filepath.Abs(filepath.Clean(baseDir))
	if err != nil {
		return "", err
	}
	audioRoot := filepath.Join(root, "audio")
	target := filepath.Join(root, filepath.FromSlash(relative))
	if !pathInside(audioRoot, target) {
		return "", fmt.Errorf("audio path must stay within bundle audio")
	}
	if resolved, resolveErr := filepath.EvalSymlinks(target); resolveErr == nil && !pathInside(audioRoot, resolved) {
		return "", fmt.Errorf("audio path symlink leaves bundle audio")
	}
	return target, nil
}

// SupportedAudioExtensions returns the extensions accepted by lead statements.
func SupportedAudioExtensions() []string {
	return []string{".aac", ".flac", ".m4a", ".mp3", ".ogg", ".opus", ".pcm16le", ".s16le", ".wav"}
}

func supportedAudioExtension(extension string) bool {
	for _, allowed := range SupportedAudioExtensions() {
		if extension == allowed {
			return true
		}
	}
	return false
}

func pathInside(root string, target string) bool {
	root, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return false
	}
	target, err = filepath.Abs(filepath.Clean(target))
	if err != nil {
		return false
	}
	relative, err := filepath.Rel(root, target)
	return err == nil && relative != "." && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func conditionFromXML(source conditionXML) (Condition, error) {
	typeName := strings.ToLower(strings.TrimSpace(source.XMLName.Local))
	if typeName == "" {
		return Condition{}, fmt.Errorf("condition type is required")
	}
	matchCase, err := boolAttribute(source.MatchCase, false)
	if err != nil {
		return Condition{}, fmt.Errorf("matchcase: %w", err)
	}
	matchWhole, err := boolAttribute(source.MatchWhole, false)
	if err != nil {
		return Condition{}, fmt.Errorf("matchwhole: %w", err)
	}
	useRegex, err := boolAttribute(source.UseRegex, false)
	if err != nil {
		return Condition{}, fmt.Errorf("useregex: %w", err)
	}
	return Condition{
		Type:       typeName,
		Key:        source.Key,
		Location:   source.Location,
		Equals:     source.Equals,
		Includes:   source.Includes,
		StartsWith: source.StartsWith,
		EndsWith:   source.EndsWith,
		MatchCase:  matchCase,
		MatchWhole: matchWhole,
		UseRegex:   useRegex,
	}, nil
}

func conditionToXML(condition Condition) conditionXML {
	return conditionXML{
		XMLName:    xml.Name{Local: condition.Type},
		Key:        condition.Key,
		Location:   condition.Location,
		Equals:     condition.Equals,
		Includes:   condition.Includes,
		StartsWith: condition.StartsWith,
		EndsWith:   condition.EndsWith,
		MatchCase:  strconv.FormatBool(condition.MatchCase),
		MatchWhole: strconv.FormatBool(condition.MatchWhole),
		UseRegex:   strconv.FormatBool(condition.UseRegex),
	}
}

func normalizeCondition(source Condition) (Condition, error) {
	condition := source
	condition.Type = strings.ToLower(strings.TrimSpace(condition.Type))
	switch condition.Type {
	case "and", "or":
		if condition.Key != "" || condition.Location != "" || condition.Equals != "" || condition.Includes != "" || condition.StartsWith != "" || condition.EndsWith != "" || condition.MatchCase || condition.MatchWhole || condition.UseRegex {
			return Condition{}, fmt.Errorf("%s cannot have match attributes", condition.Type)
		}
		return Condition{Type: condition.Type}, nil
	case "if":
	default:
		return Condition{}, fmt.Errorf("type must be if, and, or")
	}
	condition.Key = strings.TrimSpace(condition.Key)
	condition.Location = strings.TrimSpace(condition.Location)
	if (condition.Key == "") == (condition.Location == "") {
		return Condition{}, fmt.Errorf("if needs exactly one of key or location")
	}
	if utf8.RuneCountInString(firstNonBlank(condition.Key, condition.Location)) > maxConditionValueRunes {
		return Condition{}, fmt.Errorf("match target is too long")
	}
	values := []struct {
		name  string
		value *string
	}{
		{"equals", &condition.Equals},
		{"includes", &condition.Includes},
		{"startswith", &condition.StartsWith},
		{"endswith", &condition.EndsWith},
	}
	selected := 0
	for index := range values {
		*values[index].value = strings.TrimSpace(*values[index].value)
		if *values[index].value != "" {
			selected++
			if utf8.RuneCountInString(*values[index].value) > maxConditionValueRunes {
				return Condition{}, fmt.Errorf("%s value is too long", values[index].name)
			}
		}
	}
	if selected != 1 {
		return Condition{}, fmt.Errorf("if needs exactly one match operator")
	}
	if condition.UseRegex {
		_, expected := conditionOperator(condition)
		if _, err := regexp.Compile(expected); err != nil {
			return Condition{}, fmt.Errorf("invalid regular expression: %w", err)
		}
	}
	return condition, nil
}

func conditionOperator(condition Condition) (string, string) {
	for _, item := range []struct {
		name  string
		value string
	}{
		{"equals", condition.Equals},
		{"includes", condition.Includes},
		{"startswith", condition.StartsWith},
		{"endswith", condition.EndsWith},
	} {
		if item.value != "" {
			return item.name, item.value
		}
	}
	return "", ""
}

func boolAttribute(raw string, fallback bool) (bool, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return fallback, nil
	}
	switch strings.ToLower(value) {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("must be true or false")
	}
}

func firstNonBlank(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

// SortedAudioPaths returns a unique stable list useful for safe UI pickers.
func SortedAudioPaths(paths []string) []string {
	set := map[string]struct{}{}
	for _, raw := range paths {
		if normalized, err := NormalizeAudioPath(raw); err == nil && normalized != "" {
			set[normalized] = struct{}{}
		}
	}
	result := make([]string, 0, len(set))
	for value := range set {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

// CompactXML returns a comparison-stable XML representation used by tests and
// configuration writers that need to detect no-op saves.
func CompactXML(document Document) ([]byte, error) {
	raw, err := Encode(document)
	if err != nil {
		return nil, err
	}
	return bytes.TrimSpace(raw), nil
}
