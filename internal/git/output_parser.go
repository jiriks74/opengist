package git

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"github.com/rs/zerolog/log"
	"io"
	"regexp"
	"strconv"
	"strings"
)

type File struct {
	Filename    string `json:"filename"`
	Size        uint64 `json:"size"`
	HumanSize   string `json:"human_size"`
	OldFilename string `json:"-"`
	Content     string `json:"content"`
	Truncated   bool   `json:"truncated"`
	IsCreated   bool   `json:"-"`
	IsDeleted   bool   `json:"-"`
}

type CsvFile struct {
	File
	Header []string
	Rows   [][]string
}

type Commit struct {
	Hash        string
	AuthorName  string
	AuthorEmail string
	Timestamp   string
	Changed     string
	Files       []File
}

func truncateCommandOutput(out io.Reader, maxBytes int64) (string, bool, error) {
	var buf []byte
	var err error

	if maxBytes < 0 {
		buf, err = io.ReadAll(out)
	} else {
		buf, err = io.ReadAll(io.LimitReader(out, maxBytes))
	}
	if err != nil {
		return "", false, err
	}
	truncated := maxBytes > 0 && len(buf) >= int(maxBytes)
	// Remove the last line if it's truncated
	if truncated {
		// Find the index of the last newline character
		lastNewline := bytes.LastIndexByte(buf, '\n')

		if lastNewline > 0 {
			// Trim the data buffer up to the last newline character
			buf = buf[:lastNewline]
		}
	}

	return string(buf), truncated, nil
}

// todo:
// - shortstat
// - disable empty commit (in git counts log etc)
// - lines max/bytes max by line
func parseLog(out io.Reader, maxFiles int, maxBytes int) ([]*Commit, error) {
	var commits []*Commit
	var currentCommit *Commit
	var headerParsed = false
	input := bufio.NewReaderSize(out, maxBytes)

	// Loop Commits
loopCommits:
	for {
		line, err := input.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break loopCommits
			}
			return commits, err
		}
		if len(line) > 0 && (line[len(line)-1] == '\n' || line[len(line)-1] == '\r') {
			line = line[:len(line)-1]
		}

		// Attempt to parse commit header (hash, author, mail, timestamp) or a diff
		switch line[0] {
		// Commit hash
		case 'c':
			if headerParsed {
				commits = append(commits, currentCommit)
			}
			currentCommit = &Commit{Hash: line[2:], Files: []File{}}
			continue

		// Author name
		case 'a':
			headerParsed = true
			currentCommit.AuthorName = line[2:]
			continue

		// Author email
		case 'm':
			currentCommit.AuthorEmail = line[2:]
			continue

		// Commit timestamp
		case 't':
			currentCommit.Timestamp = line[2:]
			continue

		// Commit diff
		default:
			sb := strings.Builder{}
			// Loop files in diff
		loopDiff:
			for {

				if maxFiles > -1 && len(currentCommit.Files) >= maxFiles {
					_, _ = io.Copy(io.Discard, input)
					headerParsed = false
					break loopDiff
				}
				currentFile := &File{}
				parseRename := true

			currFileLoop:
				for {
					line, err = input.ReadString('\n')
					if err != nil {
						if err != io.EOF {
							return commits, err
						}
						headerParsed = false
						break loopDiff
					}
					if line == "\n" {
						currentCommit.Files = append(currentCommit.Files, *currentFile)
						headerParsed = false
						break loopDiff
					}

					switch {
					case strings.HasPrefix(line, "diff --git"):
						break currFileLoop
					case strings.HasPrefix(line, "old mode"):
					case strings.HasPrefix(line, "new mode"):
					case strings.HasPrefix(line, "index"):
					case strings.HasPrefix(line, "similarity index"):
					case strings.HasPrefix(line, "dissimilarity index"):
						continue
					case strings.HasPrefix(line, "rename from "):
						currentFile.OldFilename = line[11 : len(line)-1]
					case strings.HasPrefix(line, "rename to "):
						currentFile.Filename = line[9 : len(line)-1]
						parseRename = false
					case strings.HasPrefix(line, "copy from "):
						currentFile.OldFilename = line[9 : len(line)-1]
					case strings.HasPrefix(line, "copy to "):
						currentFile.Filename = line[7 : len(line)-1]
						parseRename = false
					case strings.HasPrefix(line, "new file"):
						currentFile.IsCreated = true
					case strings.HasPrefix(line, "deleted file"):
						currentFile.IsDeleted = true
					case strings.HasPrefix(line, "--- "):
						name := line[4 : len(line)-1]
						if parseRename && currentFile.IsDeleted {
							currentFile.Filename = name[2:]
						} else if parseRename && strings.HasPrefix(name, "a/") {
							currentFile.OldFilename = name[2:]
						}
					case strings.HasPrefix(line, "+++ "):
						name := line[4 : len(line)-1]
						if parseRename && strings.HasPrefix(name, "b/") {
							currentFile.Filename = name[2:]
						}

						// header is finally parsed

						lineBytes, isFragment, err := parseHunks(currentFile, maxBytes, input)
						if err != nil {
							if err != io.EOF {
								return commits, err
							}
							// EOF, we are done with this file
							currentCommit.Files = append(currentCommit.Files, *currentFile)
							headerParsed = false
							break loopDiff
						}
						currentCommit.Files = append(currentCommit.Files, *currentFile)
						sb.Reset()
						_, _ = sb.Write(lineBytes)

						fmt.Print("linebytes#" + string(lineBytes) + "#\n")
						if string(lineBytes) == "" {
							headerParsed = false
							break loopDiff
						}

						for isFragment {
							lineBytes, isFragment, err = input.ReadLine()
							if err != nil {
								// Now by the definition of ReadLine this cannot be io.EOF
								return commits, fmt.Errorf("unable to ReadLine: %w", err)
							}
							_, _ = sb.Write(lineBytes)

						}
						line = sb.String()

						sb.Reset()
						break currFileLoop
					}
				}
			}
		}
		commits = append(commits, currentCommit)
	}

	return commits, nil
}

func parseHunks(currentFile *File, maxBytes int, input *bufio.Reader) (lineBytes []byte, isFragment bool, err error) {
	sb := &strings.Builder{}
	var currFileLineCount int

	for {
		for isFragment {
			currentFile.Truncated = true

			// Read the next line
			_, isFragment, err = input.ReadLine()
			if err != nil {
				return nil, false, err
			}
		}

		sb.Reset()

		// Read the next line
		lineBytes, isFragment, err = input.ReadLine()
		if err != nil {
			if err == io.EOF {
				return lineBytes, false, err
			}
			return nil, false, err
		}

		if len(lineBytes) == 0 {
			return lineBytes, false, err
		}
		if lineBytes[0] == 'd' {
			// End of hunks
			return lineBytes, isFragment, err
		}

		if maxBytes > -1 && currFileLineCount >= maxBytes {
			currentFile.Truncated = true
			continue
		}

		line := string(lineBytes)
		if isFragment {
			currentFile.Truncated = true
			for isFragment {
				lineBytes, isFragment, err = input.ReadLine()
				if err != nil {
					// Now by the definition of ReadLine this cannot be io.EOF
					return lineBytes, isFragment, fmt.Errorf("unable to ReadLine: %w", err)
				}
			}
		}
		if false {
			//if len(line) > maxBytes {
			currentFile.Truncated = true
			line = line[:maxBytes]
		}
		currentFile.Content += line + "\n"
	}
}

func ParseDiffHunkString(diffhunk string) (leftLine, leftHunk, rightLine, righHunk int) {
	ss := strings.Split(diffhunk, "@@")
	ranges := strings.Split(ss[1][1:], " ")
	leftRange := strings.Split(ranges[0], ",")
	leftLine, _ = strconv.Atoi(leftRange[0][1:])
	if len(leftRange) > 1 {
		leftHunk, _ = strconv.Atoi(leftRange[1])
	}
	if len(ranges) > 1 {
		rightRange := strings.Split(ranges[1], ",")
		rightLine, _ = strconv.Atoi(rightRange[0])
		if len(rightRange) > 1 {
			righHunk, _ = strconv.Atoi(rightRange[1])
		}
	} else {
		log.Debug().Msgf("Parse line number failed: %v", diffhunk)
		rightLine = leftLine
		righHunk = leftHunk
	}
	return leftLine, leftHunk, rightLine, righHunk
}

func parseDiff(input *bufio.Reader, currentCommit *Commit, maxFiles int, maxBytes int) error {
	line, err := input.ReadString('\n')
	if err != nil {
		return err
	}
	if len(line) > 0 && (line[len(line)-1] == '\n' || line[len(line)-1] == '\r') {
		line = line[:len(line)-1]
	}

	return nil
}

func parseLog2(out io.Reader, maxBytes int) []*Commit {
	reader := bufio.NewReader(out)

	var commits []*Commit
	var currentCommit *Commit
	var currentFile *File
	var isContent bool
	var bytesRead = 0
	scanNext := true

	for {
		line, err := reader.ReadString('\n')
		if scanNext && err == io.EOF {
			break
		}
		scanNext = true

		// new commit found
		currentFile = nil
		currentCommit = &Commit{Hash: line[2:], Files: []File{}}

		line, _ = reader.ReadString('\n')
		line = line[:len(line)-1]
		currentCommit.AuthorName = line[2:]

		line, _ = reader.ReadString('\n')
		line = line[:len(line)-1]
		currentCommit.AuthorEmail = line[2:]

		line, _ = reader.ReadString('\n')
		line = line[:len(line)-1]
		currentCommit.Timestamp = line[2:]

		line, _ = reader.ReadString('\n')
		line = line[:len(line)-1]
		if line == "" {
			commits = append(commits, currentCommit)
			break
		}

		// if there is no shortstat, it means that the commit is empty, we add it and move onto the next one
		if line[0] != ' ' {
			commits = append(commits, currentCommit)

			// avoid scanning the next line, as we already did it
			scanNext = false
			continue
		}

		changed := []byte(line)[1:]
		changed = bytes.ReplaceAll(changed, []byte("(+)"), []byte(""))
		changed = bytes.ReplaceAll(changed, []byte("(-)"), []byte(""))
		currentCommit.Changed = string(changed)

		// twice because --shortstat adds a new line
		line, _ = reader.ReadString('\n')
		line = line[:len(line)-1]
		line, _ = reader.ReadString('\n')
		line = line[:len(line)-1]

		// commit header parsed

		// files changes inside the commit
		for {
			// line := reader.Bytes()

			// end of content of file
			if len(line) == 0 {
				isContent = false
				if currentFile != nil {
					currentCommit.Files = append(currentCommit.Files, *currentFile)
				}
				break
			}

			// new file found
			if bytes.HasPrefix([]byte(line), []byte("diff --git")) {
				// current file is finished, we can add it to the commit
				if currentFile != nil {
					currentCommit.Files = append(currentCommit.Files, *currentFile)
				}

				// create a new file
				isContent = false
				bytesRead = 0
				currentFile = &File{}
				filenameRegex := regexp.MustCompile(`^diff --git a/(.+) b/(.+)$`)
				matches := filenameRegex.FindStringSubmatch(string(line))
				if len(matches) == 3 {
					currentFile.Filename = matches[2]
					if matches[1] != matches[2] {
						currentFile.OldFilename = matches[1]
					}
				}
				line, _ = reader.ReadString('\n')
				line = line[:len(line)-1]
				continue
			}

			if bytes.HasPrefix([]byte(line), []byte("new")) {
				currentFile.IsCreated = true
			}

			if bytes.HasPrefix([]byte(line), []byte("deleted")) {
				currentFile.IsDeleted = true
			}

			// file content found
			if line[0] == '@' {
				isContent = true
			}

			if isContent {
				currentFile.Content += string(line) + "\n"

				bytesRead += len(line)
				if bytesRead > maxBytes {
					currentFile.Truncated = true
					currentFile.Content = ""
					isContent = false
				}
			}

			line, _ = reader.ReadString('\n')
			if len(line) > 0 && (line[len(line)-1] == '\n' || line[len(line)-1] == '\r') {
				line = line[:len(line)-1]
			}
		}

		commits = append(commits, currentCommit)

	}

	return commits
}

func ParseCsv(file *File) (*CsvFile, error) {

	reader := csv.NewReader(strings.NewReader(file.Content))
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	header := records[0]
	numColumns := len(header)

	for i := 1; i < len(records); i++ {
		if len(records[i]) != numColumns {
			return nil, fmt.Errorf("CSV file has invalid row at index %d", i)
		}
	}

	return &CsvFile{
		File:   *file,
		Header: header,
		Rows:   records[1:],
	}, nil
}
