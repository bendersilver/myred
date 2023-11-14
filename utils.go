package myred

import "fmt"

func parseValues(str string) ([]string, error) {
	values := make([]string, 0, 8)
	i := 0
	for i < len(str) {
		if str[i] != '\'' {
			j := i + 1
			for ; j < len(str) && str[j] != ','; j++ {
			}
			if str[i:j] == "NULL" {
				values = append(values, "")
			} else {
				values = append(values, str[i:j])
			}
			i = j + 1
		} else {
			j := i + 1
			escaped := false
			for j < len(str) {
				if str[j] == '\\' {
					j += 2
					escaped = true
					continue
				} else if str[j] == '\'' {
					break
				} else {
					j++
				}
			}

			if j >= len(str) {
				return nil, fmt.Errorf("parse quote values error")
			}
			value := str[i+1 : j]
			if escaped {
				value = unescapeString(value)
			}

			values = append(values, value)

			i = j + 2
		}
	}
	return values, nil
}

func unescapeString(s string) string {
	i := 0

	value := make([]byte, 0, len(s))
	for i < len(s) {
		if s[i] == '\\' {
			j := i + 1
			if j == len(s) {
				// The last char is \, remove
				break
			}

			value = append(value, unescapeChar(s[j]))
			i += 2
		} else {
			value = append(value, s[i])
			i++
		}
	}

	return string(value)
}

func unescapeChar(ch byte) byte {
	// \" \' \\ \n \0 \b \Z \r \t ==> escape to one char
	switch ch {
	case 'n':
		ch = '\n'
	case '0':
		ch = 0
	case 'b':
		ch = 8
	case 'Z':
		ch = 26
	case 'r':
		ch = '\r'
	case 't':
		ch = '\t'
	}
	return ch
}
