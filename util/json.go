package util

const (
	MAX_OBJ_DEPTH = 100
)

func IsJSONKeyExists(data interface{}, key string, depth int) bool {
	if depth >= MAX_OBJ_DEPTH {
		return false
	}
	depth++

	switch obj := data.(type) {
	case map[string]interface{}:
		for k, v := range obj {
			if k == key {
				return true
			}
			if exists := IsJSONKeyExists(v, key, depth); exists {
				return true
			}
		}
	case []interface{}:
		for _, v := range obj {
			if exists := IsJSONKeyExists(v, key, depth); exists {
				return true
			}
		}
	}

	return false
}

func GetJSONObjectsByKey(data interface{}, key string) []interface{} {
	objects := []interface{}{}

	items := []interface{}{data}
	for i := 0; i < len(items); i++ {
		switch item := items[i].(type) {
		case map[string]interface{}:
			for k, v := range item {
				if k == key {
					objects = append(objects, v)
					continue
				}

				switch val := v.(type) {
				case map[string]interface{}, []interface{}:
					items = append(items, val)
				}
			}
		case []interface{}:
			items = append(items, item...)
		}
	}

	return objects
}
