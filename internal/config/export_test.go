package config

func Validate(node Key, value string) error {
	return node.validate(value)
}
