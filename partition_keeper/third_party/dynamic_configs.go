package third_party

type ConfigWatcher interface {
	OnValueRemoved(key string) error
	OnStringValueUpdated(key string, val string) error
}

func ConfigAddStringWatcher(configPath string, watcher ConfigWatcher) error {
	// in kuaishou, we have internal dynamic config called Kconf,
	// here you may implement yours
	return nil
}

func GetStringConfig(configPath string) (string, error) {
	// in kuaishou, we have internal dynamic config called Kconf,
	// here you may implement yours
	return "", nil
}
