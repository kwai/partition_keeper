package third_party

type NullPerfLogger struct {
}

func (n *NullPerfLogger) LogWithTags(
	bizdef, namespace, subtag, extra1, extra2, extra3, extra4, extra5, extra6 string,
	value uint64,
) {
}
