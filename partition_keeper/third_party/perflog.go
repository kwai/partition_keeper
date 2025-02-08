package third_party

type PerfLogger interface {
	// at most support 6 tags
	LogWithTags(
		bizdef, namespace, subtag, extra1, extra2, extra3, extra4, extra5, extra6 string,
		value uint64,
	)
}

var (
	perfLogger PerfLogger = &NullPerfLogger{}
)

func SetPerfLogger(pl PerfLogger) {
	perfLogger = pl
}

func PerfLog(bizdef, namespace, subtag string, value uint64) {
	perfLogger.LogWithTags(bizdef, namespace, subtag, "", "", "", "", "", "", value)
}

func PerfLog1(bizdef, namespace, subtag, extra1 string, value uint64) {
	perfLogger.LogWithTags(bizdef, namespace, subtag, extra1, "", "", "", "", "", value)
}

func PerfLog2(bizdef, namespace, subtag, extra1, extra2 string, value uint64) {
	perfLogger.LogWithTags(bizdef, namespace, subtag, extra1, extra2, "", "", "", "", value)
}

func PerfLog3(bizdef, namespace, subtag, extra1, extra2, extra3 string, value uint64) {
	perfLogger.LogWithTags(bizdef, namespace, subtag, extra1, extra2, extra3, "", "", "", value)
}

func PerfLog4(bizdef, namespace, subtag, extra1, extra2, extra3, extra4 string, value uint64) {
	perfLogger.LogWithTags(bizdef, namespace, subtag, extra1, extra2, extra3, extra4, "", "", value)
}
