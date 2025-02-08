package utils

import "sort"

var (
	az2Region = map[string]string{
		"STAGING": "STAGING",
		"YZ":      "HB1",
		"ZW":      "HB1",
		"SY":      "HB1",
		"HB1AZ1":  "HB1",
		"HB1AZ2":  "HB1",
		"HB1AZ3":  "HB1",
		"HB1AZ4":  "HB1",
		"HB2AZ1":  "HB2",
		"GZ1":     "HN1",
		"TXSGP1":  "SGP",
		"GCPSGPC": "SGP",
		"TXVA2":   "VA",
		"SGPAZ1":  "SGP",
		"SGPAZ2":  "SGP",
		"SGPAZ3":  "SGP",
		"SGPAZ4":  "SGP",
	}
	pazs = map[string]bool{
		"STAGING": true,
		"HB1AZ1":  true,
		"HB1AZ2":  true,
		"HB1AZ3":  true,
		"HB1AZ4":  true,
		"SGPAZ1":  true,
		"SGPAZ2":  true,
		"SGPAZ3":  true,
		"SGPAZ4":  true,
	}
	paz2vaz = map[string]string{
		"STAGING": "STAGING",
		"HB1AZ1":  "ZW",
		"HB1AZ2":  "YZ",
		"HB1AZ3":  "ZW",
		"HB1AZ4":  "YZ",
	}
)

func AzNameValid(az string) bool {
	_, ok := az2Region[az]
	return ok
}

func PazNameValid(az string) bool {
	_, ok := pazs[az]
	return ok
}

func Paz2Vaz(paz string) string {
	vaz, ok := paz2vaz[paz]
	if ok {
		return vaz
	}
	return ""
}

func RegionOfAz(az string) string {
	return az2Region[az]
}

func RegionsOfAzs(azs map[string]bool) map[string]bool {
	ans := map[string]bool{}
	for az := range azs {
		ans[RegionOfAz(az)] = true
	}
	return ans
}

func RegionListOfAzs(azs map[string]bool) []string {
	ans := RegionsOfAzs(azs)
	output := []string{}
	for region := range ans {
		output = append(output, region)
	}
	sort.Strings(output)
	return output
}
