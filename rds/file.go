package rds

import (
	"github.com/aws/aws-sdk-go/service/rds"
)

const (
	// ZeroMarker zero value of rds api marker
	ZeroMarker = "0"
	// DefaultNumberOfLines default NumberOfLines value of rds api
	DefaultNumberOfLines = int64(10000)
)

// ByFileName implements sort.Interface for []*rds.DescribeDBLogFilesDetails
// based on the LogFileName field.
type ByFileName []*rds.DescribeDBLogFilesDetails

func (f ByFileName) Len() int      { return len(f) }
func (f ByFileName) Swap(i, j int) { f[i], f[j] = f[j], f[i] }
func (f ByFileName) Less(i, j int) bool {
	if f[j] == nil || f[j].LogFileName == nil {
		return false
	}

	if f[i] != nil && f[i].LogFileName != nil &&
		(len(*f[i].LogFileName) < len(*f[j].LogFileName) ||
			*f[i].LogFileName < *f[j].LogFileName) {
		return true
	}

	return false
}

// GetSlowQueryLogFiles returns log file details of aws rds
func (svc *Service) GetSlowQueryLogFiles(lastWritten *int64, prefix *string) ([]*rds.DescribeDBLogFilesDetails, error) {
	files := make([]*rds.DescribeDBLogFilesDetails, 0)

	result, err := svc.DescribeDBLogFiles(&rds.DescribeDBLogFilesInput{
		DBInstanceIdentifier: &svc.instance,
		FileLastWritten:      lastWritten,
		FilenameContains:     prefix,
	})
	if err != nil {
		return nil, err
	}

	files = append(files, result.DescribeDBLogFiles...)
	for result.Marker != nil && *result.Marker != ZeroMarker {
		result, err = svc.DescribeDBLogFiles(&rds.DescribeDBLogFilesInput{
			DBInstanceIdentifier: &svc.instance,
			FileLastWritten:      lastWritten,
			Marker:               result.Marker,
			FilenameContains:     prefix,
		})
		if err != nil {
			return nil, err
		}
		files = append(files, result.DescribeDBLogFiles...)
	}

	return files, nil
}

// DownloadDBLogFilePortion calls rds api DownloadDBLogFilePortion
func (svc *Service) DownloadDBLogFilePortion(logFileName, marker *string, lines *int64) (*rds.DownloadDBLogFilePortionOutput, error) {
	return svc.RDS.DownloadDBLogFilePortion(&rds.DownloadDBLogFilePortionInput{
		DBInstanceIdentifier: &svc.instance,
		LogFileName:          logFileName,
		Marker:               marker,
		NumberOfLines:        lines,
	})
}
