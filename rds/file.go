package rds

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
)

const (
	// ZeroMarker zero value of rds api marker
	ZeroMarker = "0"
	// DefaultNumberOfLines default NumberOfLines value of rds api
	DefaultNumberOfLines = int32(10000)
)

// ByFileName implements sort.Interface for []*rds.DescribeDBLogFilesDetails
// based on the LogFileName field.
type ByFileName []types.DescribeDBLogFilesDetails

func (f ByFileName) Len() int      { return len(f) }
func (f ByFileName) Swap(i, j int) { f[i], f[j] = f[j], f[i] }
func (f ByFileName) Less(i, j int) bool {
	if f[j].LogFileName == nil {
		return false
	}

	if f[i].LogFileName != nil &&
		(len(*f[i].LogFileName) < len(*f[j].LogFileName) ||
			*f[i].LogFileName < *f[j].LogFileName) {
		return true
	}

	return false
}

// GetLogFiles returns log file details of aws rds
func (svc *Service) GetLogFiles(lastWritten *int64, prefix *string) ([]types.DescribeDBLogFilesDetails, error) {
	files := make([]types.DescribeDBLogFilesDetails, 0)

	result, err := svc.DescribeDBLogFiles(context.TODO(), &rds.DescribeDBLogFilesInput{
		DBInstanceIdentifier: &svc.instance,
		FileLastWritten:      lastWritten,
		FilenameContains:     prefix,
	})
	if err != nil {
		return nil, err
	}

	files = append(files, result.DescribeDBLogFiles...)
	for result.Marker != nil && *result.Marker != ZeroMarker {
		result, err = svc.DescribeDBLogFiles(context.TODO(), &rds.DescribeDBLogFilesInput{
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
func (svc *Service) DownloadDBLogFilePortion(logFileName, marker *string, lines *int32) (*rds.DownloadDBLogFilePortionOutput, error) {
	return svc.Client.DownloadDBLogFilePortion(context.TODO(), &rds.DownloadDBLogFilePortionInput{
		DBInstanceIdentifier: &svc.instance,
		LogFileName:          logFileName,
		Marker:               marker,
		NumberOfLines:        lines,
	})
}
