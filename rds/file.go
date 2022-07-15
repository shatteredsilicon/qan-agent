package rds

import (
	"bytes"

	"github.com/aws/aws-sdk-go/service/rds"
)

const (
	zeroMarker = "0"
)

// GetLogFiles returns log file details of aws rds
func (svc *Service) GetLogFiles(lastWritten *int64) ([]*rds.DescribeDBLogFilesDetails, error) {
	files := make([]*rds.DescribeDBLogFilesDetails, 0)

	result, err := svc.DescribeDBLogFiles(&rds.DescribeDBLogFilesInput{
		DBInstanceIdentifier: &svc.instance,
		FileLastWritten:      lastWritten,
	})
	if err != nil {
		return nil, err
	}

	files = append(files, result.DescribeDBLogFiles...)
	for result.Marker != nil && *result.Marker != zeroMarker {
		result, err = svc.DescribeDBLogFiles(&rds.DescribeDBLogFilesInput{
			DBInstanceIdentifier: &svc.instance,
			FileLastWritten:      lastWritten,
			Marker:               result.Marker,
		})
		if err != nil {
			return nil, err
		}
		files = append(files, result.DescribeDBLogFiles...)
	}

	return files, nil
}

// DownloadLogFile downloads full data of a rds log file and returns it
func (svc *Service) DownloadLogFile(filename string) ([]byte, error) {
	result, err := svc.DownloadDBLogFilePortion(&rds.DownloadDBLogFilePortionInput{
		DBInstanceIdentifier: &svc.instance,
		LogFileName:          &filename,
	})
	if err != nil {
		return nil, err
	}

	var data bytes.Buffer
	data.WriteString(*result.LogFileData)
	for result.AdditionalDataPending != nil && *result.AdditionalDataPending {
		result, err = svc.DownloadDBLogFilePortion(&rds.DownloadDBLogFilePortionInput{
			DBInstanceIdentifier: &svc.instance,
			LogFileName:          &filename,
			Marker:               result.Marker,
		})
		if err != nil {
			return nil, err
		}
		data.WriteString(*result.LogFileData)
	}

	return data.Bytes(), nil
}
