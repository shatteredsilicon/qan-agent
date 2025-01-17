package rds

import (
	"errors"

	"github.com/aws/aws-sdk-go/service/rds"
)

const (
	statusParamGroupApplied = "in-sync"
	sourceUser              = "user"

	// FILEParamValue a parameter value indicates that it's file
	FILEParamValue = "FILE"
	// TABLEParamValue a parameter value indicates that it's table
	TABLEParamValue = "TABLE"
	// TrueParamValue a parameter value indicates that it's true
	TrueParamValue = "1"
)

var (
	// ErrInstanceNotFound instance not found error
	ErrInstanceNotFound = errors.New("specified instance not found")
	// ErrNoParamGroupApplied no paramter group applied error
	ErrNoParamGroupApplied = errors.New("no parameter group applied on this instance")
	// ErrParamNotFound parameter not found error
	ErrParamNotFound = errors.New("specified parameter not found")
	// ErrClusterNotFound cluster not found error
	ErrClusterNotFound = errors.New("cluster that specified instance associated to not found")
	// ErrNoClusterParamGroupApplied no cluster paramter group applied error
	ErrNoClusterParamGroupApplied = errors.New("no parameter group applied on this cluster")
)

// Service rds service
type Service struct {
	*service
}

type service struct {
	*rds.RDS
	instance string
}

// NewService returns a rds service
func NewService(r *rds.RDS, instance string) *Service {
	return &Service{
		service: &service{
			RDS:      r,
			instance: instance,
		},
	}
}
