package rds

import "github.com/aws/aws-sdk-go/service/rds"

// GetParam returns value of a specific paramter in aws rds parameter group
func (svc *Service) GetParam(name string) (*rds.Parameter, error) {
	instanceOutput, err := svc.DescribeDBInstances(&rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: &svc.instance,
	})
	if err != nil {
		return nil, err
	}
	if len(instanceOutput.DBInstances) < 1 {
		return nil, ErrInstanceNotFound
	}

	var paramGroup *string
	for _, group := range instanceOutput.DBInstances[0].DBParameterGroups {
		if group != nil && *group.ParameterApplyStatus == statusParamGroupApplied {
			paramGroup = group.DBParameterGroupName
			break
		}
	}
	if paramGroup == nil {
		return nil, ErrNoParamGroupApplied
	}

	paramOutput, err := svc.DescribeDBParameters(&rds.DescribeDBParametersInput{
		DBParameterGroupName: paramGroup,
	})
	if err != nil {
		return nil, err
	}
	for {
		for _, param := range paramOutput.Parameters {
			if param.ParameterName != nil && *param.ParameterName == name {
				return param, nil
			}
		}
		if paramOutput.Marker == nil || len(paramOutput.Parameters) == 0 {
			break
		}
		paramOutput, err = svc.DescribeDBParameters(&rds.DescribeDBParametersInput{
			DBParameterGroupName: paramGroup,
			Marker:               paramOutput.Marker,
		})
		if err != nil {
			return nil, err
		}
	}

	return nil, ErrParamNotFound
}
