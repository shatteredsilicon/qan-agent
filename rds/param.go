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

	dbClusterID := instanceOutput.DBInstances[0].DBClusterIdentifier
	if paramGroup == nil && (dbClusterID == nil || len(*dbClusterID) == 0) {
		return nil, ErrNoParamGroupApplied
	}

	var dbParam *rds.Parameter
	if paramGroup != nil {
		paramOutput, err := svc.DescribeDBParameters(&rds.DescribeDBParametersInput{
			DBParameterGroupName: paramGroup,
		})
		if err != nil {
			return nil, err
		}
		for {
			for _, param := range paramOutput.Parameters {
				if param.ParameterName != nil && *param.ParameterName == name {
					dbParam = param
					break
				}
			}
			if dbParam != nil || paramOutput.Marker == nil || len(paramOutput.Parameters) == 0 {
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
	}

	var dbClusterParam *rds.Parameter
	if dbClusterID != nil && (paramGroup == nil || dbParam == nil || *dbParam.Source != sourceUser) {
		// check cluster parameter
		clusterOutput, err := svc.DescribeDBClusters(&rds.DescribeDBClustersInput{
			DBClusterIdentifier: dbClusterID,
		})
		if err != nil {
			return nil, err
		}

		if len(clusterOutput.DBClusters) < 1 {
			return nil, ErrClusterNotFound
		}

		var exists bool
		for _, member := range clusterOutput.DBClusters[0].DBClusterMembers {
			if member.DBInstanceIdentifier != nil && *member.DBInstanceIdentifier == svc.instance {
				exists = true
				break
			}
		}

		// db instance not associated to this cluster anymore
		if !exists {
			return nil, ErrClusterNotFound
		}

		// cluster parameter group not exists
		if clusterOutput.DBClusters[0].DBClusterParameterGroup == nil {
			return nil, ErrNoClusterParamGroupApplied
		}

		paramOutput, err := svc.DescribeDBClusterParameters(&rds.DescribeDBClusterParametersInput{
			DBClusterParameterGroupName: clusterOutput.DBClusters[0].DBClusterParameterGroup,
		})
		if err != nil {
			return nil, err
		}

		for {
			for _, param := range paramOutput.Parameters {
				if param.ParameterName != nil && *param.ParameterName == name {
					dbClusterParam = param
					break
				}
			}
			if dbClusterParam != nil || paramOutput.Marker == nil || len(paramOutput.Parameters) == 0 {
				break
			}
			paramOutput, err = svc.DescribeDBClusterParameters(&rds.DescribeDBClusterParametersInput{
				DBClusterParameterGroupName: clusterOutput.DBClusters[0].DBClusterParameterGroup,
				Marker:                      paramOutput.Marker,
			})
			if err != nil {
				return nil, err
			}
		}
	}

	if dbParam == nil && dbClusterParam == nil {
		return nil, ErrParamNotFound
	}

	if dbParam == nil || (*dbParam.Source != sourceUser && dbClusterParam != nil) {
		return dbClusterParam, nil
	}

	return dbParam, nil
}
