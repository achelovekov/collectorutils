# nxapi-ins

Process structured text data from NXOS via NXAPI. Data comes in response body as structured text. 

# Examples

response:\
```
{
		"result": {
		  "status": "CC_STATUS_OK",
		  "checkers": [
			{
			  "version": 1,
			  "type": "CC_TYPE_IF_AGGR_MEMBERSHIP",
			  "status": "CC_STATUS_OK",
			  "platformDetails": {
				"classType": "CC_PLTFM_NXOS_CSC_ROCKY"
			  },
			  "recoveryActions": [],
			  "failedEntities": [],
			  "passedEntities": [
				{
				  "key": [
					{
					  "type": "CC_ENTITY_IF_AGGR",
					  "value": "po20"
					}
				  ],
				  "value": {
					"errorCode": "CC_ERR_SUCCESS",
					"entityDetails": {
					  "ifIndex": 369098771,
					  "nxIndex": 4768,
					  "nsPId": 0,
					  "dMod": 0,
					  "dPId": 2,
					  "LTL": 3,
					  "srcId": 0
					},
					"recoveryActions": [],
					"checkedProperties": {
					  "tahoe": {
						"egrNumPaths_Sw_Hw": {
						  "errorCode": "CC_ERR_SUCCESS",
						  "expected": 1,
						  "actual": 1,
						  "expectedDetails": {},
						  "actualDetails": {
							"hwTableName": "tah_hom_luc_ucportchannelconfigtable 1538 changed",
							"hwTableField": "num_paths, base_ptr"
						  }
						},
						"egrMembers_Sw_Hw": {
						  "errorCode": "CC_ERR_SUCCESS",
						  "expected": [
							"eth1/20"
						  ],
						  "actual": [
							"eth1/20"
						  ],
						  "expectedDetails": {},
						  "actualDetails": {
							"hwTableName": "tah_hom_luc_ucportchannelmembertable 1537",
							"hwTableField": "dst_chip, dst_port"
						  }
						}
					  }
					}
				  }
				},
				{
				  "key": [
					{
					  "type": "CC_ENTITY_IF_AGGR",
					  "value": "po3967"
					}
				  ],
				  "value": {
					"errorCode": "CC_ERR_SUCCESS",
					"entityDetails": {
					  "ifIndex": 369102718,
					  "nxIndex": 4768,
					  "nsPId": 0,
					  "dMod": 0,
					  "dPId": 3,
					  "LTL": 4,
					  "srcId": 0
					},
					"recoveryActions": [],
					"checkedProperties": {
					  "tahoe": {
						"egrNumPaths_Sw_Hw": {
						  "errorCode": "CC_ERR_SUCCESS",
						  "expected": 2,
						  "actual": 2,
						  "expectedDetails": {},
						  "actualDetails": {
							"hwTableName": "tah_hom_luc_ucportchannelconfigtable 1539 changed",
							"hwTableField": "num_paths, base_ptr"
						  }
						},
						"egrMembers_Sw_Hw": {
						  "errorCode": "CC_ERR_SUCCESS",
						  "expected": [
							"eth1/53",
							"eth1/54"
						  ],
						  "actual": [
							"eth1/53",
							"eth1/54"
						  ],
						  "expectedDetails": {},
						  "actualDetails": {
							"hwTableName": "tah_hom_luc_ucportchannelmembertable 1540",
							"hwTableField": "dst_chip, dst_port"
						  }
						}
					  }
					}
				  }
				}
			  ],
			  "skippedEntities": []
			}
		  ]
		}
	  }
```

processed output:
```
Pretty processed output {
  "checkers.status": "CC_STATUS_OK",
  "checkers.type": "CC_TYPE_IF_AGGR_MEMBERSHIP",
  "checkers.version": 1,
  "egrMembers_Sw_Hw.errorCode": "CC_ERR_SUCCESS",
  "egrNumPaths_Sw_Hw.actual": 1,
  "egrNumPaths_Sw_Hw.errorCode": "CC_ERR_SUCCESS",
  "egrNumPaths_Sw_Hw.expected": 1,
  "entityDetails.LTL": 3,
  "entityDetails.dMod": 0,
  "entityDetails.dPId": 2,
  "entityDetails.ifIndex": 369098771,
  "entityDetails.nsPId": 0,
  "entityDetails.nxIndex": 4768,
  "entityDetails.srcId": 0,
  "key.type": "CC_ENTITY_IF_AGGR",
  "key.value": "po20",
  "platformDetails.classType": "CC_PLTFM_NXOS_CSC_ROCKY",
  "result.status": "CC_STATUS_OK",
  "value.errorCode": "CC_ERR_SUCCESS"
}
Pretty processed output {
  "checkers.status": "CC_STATUS_OK",
  "checkers.type": "CC_TYPE_IF_AGGR_MEMBERSHIP",
  "checkers.version": 1,
  "egrMembers_Sw_Hw.errorCode": "CC_ERR_SUCCESS",
  "egrNumPaths_Sw_Hw.actual": 2,
  "egrNumPaths_Sw_Hw.errorCode": "CC_ERR_SUCCESS",
  "egrNumPaths_Sw_Hw.expected": 2,
  "entityDetails.LTL": 4,
  "entityDetails.dMod": 0,
  "entityDetails.dPId": 3,
  "entityDetails.ifIndex": 369102718,
  "entityDetails.nsPId": 0,
  "entityDetails.nxIndex": 4768,
  "entityDetails.srcId": 0,
  "key.type": "CC_ENTITY_IF_AGGR",
  "key.value": "po3967",
  "platformDetails.classType": "CC_PLTFM_NXOS_CSC_ROCKY",
  "result.status": "CC_STATUS_OK",
  "value.errorCode": "CC_ERR_SUCCESS"
}
```