import pandas as pd
import requests
import json

RT = []
#api_url = "https://wabi-brazil-south-api.analysis.windows.net/public/reports/querydata?synchronous=true"
api_url = "https://wabi-west-europe-f-primary-api.analysis.windows.net/public/reports/querydata?synchronous=true"

headers = {"X-PowerBI-ResourceKey": "7e74c25a-e015-435a-a16c-98af7bb481cd"}

country = "Brazil"
n_items = 500000

def get_query(country, n_items):
    query = {
    "version": "1.0.0",
    "queries": [
        {
            "Query": {
                "Commands": [
                    {
                        "SemanticQueryDataShapeCommand": {
                            "Query": {
                                "Version": 2,
                                "From": [
                                    {
                                        "Name": "o",
                                        "Entity": "Organization",
                                        "Type": 0
                                    },
                                    {
                                        "Name": "c",
                                        "Entity": "Certificate",
                                        "Type": 0
                                    },
                                    {
                                        "Name": "c1",
                                        "Entity": "Country_R",
                                        "Type": 0
                                    }
                                ],
                                "Select": [
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {
                                                    "Source": "o"
                                                }
                                            },
                                            "Property": "Organization Name"
                                        },
                                        "Name": "Organization.Name"
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {
                                                    "Source": "c"
                                                }
                                            },
                                            "Property": "License"
                                        },
                                        "Name": "Certificate.License"
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {
                                                    "Source": "c"
                                                }
                                            },
                                            "Property": "Date_From__c"
                                        },
                                        "Name": "Certificate.Date_From__c"
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {
                                                    "Source": "c"
                                                }
                                            },
                                            "Property": "Date_To__c"
                                        },
                                        "Name": "Certificate.Date_To__c"
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {
                                                    "Source": "c"
                                                }
                                            },
                                            "Property": "Full_Certificate_Code__c"
                                        },
                                        "Name": "Certificate.Full_Certificate_Code__c"
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {
                                                    "Source": "c"
                                                }
                                            },
                                            "Property": "status"
                                        },
                                        "Name": "Certificate.status"
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {
                                                    "Source": "o"
                                                }
                                            },
                                            "Property": "Type (groups)"
                                        },
                                        "Name": "Organization.Type (groups)"
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {
                                                    "Source": "c"
                                                }
                                            },
                                            "Property": "CW"
                                        },
                                        "Name": "Certificate.CW"
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {
                                                    "Source": "o"
                                                }
                                            },
                                            "Property": "State_County__c"
                                        },
                                        "Name": "Organization.State_County__c"
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {
                                                    "Source": "o"
                                                }
                                            },
                                            "Property": "Site status"
                                        },
                                        "Name": "Organization.Site status"
                                    },
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {
                                                    "Source": "c1"
                                                }
                                            },
                                            "Property": "Country/Area"
                                        },
                                        "Name": "Country_R.Country/Region",
                                        "NativeReferenceName": "Country/Region"
                                    }
                                ],
                                "Where": [
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "c"
                                                                }
                                                            },
                                                            "Property": "status"
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": "'Valid'"
                                                            }
                                                        }
                                                    ]
                                                ]
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "c1"
                                                                }
                                                            },
                                                            "Property": "Country/Area"
                                                        }
                                                    }
                                                ],
                                                "Values": [
                                                    [
                                                        {
                                                            "Literal": {
                                                                "Value": f"'{country}'"
                                                            }
                                                        }
                                                    ]
                                                ]
                                            }
                                        }
                                    },
                                    {
                                        "Condition": {
                                            "Not": {
                                                "Expression": {
                                                    "In": {
                                                        "Expressions": [
                                                            {
                                                                "Column": {
                                                                    "Expression": {
                                                                        "SourceRef": {
                                                                            "Source": "c"
                                                                        }
                                                                    },
                                                                    "Property": "Cert_Status__c"
                                                                }
                                                            }
                                                        ],
                                                        "Values": [
                                                            [
                                                                {
                                                                    "Literal": {
                                                                        "Value": "null"
                                                                    }
                                                                }
                                                            ],
                                                            [
                                                                {
                                                                    "Literal": {
                                                                        "Value": "'Applicant'"
                                                                    }
                                                                }
                                                            ]
                                                        ]
                                                    }
                                                }
                                            }
                                        }
                                    }
                                ],
                                "OrderBy": [
                                    {
                                        "Direction": 2,
                                        "Expression": {
                                            "Column": {
                                                "Expression": {
                                                    "SourceRef": {
                                                        "Source": "c"
                                                    }
                                                },
                                                "Property": "Date_From__c"
                                            }
                                        }
                                    }
                                ]
                            },
                            "Binding": {
                                "Primary": {
                                    "Groupings": [
                                        {
                                            "Projections": [
                                                1,
                                                4,
                                                5,
                                                7,
                                                2,
                                                3,
                                                0,
                                                6,
                                                9,
                                                8,
                                                10
                                            ],
                                            "ShowItemsWithNoData": [
                                                1,
                                                4,
                                                5,
                                                7,
                                                2,
                                                3,
                                                0,
                                                6,
                                                9,
                                                8,
                                                10
                                            ]
                                        }
                                    ]
                                },
                                "DataReduction": {
                                    "DataVolume": 3,
                                    "Primary": {
                                        "Window": {
                                            "Count": n_items
                                        }
                                    }
                                },
                                "Version": 1
                            },
                            "ExecutionMetricsKind": 1
                        }
                    }
                ]
            },
            "QueryId": "",
            "ApplicationContext": {
                "DatasetId": "d94c3e96-8435-4af1-b2ba-21ef6defaa95",
                "Sources": [
                    {
                        "ReportId": "8be6b660-e5ba-422d-951f-797c9e2a4af7",
                        "VisualId": "8eb8554a410440b2a897"
                    }
                ]
            }
        }
    ],
    "cancelQueries": [],
    "modelId": 542581
    }
    return query


def find_key_recursively(obj, key):
    if isinstance(obj, dict):
        if key in obj:
            yield obj[key]

        for v in obj.values():
            yield from find_key_recursively(v, key)
    elif isinstance(obj, list):
        for v in obj:
            yield from find_key_recursively(v, key)

def parse_json(json_data):
    try:
        results = json_data.get("results", [])
        if not results:
            return pd.DataFrame()  # Return empty DataFrame if no results

        result = results[0].get("result", {})
        data = result.get("data", {})
        dsr = data.get("dsr", {})
        ds = dsr.get("DS", [])

        # Extract columns
        descriptor = data.get("descriptor", {})
        select_keys = descriptor.get("Select", [])
        columns = [key.get('Name', '').replace('.', '_') for key in select_keys]

        # Extract rows
        rows = []
        for dataset in ds:
            # Handle potential 'RT' data
            if 'RT' in dataset:
                rt_data = dataset['RT'][0]
                rows.append(rt_data)
            else:
                # General case for 'C' and 'PH' keys
                ph_data = dataset.get('PH', [])
                for ph_item in ph_data:
                    dm_data = ph_item.get('DM0', [])
                    for dm_item in dm_data:
                        row = dm_item.get('C', [])
                        rows.append(row)

        # Ensure that rows match columns
        max_columns = max(len(row) for row in rows)
        if len(columns) != max_columns:
            columns = columns[:max_columns]

        df = pd.DataFrame(rows, columns=columns)
        return df

    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error


all_data = []

data = requests.post(api_url, json=get_query(country, n_items), headers=headers).json()
with open('response_data.json', 'w') as f: f.write(json.dumps(data, indent=4, ensure_ascii=False))

df = parse_json(data)

print(df)
df.to_csv("fsc-data.csv", index=False)