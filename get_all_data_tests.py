import json
import pandas as pd
import os 

test_output   = os.popen('dbt ls --output json --resource-type test').read().split('\n')
source_output = os.popen('dbt ls --output json --resource-type source').read().split('\n')
model_output  = os.popen('dbt ls --output json --resource-type model').read().split('\n')
del test_output[-1]
del source_output[-1]
del model_output[-1]


testlist = [json.loads(line) for line in test_output]
sourcemodellist = [json.loads(line) for line in source_output] +  [json.loads(line) for line in model_output]

resources={}
for sourcemodel in sourcemodellist:
  source_name = sourcemodel['source_name'] if 'source_name' in sourcemodel.keys() else ""
  res_name = sourcemodel['resource_type'] 
  res_name += "." + sourcemodel['package_name'] 
  res_name += "." + sourcemodel['source_name'] if 'source_name' in sourcemodel.keys() else ""
  res_name += "." + sourcemodel['name']
  resources[res_name] = { 
  'package': sourcemodel['package_name'],
  'type': sourcemodel['resource_type'],
  'source_name': source_name,
  'resource_path': res_name,
  'name': sourcemodel['name'],
  'tests':[False]
  }
  
for test in testlist:
  key = test['depends_on']['nodes'][0] 
  test_meta = { 'severity': test['config']['severity'],
                'short_name': test['name'].replace(resources[key]['name'],""),
                'full_name': test['name'] 
            }
  if resources[key]['tests'][0]:
    resources[key]['tests'].append(test_meta)
  else:
    resources[key]['tests'] = [test_meta]


resource_list = [value for key, value in resources.items()]
print(json.dumps(resource_list, sort_keys=True, indent=4))

csv_output = ",".join(['package','source_name','resource_path','type','name','test_n','severity','short_name','full_name'])
for r in resource_list:
  test_n = 0
  for t in r['tests']:
    if t:
      test_n+=1
      test = [str(test_n), t['severity'], t['short_name'], t['full_name']]
    else:
      test = ['0','','','']
    
    row = [r['package'],r['source_name'],r['resource_path'],r['type'],r['name']] + test
    csv_output += "\n"
    csv_output += ",".join(row)

with open("sources_models_tests.csv","w+") as f:
    f.write(csv_output)