# CMG-Data | Version 3

## Steps of preprocessing
1. Remove error commit
2. Remove empty messages: ['None', '*** empty log message ***', '...', 'no message','typo', 'Typo.']
3. Filter message length: 3 <= len(msg) <= 12
4. Filter change_size: 1 <= change_size <= 10
5. Drop commit has mutiple change files

Num of commits: 25498

## Extract variables map

* Code diff: variable_diff.json
* Code change: variable_change.json
  
File format
```
{
	'index1': {
		'count' : 'var1',
		'sum' : 'var2',
	}, 
	'index2': {
		'count' : 'var3',
		'sum' : 'var4',
	}, 
}
```
## For read data: 
```
pd.read_parquet(f'cmg-data/cmg-data-processed.parquet', engine='fastparquet')
```